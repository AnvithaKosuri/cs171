import json
import socket
import threading
import time
import sys
import random
from typing import Dict, Any, Tuple, Optional, List, Set

from blockchain import Blockchain, Block, Transaction
from storage import (
    init_balances, load_balances, save_balances,
    init_blockchain, load_blockchain, save_blockchain,
    init_paxos_state, load_paxos_state, save_paxos_state, apply_block,
)

CONFIG_FILE = "config.json"
Ballot = Tuple[int, int, int]  # (seq_num, proc_id, depth)


class PaxosState:
    """Per-depth Paxos acceptor state."""
    def __init__(self):
        self.promised_n: Optional[Ballot] = None
        self.accepted_n: Optional[Ballot] = None
        self.accepted_block: Optional[Block] = None


class Node:
    """Blockchain node participating in Paxos consensus."""

    def __init__(self, node_id: int):
        self.id = node_id
        
        # Load config
        with open(CONFIG_FILE) as f:
            cfg = json.load(f)
        self.nodes = cfg["nodes"]
        self.delay = cfg.get("message_delay_sec", 0.0)
        self.me = next(n for n in self.nodes if n["id"] == self.id)
        self.majority = len(self.nodes) // 2 + 1

        # Initialize persistent state
        init_balances(self.id, len(self.nodes))
        init_blockchain(self.id)
        init_paxos_state(self.id)
        
        self.balances = load_balances(self.id)
        self.blockchain = load_blockchain(self.id)

        # Paxos state
        self.paxos: Dict[int, PaxosState] = {}  # depth -> state
        self.decided: Set[int] = set()
        self.proposal_seq = 0
        self.current_ballot: Dict[int, Ballot] = {}
        self.proposed_blocks: Dict[int, Block] = {}
        self.promises: Dict[int, Dict[int, Tuple]] = {}
        self.accepted_counts: Dict[Tuple, int] = {}
        self.accept_started: Set[int] = set()
        
        # Recovery state
        self.depth_responses: Dict[int, int] = {}
        self.received_chains: Dict[int, Dict] = {}
        self.awaiting_depths = False
        self.syncing = False
        
        # Restore from disk
        self._restore_state()

        # Networking
        self.sock: Optional[socket.socket] = None
        self.running = True
        self.lock = threading.Lock()

        print(f"[Node {self.id}] Started on {self.me['host']}:{self.me['port']}")

    # ==================== State Persistence ====================

    def _restore_state(self):
        saved = load_paxos_state(self.id)
        if not saved:
            return
        self.decided = set(saved.get("decided_depths", []))
        self.proposal_seq = saved.get("proposal_seq", 0)
        for d_str, st in saved.get("paxos_states", {}).items():
            d = int(d_str)
            ps = PaxosState()
            if st.get("promised_n"):
                ps.promised_n = tuple(st["promised_n"])
            if st.get("accepted_n"):
                ps.accepted_n = tuple(st["accepted_n"])
            if st.get("accepted_block"):
                ps.accepted_block = Block.from_dict(st["accepted_block"])
            self.paxos[d] = ps
        print(f"[Node {self.id}] Restored: decided={self.decided}, seq={self.proposal_seq}")

    def _save_state(self):
        save_paxos_state(self.id, self.paxos, self.decided, self.proposal_seq)

    def _recompute_balances(self):
        """Recompute balances from blockchain."""
        self.balances = {str(i): 100 for i in range(1, len(self.nodes) + 1)}
        for b in self.blockchain.blocks:
            if not b.tentative:
                apply_block(b, self.balances)
        save_balances(self.id, self.balances)

    # ==================== Networking ====================

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.me["host"], self.me["port"]))
        self.sock.listen()
        threading.Thread(target=self._listen, daemon=True).start()

    def _listen(self):
        while self.running:
            try:
                conn, _ = self.sock.accept()
                threading.Thread(target=self._handle, args=(conn,), daemon=True).start()
            except OSError:
                break

    def _handle(self, conn: socket.socket):
        try:
            data = conn.recv(65536)
            if data:
                msg = json.loads(data.decode().strip())
                self._dispatch(msg)
        except:
            pass
        finally:
            conn.close()

    def send(self, target_id: int, msg: Dict):
        if self.delay > 0:
            time.sleep(self.delay)
        target = next((n for n in self.nodes if n["id"] == target_id), None)
        if not target:
            return
        try:
            with socket.create_connection((target["host"], target["port"]), timeout=2) as s:
                s.sendall((json.dumps(msg) + "\n").encode())
        except OSError as e:
            print(f"[Node {self.id}] Send error to {target_id}: {e}")

    def broadcast(self, msg: Dict):
        for n in self.nodes:
            self.send(n["id"], msg)

    # ==================== Message Dispatch ====================

    def _dispatch(self, msg: Dict):
        handlers = {
            "PREPARE": self._on_prepare, "PROMISE": self._on_promise,
            "ACCEPT": self._on_accept, "ACCEPTED": self._on_accepted,
            "DECIDE": self._on_decide, "SYNC_REQ": self._on_sync_req,
            "SYNC_RESP": self._on_sync_resp, "DEPTH_REQ": self._on_depth_req,
            "DEPTH_RESP": self._on_depth_resp,
        }
        handler = handlers.get(msg.get("type"))
        if handler:
            handler(msg)

    # ==================== Paxos Protocol ====================

    def _get_paxos(self, depth: int) -> PaxosState:
        if depth not in self.paxos:
            self.paxos[depth] = PaxosState()
        return self.paxos[depth]

    def _next_ballot(self, depth: int) -> Ballot:
        self.proposal_seq += 1
        self._save_state()
        return (self.proposal_seq, self.id, depth)

    def _on_prepare(self, msg: Dict):
        depth, ballot, sender = msg["depth"], tuple(msg["ballot"]), msg["from"]
        
        with self.lock:
            my_depth = self.blockchain.height()
            
            # Help stale proposers
            if depth < my_depth:
                print(f"[Node {self.id}] Stale PREPARE from {sender} (depth {depth} < {my_depth})")
                self.send(sender, {"type": "SYNC_RESP", "from": self.id, 
                                  "blockchain": self.blockchain.to_dict(), "depth": my_depth})
            
            ps = self._get_paxos(depth)
            if ps.promised_n and ballot < ps.promised_n:
                return  # Reject lower ballot
            
            ps.promised_n = ballot
            self._save_state()
            
            resp = {"type": "PROMISE", "from": self.id, "depth": depth, 
                   "ballot": list(ballot), "my_depth": my_depth}
            if ps.accepted_n and ps.accepted_block:
                resp["accepted_n"] = list(ps.accepted_n)
                resp["accepted_block"] = ps.accepted_block.to_dict()
        
        self.send(sender, resp)

    def _on_promise(self, msg: Dict):
        depth, ballot, sender = msg["depth"], tuple(msg["ballot"]), msg["from"]
        their_depth = msg.get("my_depth", depth)

        with self.lock:
            # Check if we're stale
            if their_depth > self.blockchain.height():
                self.send(sender, {"type": "SYNC_REQ", "from": self.id, 
                                  "my_depth": self.blockchain.height()})

            if self.current_ballot.get(depth) != ballot:
                return
            
            proms = self.promises.setdefault(depth, {})
            acc_n = tuple(msg["accepted_n"]) if "accepted_n" in msg else None
            acc_blk = Block.from_dict(msg["accepted_block"]) if "accepted_block" in msg else None
            proms[sender] = (acc_n, acc_blk)

            if depth in self.accept_started or len(proms) < self.majority:
                return

            print(f"[Node {self.id}] Got majority promises for depth={depth}")
            
            # Choose value per Paxos rule
            best_n, best_blk = None, None
            for an, ab in proms.values():
                if an and ab and (not best_n or an > best_n):
                    best_n, best_blk = an, ab
            
            block = best_blk or self.proposed_blocks.get(depth)
            if not block:
                return

            self.accept_started.add(depth)
            acc_msg = {"type": "ACCEPT", "from": self.id, "depth": depth,
                      "ballot": list(ballot), "block": block.to_dict()}

        # Leader sync in background
        threading.Thread(target=self._leader_sync, daemon=True).start()
        self.broadcast(acc_msg)

    def _on_accept(self, msg: Dict):
        depth, ballot = msg["depth"], tuple(msg["ballot"])
        block = Block.from_dict(msg["block"])

        with self.lock:
            ps = self._get_paxos(depth)
            if ps.promised_n and ballot < ps.promised_n:
                return
            
            ps.promised_n = ballot
            ps.accepted_n = ballot
            ps.accepted_block = block
            self._save_state()

        self.broadcast({"type": "ACCEPTED", "from": self.id, "depth": depth,
                       "ballot": list(ballot), "block": block.to_dict()})

    def _on_accepted(self, msg: Dict):
        depth, ballot = msg["depth"], tuple(msg["ballot"])
        key = (depth, ballot)

        with self.lock:
            self.accepted_counts[key] = self.accepted_counts.get(key, 0) + 1
            
            if ballot[1] != self.id or depth in self.decided:
                return
            if self.accepted_counts[key] < self.majority:
                return

            block = Block.from_dict(msg["block"])
            print(f"[Node {self.id}] DECIDED depth={depth}")
            self.decided.add(depth)
            self._save_state()
            self._commit(depth, block)

        self.broadcast({"type": "DECIDE", "from": self.id, "depth": depth, 
                       "block": block.to_dict()})

    def _on_decide(self, msg: Dict):
        depth, sender = msg["depth"], msg["from"]
        block = Block.from_dict(msg["block"])
        
        with self.lock:
            my_depth = self.blockchain.height()
            if depth > my_depth:
                print(f"[Node {self.id}] Stale! Need sync (decide depth={depth}, mine={my_depth})")
                self.send(sender, {"type": "SYNC_REQ", "from": self.id, "my_depth": my_depth})
                return

            if depth in self.decided:
                return
            
            print(f"[Node {self.id}] DECIDE received for depth={depth}")
            self.decided.add(depth)
            self._save_state()
            self._commit(depth, block)

    def _commit(self, depth: int, block: Block):
        """Commit a decided block."""
        block.tentative = False
        self.blockchain.append_block(depth, block)
        self._recompute_balances()
        save_blockchain(self.id, self.blockchain)

    # ==================== Recovery / Sync ====================

    def _on_sync_req(self, msg: Dict):
        sender, their_depth = msg["from"], msg.get("my_depth", 0)
        my_depth = self.blockchain.height()
        print(f"[Node {self.id}] SYNC_REQ from {sender} (theirs={their_depth}, mine={my_depth})")
        if my_depth > their_depth:
            self.send(sender, {"type": "SYNC_RESP", "from": self.id,
                              "blockchain": self.blockchain.to_dict(), "depth": my_depth})

    def _on_sync_resp(self, msg: Dict):
        sender, their_depth = msg["from"], msg["depth"]
        
        with self.lock:
            if their_depth <= self.blockchain.height():
                return
            
            print(f"[Node {self.id}] Syncing from {sender} (depth {self.blockchain.height()} -> {their_depth})")
            self.blockchain = Blockchain.from_dict(msg["blockchain"])
            for i, b in enumerate(self.blockchain.blocks):
                b.tentative = False
                self.decided.add(i)
            
            self._recompute_balances()
            save_blockchain(self.id, self.blockchain)
            self._save_state()
            print(f"[Node {self.id}] Sync complete! depth={self.blockchain.height()}")

    def _on_depth_req(self, msg: Dict):
        self.send(msg["from"], {"type": "DEPTH_RESP", "from": self.id,
                               "depth": self.blockchain.height(),
                               "blockchain": self.blockchain.to_dict()})

    def _on_depth_resp(self, msg: Dict):
        sender, depth = msg["from"], msg["depth"]
        
        with self.lock:
            if not self.awaiting_depths:
                return
            self.depth_responses[sender] = depth
            if "blockchain" in msg:
                self.received_chains[sender] = msg["blockchain"]
            
            if len(self.depth_responses) >= self.majority:
                self.awaiting_depths = False
                self._sync_stale_nodes()

    def _leader_sync(self):
        """Leader collects depths and syncs stale nodes."""
        with self.lock:
            self.awaiting_depths = True
            self.depth_responses = {}
            self.received_chains = {}
        self.broadcast({"type": "DEPTH_REQ", "from": self.id})

    def _sync_stale_nodes(self):
        """Send best blockchain to all stale nodes."""
        self.depth_responses[self.id] = self.blockchain.height()
        self.received_chains[self.id] = self.blockchain.to_dict()
        
        max_depth, best_id = 0, self.id
        for nid, d in self.depth_responses.items():
            if d > max_depth:
                max_depth, best_id = d, nid
        
        best_chain = self.received_chains.get(best_id)
        if not best_chain:
            return

        # Update self if needed
        if self.blockchain.height() < max_depth:
            self.blockchain = Blockchain.from_dict(best_chain)
            for i, b in enumerate(self.blockchain.blocks):
                b.tentative = False
                self.decided.add(i)
            self._recompute_balances()
            save_blockchain(self.id, self.blockchain)
            self._save_state()

        # Update stale nodes
        for nid, d in self.depth_responses.items():
            if d < max_depth and nid != self.id:
                print(f"[Node {self.id}] Syncing stale node {nid}")
                self.send(nid, {"type": "SYNC_RESP", "from": self.id,
                               "blockchain": best_chain, "depth": max_depth})

    # ==================== Commands ====================

    def run(self):
        print("Commands:\n"
              "  moneyTransfer <sender> <receiver> <amount>\n"
              "  printBlockchain\n"
              "  printBalance\n"
              "  failProcess\n"
              "  fixProcess")
        for line in sys.stdin:
            parts = line.strip().split()
            if not parts:
                continue
            cmd = parts[0]
            try:
                if cmd == "moneyTransfer" and len(parts) == 4:
                    self._transfer(int(parts[1]), int(parts[2]), int(parts[3]))
                elif cmd == "printBlockchain":
                    self.blockchain.print_chain()
                elif cmd == "printBalance":
                    self.print_balances()
                elif cmd == "failProcess":
                    print(f"[Node {self.id}] Failing...")
                    self._shutdown()
                    break
                elif cmd == "fixProcess":
                    self._fix()
                else:
                    print(f"Unknown: {cmd}")
            except Exception as e:
                print(f"Error: {e}")
    
    def print_balances(self):
        print("Balances:")
        for nid, bal in sorted(self.balances.items(), key=lambda x: int(x[0])):
            print(f"  {nid}: {bal}")

    def _transfer(self, sender: int, receiver: int, amount: int):
        # Per spec: sender must be this node
        if sender != self.id:
            return print(f"Error: sender must be this node ({self.id})")
        if sender == receiver:
            return print("Error: sender == receiver")
        if str(sender) not in self.balances or str(receiver) not in self.balances:
            return print("Error: invalid account")
        if self.balances[str(sender)] < amount:
            return print("Error: insufficient balance")

        depth = self.blockchain.height()
        tx = Transaction(sender, receiver, amount)
        block = Block.mine(depth, tx, self.blockchain.last_hash())
        
        print(f"[Node {self.id}] Proposing: {sender}->{receiver} ${amount} at depth={depth}")

        with self.lock:
            self.proposed_blocks[depth] = block
            ballot = self._next_ballot(depth)
            self.current_ballot[depth] = ballot
            self.promises[depth] = {}
            self.accept_started.discard(depth)

        self.broadcast({"type": "PREPARE", "from": self.id, "depth": depth, "ballot": list(ballot)})

    def _shutdown(self):
        self._save_state()
        self.running = False
        if self.sock:
            self.sock.close()

    def _fix(self):
        print(f"[Node {self.id}] Fixing - reloading state...")
        self.blockchain = load_blockchain(self.id)
        self.balances = load_balances(self.id)
        self._restore_state()
        
        if not self.running:
            self.running = True
            self.start()
        
        print(f"[Node {self.id}] Fixed! depth={self.blockchain.height()}")
        # Sync with random node
        other = [n["id"] for n in self.nodes if n["id"] != self.id]
        if other:
            self.send(random.choice(other), {"type": "SYNC_REQ", "from": self.id, 
                                             "my_depth": self.blockchain.height()})


def main():
    if len(sys.argv) != 2:
        print("Usage: python node.py <node_id>")
        sys.exit(1)
    node = Node(int(sys.argv[1]))
    node.start()
    node.run()


if __name__ == "__main__":
    main()
