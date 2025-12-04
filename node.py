import json
import socket
import threading
import time
import sys
from typing import Dict, Any, Tuple, Optional, List, Set

from blockchain import Blockchain, Block, Transaction
from storage import (
    init_balances_if_missing,
    init_blockchain_if_missing,
    load_balances,
    save_balances,
    load_blockchain,
    save_blockchain,
    apply_decided_block_to_balances,
)
from paxos import PaxosState, compare_ballot


CONFIG_FILE = "config.json"


class Node:
    """
    A single blockchain node that participates in Paxos for each block depth.
    It acts as proposer, acceptor, and learner.
    """

    def __init__(self, node_id: int, config_path: str = CONFIG_FILE) -> None:
        self.node_id = node_id
        self.config_path = config_path

        # Load configuration
        with open(self.config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        self.nodes: List[Dict[str, Any]] = cfg["nodes"]
        self.delay: float = cfg.get("message_delay_sec", 0.0)

        self.me = next(n for n in self.nodes if n["id"] == self.node_id)
        self.num_nodes = len(self.nodes)
        self.majority = self.num_nodes // 2 + 1

        # Persistent state
        init_balances_if_missing(self.node_id, self.num_nodes)
        init_blockchain_if_missing(self.node_id)
        self.balances: Dict[str, int] = load_balances(self.node_id)
        self.blockchain: Blockchain = load_blockchain(self.node_id)

        # Paxos state
        self.paxos_states: Dict[int, PaxosState] = {}  # depth -> PaxosState
        self.accepted_counts: Dict[Tuple[int, Tuple[int, int, int]], int] = {}
        self.current_ballot: Dict[int, Tuple[int, int, int]] = {}
        self.proposed_blocks: Dict[int, Block] = {}  # depth -> value we want
        self.promises: Dict[int, Dict[int, Tuple[Optional[Tuple[int, int, int]], Optional[Block]]]] = {}
        self.accept_phase_started: Set[int] = set()
        self.decided_depths: Set[int] = set()
        self.global_proposal_seq: int = 0

        # Networking
        self.server_socket: Optional[socket.socket] = None
        self.listener_thread: Optional[threading.Thread] = None
        self.running = True

        # Lock for all mutable shared state
        self.lock = threading.Lock()

        print(f"[Node {self.node_id}] Initialized. Listening on {self.me['host']}:{self.me['port']}")

    # ------------- Networking helpers -------------

    def start_listener_thread(self) -> None:
        """
        Start background thread that listens for incoming TCP connections.
        Each connection carries a single JSON message.
        """
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.me["host"], self.me["port"]))
        self.server_socket.listen()

        def _listen() -> None:
            print(f"[Node {self.node_id}] Listener started.")
            while self.running:
                try:
                    client_sock, _ = self.server_socket.accept()
                except OSError:
                    break  # Socket closed
                t = threading.Thread(target=self._handle_client, args=(client_sock,))
                t.daemon = True
                t.start()
            print(f"[Node {self.node_id}] Listener thread exiting.")

        self.listener_thread = threading.Thread(target=_listen, daemon=True)
        self.listener_thread.start()

    def _handle_client(self, client_sock: socket.socket) -> None:
        try:
            data = client_sock.recv(65536)
            if not data:
                return
            msg_str = data.decode("utf-8").strip()
            if not msg_str:
                return
            try:
                msg = json.loads(msg_str)
            except json.JSONDecodeError as e:
                print(f"[Node {self.node_id}] Failed to decode JSON: {e}")
                return
            self.handle_message(msg)
        finally:
            try:
                client_sock.close()
            except OSError:
                pass

    def send_to_node(self, target_id: int, msg: Dict[str, Any]) -> None:
        """
        Send a single JSON message to another node over TCP.
        """
        if self.delay > 0:
            time.sleep(self.delay)

        target = next((n for n in self.nodes if n["id"] == target_id), None)
        if target is None:
            print(f"[Node {self.node_id}] Unknown target id {target_id}")
            return

        host = target["host"]
        port = target["port"]
        try:
            with socket.create_connection((host, port), timeout=2.0) as s:
                s.sendall((json.dumps(msg) + "\n").encode("utf-8"))
        except OSError as e:
            print(f"[Node {self.node_id}] Error sending to node {target_id}: {e}")

    def broadcast(self, msg: Dict[str, Any]) -> None:
        for n in self.nodes:
            self.send_to_node(n["id"], msg)

    # ------------- Paxos helpers -------------

    def get_paxos_state(self, depth: int) -> PaxosState:
        st = self.paxos_states.get(depth)
        if st is None:
            st = PaxosState()
            self.paxos_states[depth] = st
        return st

    def next_ballot(self, depth: int) -> Tuple[int, int, int]:
        self.global_proposal_seq += 1
        return (self.global_proposal_seq, self.node_id, depth)

    # ------------- Command handling -------------

    def command_loop(self) -> None:
        print(
            "Commands:\n"
            "  moneyTransfer <sender> <receiver> <amount>\n"
            "  printBlockchain\n"
            "  printBalance\n"
            "  failProcess"
        )
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            cmd = parts[0]
            try:
                if cmd == "moneyTransfer":
                    if len(parts) != 4:
                        print("Usage: moneyTransfer <sender> <receiver> <amount>")
                        continue
                    s = int(parts[1])
                    r = int(parts[2])
                    amt = int(parts[3])
                    self.handle_money_transfer(s, r, amt)
                elif cmd == "printBlockchain":
                    self.blockchain.print_chain()
                elif cmd == "printBalance":
                    self.print_balances()
                elif cmd == "failProcess":
                    print(f"[Node {self.node_id}] Failing process as requested.")
                    self.shutdown()
                    break
                else:
                    print(f"Unknown command: {cmd}")
            except Exception as e:
                print(f"[Node {self.node_id}] Error handling command '{line}': {e}")

    def print_balances(self) -> None:
        print("Balances:")
        for nid, bal in sorted(self.balances.items(), key=lambda x: int(x[0])):
            print(f"  {nid}: {bal}")

    # ------------- High-level transaction entry -------------

    def handle_money_transfer(self, sender_id: int, receiver_id: int, amount: int) -> None:
        """
        Initiate a new Paxos instance for the next block depth.
        """
        if sender_id == receiver_id:
            print("[Error] Sender and receiver must be different.")
            return

        s_key = str(sender_id)
        r_key = str(receiver_id)
        if s_key not in self.balances or r_key not in self.balances:
            print("[Error] Unknown account id(s).")
            return

        if self.balances[s_key] < amount:
            print("[Error] Insufficient balance.")
            return

        depth = len(self.blockchain)
        print(f"[Node {self.node_id}] Initiating Paxos for depth={depth} tx {sender_id}->{receiver_id}:{amount}")

        tx = Transaction(sender_id=sender_id, receiver_id=receiver_id, amount=amount)
        prev_hash = self.blockchain.last_hash()
        block = Block.mine(index=depth, tx=tx, prev_hash=prev_hash)

        with self.lock:
            self.proposed_blocks[depth] = block
            ballot = self.next_ballot(depth)
            self.current_ballot[depth] = ballot
            self.promises[depth] = {}
            if depth in self.accept_phase_started:
                self.accept_phase_started.remove(depth)

        prepare_msg = {
            "type": "PREPARE",
            "from": self.node_id,
            "depth": depth,
            "ballot": list(ballot),
        }
        self.broadcast(prepare_msg)

    # ------------- Message dispatch -------------

    def handle_message(self, msg: Dict[str, Any]) -> None:
        mtype = msg.get("type")
        if mtype == "PREPARE":
            self.handle_prepare(msg)
        elif mtype == "PROMISE":
            self.handle_promise(msg)
        elif mtype == "ACCEPT":
            self.handle_accept(msg)
        elif mtype == "ACCEPTED":
            self.handle_accepted(msg)
        elif mtype == "DECIDE":
            self.handle_decide(msg)
        else:
            print(f"[Node {self.node_id}] Unknown message type: {mtype}")

    # ------------- Paxos roles -------------

    def handle_prepare(self, msg: Dict[str, Any]) -> None:
        depth = int(msg["depth"])
        ballot = tuple(msg["ballot"])
        sender = int(msg["from"])

        with self.lock:
            st = self.get_paxos_state(depth)
            if st.promised_n is None or compare_ballot(ballot, st.promised_n) >= 0:
                st.promised_n = ballot
                resp: Dict[str, Any] = {
                    "type": "PROMISE",
                    "from": self.node_id,
                    "to": sender,
                    "depth": depth,
                    "ballot": list(ballot),
                }
                if st.accepted_n is not None and st.accepted_block is not None:
                    resp["accepted_n"] = list(st.accepted_n)
                    resp["accepted_block"] = st.accepted_block.to_dict()
            else:
                # Ignore lower ballot
                return

        self.send_to_node(sender, resp)

    def handle_promise(self, msg: Dict[str, Any]) -> None:
        depth = int(msg["depth"])
        ballot = tuple(msg["ballot"])
        sender = int(msg["from"])

        with self.lock:
            cur_ballot = self.current_ballot.get(depth)
            if cur_ballot is None or cur_ballot != ballot:
                return

            prom_dict = self.promises.setdefault(depth, {})
            accepted_n = None
            accepted_block = None
            if "accepted_n" in msg and "accepted_block" in msg:
                accepted_n = tuple(msg["accepted_n"])
                accepted_block = Block.from_dict(msg["accepted_block"])
            prom_dict[sender] = (accepted_n, accepted_block)

            if depth in self.accept_phase_started:
                return

            if len(prom_dict) < self.majority:
                return

            # Choose value to propose according to Paxos rule.
            best_n: Optional[Tuple[int, int, int]] = None
            best_block: Optional[Block] = None
            for acc_n, acc_block in prom_dict.values():
                if acc_n is not None and acc_block is not None:
                    if best_n is None or compare_ballot(acc_n, best_n) > 0:
                        best_n = acc_n
                        best_block = acc_block

            if best_block is None:
                best_block = self.proposed_blocks.get(depth)
                if best_block is None:
                    return

            self.accept_phase_started.add(depth)
            acc_msg = {
                "type": "ACCEPT",
                "from": self.node_id,
                "depth": depth,
                "ballot": list(ballot),
                "block": best_block.to_dict(),
            }

        self.broadcast(acc_msg)

    def handle_accept(self, msg: Dict[str, Any]) -> None:
        depth = int(msg["depth"])
        ballot = tuple(msg["ballot"])
        block_dict = msg["block"]
        block = Block.from_dict(block_dict)

        with self.lock:
            st = self.get_paxos_state(depth)
            if st.promised_n is None or compare_ballot(ballot, st.promised_n) >= 0:
                st.promised_n = ballot
                st.accepted_n = ballot
                st.accepted_block = block
            else:
                return

        accepted_msg = {
            "type": "ACCEPTED",
            "from": self.node_id,
            "depth": depth,
            "ballot": list(ballot),
            "block": block.to_dict(),
        }
        self.broadcast(accepted_msg)

    def handle_accepted(self, msg: Dict[str, Any]) -> None:
        depth = int(msg["depth"])
        ballot = tuple(msg["ballot"])

        key = (depth, ballot)

        with self.lock:
            current = self.accepted_counts.get(key, 0) + 1
            self.accepted_counts[key] = current

            _, leader_id, _ = ballot
            if self.node_id != leader_id:
                return

            if depth in self.decided_depths:
                return

            if current < self.majority:
                return

            block_dict = msg.get("block")
            if block_dict is None:
                return
            block = Block.from_dict(block_dict)

            print(f"[Node {self.node_id}] DECISION READY at depth={depth}")
            self.decided_depths.add(depth)

            self.mark_block_decided_and_apply(depth, block)

            decide_msg = {
                "type": "DECIDE",
                "from": self.node_id,
                "depth": depth,
                "block": block.to_dict(),
            }

        print(f"[Node {self.node_id}] Broadcasting DECIDE depth={depth}")
        self.broadcast(decide_msg)

    def handle_decide(self, msg: Dict[str, Any]) -> None:
        depth = int(msg["depth"])
        block = Block.from_dict(msg["block"])
        print(f"[Node {self.node_id}] DECIDE received for depth={depth}")
        with self.lock:
            if depth in self.decided_depths:
                return
            self.decided_depths.add(depth)
            self.mark_block_decided_and_apply(depth, block)

    # ------------- Commit / balances -------------

    def mark_block_decided_and_apply(self, depth: int, block: Block) -> None:
        """
        Integrate a decided block into the local blockchain and recompute balances.
        """
        block.tentative = False
        self.blockchain.ensure_block_at_depth(depth, block)

        new_balances: Dict[str, int] = {}
        for nid in self.balances.keys():
            new_balances[nid] = 100

        for b in self.blockchain.blocks:
            if not b.tentative:
                apply_decided_block_to_balances(b, new_balances)

        self.balances = new_balances
        save_balances(self.node_id, self.balances)
        save_blockchain(self.node_id, self.blockchain)

    # ------------- Shutdown -------------

    def shutdown(self) -> None:
        self.running = False
        if self.server_socket is not None:
            try:
                self.server_socket.close()
            except OSError:
                pass


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python node.py <node_id>")
        sys.exit(1)
    node_id = int(sys.argv[1])
    node = Node(node_id)
    node.start_listener_thread()
    node.command_loop()


if __name__ == "__main__":
    main()
