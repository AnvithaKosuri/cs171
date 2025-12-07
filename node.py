import json
import socket
import threading
import time
import sys
import random
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
    save_paxos_state,
    load_paxos_state,
    init_paxos_state_if_missing,
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

        # Paxos state - try to restore from disk first
        init_paxos_state_if_missing(self.node_id)
        self.paxos_states: Dict[int, PaxosState] = {}  # depth -> PaxosState
        self.accepted_counts: Dict[Tuple[int, Tuple[int, int, int]], int] = {}
        self.current_ballot: Dict[int, Tuple[int, int, int]] = {}
        self.proposed_blocks: Dict[int, Block] = {}  # depth -> value we want
        self.promises: Dict[int, Dict[int, Tuple[Optional[Tuple[int, int, int]], Optional[Block]]]] = {}
        self.accept_phase_started: Set[int] = set()
        self.decided_depths: Set[int] = set()
        self.global_proposal_seq: int = 0
        
        # Restore Paxos state from disk if available
        self._restore_paxos_state()

        # Recovery state
        self.depth_responses: Dict[int, int] = {}  # node_id -> depth
        self.awaiting_depth_responses: bool = False
        self.pending_leader_ballot: Optional[Tuple[int, int, int]] = None
        self.syncing: bool = False

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
        self._persist_paxos_state()
        return (self.global_proposal_seq, self.node_id, depth)

    def _restore_paxos_state(self) -> None:
        """
        Restore Paxos state from disk after a crash/restart.
        """
        saved_state = load_paxos_state(self.node_id)
        if saved_state is None:
            print(f"[Node {self.node_id}] No saved Paxos state found, starting fresh")
            return
        
        # Restore decided_depths
        self.decided_depths = set(saved_state.get("decided_depths", []))
        
        # Restore global_proposal_seq
        self.global_proposal_seq = saved_state.get("global_proposal_seq", 0)
        
        # Restore paxos_states
        paxos_states_data = saved_state.get("paxos_states", {})
        for depth_str, state_dict in paxos_states_data.items():
            depth = int(depth_str)
            state = PaxosState()
            
            if state_dict.get("promised_n"):
                state.promised_n = tuple(state_dict["promised_n"])
            if state_dict.get("accepted_n"):
                state.accepted_n = tuple(state_dict["accepted_n"])
            if state_dict.get("accepted_block"):
                state.accepted_block = Block.from_dict(state_dict["accepted_block"])
            
            self.paxos_states[depth] = state
        
        print(f"[Node {self.node_id}] Restored Paxos state: "
              f"decided_depths={self.decided_depths}, "
              f"global_proposal_seq={self.global_proposal_seq}, "
              f"paxos_states for depths={list(self.paxos_states.keys())}")

    def _persist_paxos_state(self) -> None:
        """
        Save current Paxos state to disk for crash recovery.
        """
        save_paxos_state(
            self.node_id,
            self.paxos_states,
            self.decided_depths,
            self.global_proposal_seq,
        )

    # ------------- Command handling -------------

    def command_loop(self) -> None:
        print(
            "Commands:\n"
            "  moneyTransfer <sender> <receiver> <amount>\n"
            "  printBlockchain\n"
            "  printBalance\n"
            "  failProcess\n"
            "  fixProcess"
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
                elif cmd == "fixProcess":
                    print(f"[Node {self.node_id}] Restarting process...")
                    self.fix_process()
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
        elif mtype == "SYNC_REQUEST":
            self.handle_sync_request(msg)
        elif mtype == "SYNC_RESPONSE":
            self.handle_sync_response(msg)
        elif mtype == "DEPTH_REQUEST":
            self.handle_depth_request(msg)
        elif mtype == "DEPTH_RESPONSE":
            self.handle_depth_response(msg)
        else:
            print(f"[Node {self.node_id}] Unknown message type: {mtype}")

    # ------------- Paxos roles -------------

    def handle_prepare(self, msg: Dict[str, Any]) -> None:
        depth = int(msg["depth"])
        ballot = tuple(msg["ballot"])
        sender = int(msg["from"])

        with self.lock:
            my_depth = self.blockchain.height()
            
            # Check if the sender has a stale blockchain (their depth < our decided depth)
            # In this case, we should inform them they need to sync
            if depth < my_depth:
                # Sender's blockchain is stale - send them our blockchain
                print(f"[Node {self.node_id}] Received stale PREPARE from Node {sender} "
                      f"(their depth={depth}, my depth={my_depth}). Sending blockchain sync.")
                sync_msg = {
                    "type": "SYNC_RESPONSE",
                    "from": self.node_id,
                    "blockchain": self.blockchain.to_dict(),
                    "depth": my_depth,
                }
                # Still process the prepare but also send sync info
                self.send_to_node(sender, sync_msg)
            
            st = self.get_paxos_state(depth)
            if st.promised_n is None or compare_ballot(ballot, st.promised_n) >= 0:
                st.promised_n = ballot
                self._persist_paxos_state()  # Persist state change
                resp: Dict[str, Any] = {
                    "type": "PROMISE",
                    "from": self.node_id,
                    "to": sender,
                    "depth": depth,
                    "ballot": list(ballot),
                    "my_depth": my_depth,  # Include our depth in response
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
        responder_depth = int(msg.get("my_depth", depth))

        with self.lock:
            my_depth = self.blockchain.height()
            
            # Check if the responder has a longer blockchain - we might be stale
            if responder_depth > my_depth:
                print(f"[Node {self.node_id}] Detected during PROMISE that Node {sender} "
                      f"has longer blockchain (theirs={responder_depth}, mine={my_depth})")
                # Request sync from this node
                self.syncing = True
                sync_request = {
                    "type": "SYNC_REQUEST",
                    "from": self.node_id,
                    "my_depth": my_depth,
                }
                # Don't hold lock while sending
                need_sync = True
            else:
                need_sync = False
                
        if need_sync:
            self.send_to_node(sender, sync_request)
            # Continue processing the promise anyway

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

            # We just got majority of promises - we're becoming the leader!
            # Initiate depth collection to sync all nodes
            print(f"[Node {self.node_id}] Received majority promises - becoming leader for depth={depth}")
            should_collect_depths = True
            
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

        # Initiate leader depth collection in background
        if should_collect_depths:
            threading.Thread(target=self.initiate_leader_depth_collection, daemon=True).start()
            
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
                self._persist_paxos_state()  # Persist state change
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
            self._persist_paxos_state()  # Persist state change

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
        sender = int(msg["from"])
        print(f"[Node {self.node_id}] DECIDE received for depth={depth}")
        
        with self.lock:
            my_depth = self.blockchain.height()
            
            # Check if we're behind - the DECIDE depth is beyond our next expected block
            if depth > my_depth:
                # We missed some blocks - need to sync
                print(f"[Node {self.node_id}] Detected stale blockchain! "
                      f"(DECIDE depth={depth}, my depth={my_depth}). Requesting sync...")
                self.syncing = True
                
        # If we're stale, request sync from the leader
        if depth > my_depth:
            self.request_sync_from_node(sender)
            return  # Wait for sync to complete before applying this block
            
        with self.lock:
            if depth in self.decided_depths:
                return
            self.decided_depths.add(depth)
            self._persist_paxos_state()  # Persist state change
            self.mark_block_decided_and_apply(depth, block)

    # ------------- Recovery / Sync -------------

    def request_sync_from_node(self, target_id: int) -> None:
        """
        Request the full blockchain from a specific node.
        """
        print(f"[Node {self.node_id}] Requesting blockchain sync from Node {target_id}")
        sync_request = {
            "type": "SYNC_REQUEST",
            "from": self.node_id,
            "my_depth": self.blockchain.height(),
        }
        self.send_to_node(target_id, sync_request)

    def request_sync_from_random_node(self) -> None:
        """
        Request the full blockchain from a randomly selected node.
        """
        other_nodes = [n["id"] for n in self.nodes if n["id"] != self.node_id]
        if not other_nodes:
            print(f"[Node {self.node_id}] No other nodes available for sync")
            return
        target_id = random.choice(other_nodes)
        self.request_sync_from_node(target_id)

    def handle_sync_request(self, msg: Dict[str, Any]) -> None:
        """
        Respond to a sync request by sending our full blockchain.
        """
        sender = int(msg["from"])
        sender_depth = int(msg.get("my_depth", 0))
        
        with self.lock:
            my_depth = self.blockchain.height()
            
        print(f"[Node {self.node_id}] Received SYNC_REQUEST from Node {sender} "
              f"(their depth={sender_depth}, my depth={my_depth})")
        
        if my_depth > sender_depth:
            sync_response = {
                "type": "SYNC_RESPONSE",
                "from": self.node_id,
                "blockchain": self.blockchain.to_dict(),
                "depth": my_depth,
            }
            self.send_to_node(sender, sync_response)
        else:
            print(f"[Node {self.node_id}] Cannot help Node {sender} - my blockchain is not longer")

    def handle_sync_response(self, msg: Dict[str, Any]) -> None:
        """
        Handle a sync response containing a full blockchain.
        Update our local blockchain if the received one is longer.
        """
        sender = int(msg["from"])
        received_depth = int(msg["depth"])
        
        with self.lock:
            my_depth = self.blockchain.height()
            
            if received_depth <= my_depth:
                print(f"[Node {self.node_id}] Ignoring SYNC_RESPONSE from Node {sender} "
                      f"- not longer than mine (received={received_depth}, mine={my_depth})")
                self.syncing = False
                return
            
            print(f"[Node {self.node_id}] Applying SYNC_RESPONSE from Node {sender} "
                  f"(updating from depth={my_depth} to depth={received_depth})")
            
            # Parse and apply the received blockchain
            received_blockchain = Blockchain.from_dict(msg["blockchain"])
            
            # Update our blockchain with the received blocks
            self.blockchain = received_blockchain
            
            # Mark all received blocks as decided and update decided_depths
            for i, block in enumerate(self.blockchain.blocks):
                block.tentative = False
                self.decided_depths.add(i)
            
            # Recompute balances from scratch
            new_balances: Dict[str, int] = {}
            for nid in self.balances.keys():
                new_balances[nid] = 100
            
            for b in self.blockchain.blocks:
                if not b.tentative:
                    apply_decided_block_to_balances(b, new_balances)
            
            self.balances = new_balances
            save_balances(self.node_id, self.balances)
            save_blockchain(self.node_id, self.blockchain)
            self._persist_paxos_state()  # Persist state change
            
            self.syncing = False
            
            print(f"[Node {self.node_id}] Sync complete! New blockchain depth={self.blockchain.height()}")

    def handle_depth_request(self, msg: Dict[str, Any]) -> None:
        """
        Respond to a depth request from a leader collecting blockchain depths.
        """
        sender = int(msg["from"])
        
        with self.lock:
            my_depth = self.blockchain.height()
        
        depth_response = {
            "type": "DEPTH_RESPONSE",
            "from": self.node_id,
            "depth": my_depth,
            "blockchain": self.blockchain.to_dict(),
        }
        self.send_to_node(sender, depth_response)

    def handle_depth_response(self, msg: Dict[str, Any]) -> None:
        """
        Handle depth responses when acting as leader.
        Collect depths and update stale nodes once we have majority.
        """
        sender = int(msg["from"])
        depth = int(msg["depth"])
        
        with self.lock:
            if not self.awaiting_depth_responses:
                return
            
            self.depth_responses[sender] = depth
            
            # Also store the blockchain if provided
            if "blockchain" in msg:
                # Store for potential use in updating stale nodes
                if not hasattr(self, 'received_blockchains'):
                    self.received_blockchains: Dict[int, Dict[str, Any]] = {}
                self.received_blockchains[sender] = msg["blockchain"]
            
            print(f"[Node {self.node_id}] Received DEPTH_RESPONSE from Node {sender}: depth={depth}")
            
            # Check if we have enough responses
            if len(self.depth_responses) >= self.majority:
                self.awaiting_depth_responses = False
                self._perform_leader_sync()

    def _perform_leader_sync(self) -> None:
        """
        As leader, find the longest blockchain and update all stale nodes.
        """
        print(f"[Node {self.node_id}] Leader performing blockchain sync across nodes")
        
        # Include our own depth
        self.depth_responses[self.node_id] = self.blockchain.height()
        if not hasattr(self, 'received_blockchains'):
            self.received_blockchains = {}
        self.received_blockchains[self.node_id] = self.blockchain.to_dict()
        
        # Find the node with the longest blockchain
        max_depth = 0
        best_node = self.node_id
        for node_id, depth in self.depth_responses.items():
            if depth > max_depth:
                max_depth = depth
                best_node = node_id
        
        print(f"[Node {self.node_id}] Best blockchain: Node {best_node} with depth={max_depth}")
        
        # Get the best blockchain
        best_blockchain_dict = self.received_blockchains.get(best_node)
        if best_blockchain_dict is None:
            print(f"[Node {self.node_id}] Warning: No blockchain available from best node")
            return
        
        # Update ourselves if we're behind
        if self.blockchain.height() < max_depth:
            print(f"[Node {self.node_id}] Updating own blockchain from Node {best_node}")
            received_blockchain = Blockchain.from_dict(best_blockchain_dict)
            self.blockchain = received_blockchain
            for i, block in enumerate(self.blockchain.blocks):
                block.tentative = False
                self.decided_depths.add(i)
            
            # Recompute balances
            new_balances: Dict[str, int] = {}
            for nid in self.balances.keys():
                new_balances[nid] = 100
            for b in self.blockchain.blocks:
                if not b.tentative:
                    apply_decided_block_to_balances(b, new_balances)
            self.balances = new_balances
            save_balances(self.node_id, self.balances)
            save_blockchain(self.node_id, self.blockchain)
            self._persist_paxos_state()  # Persist state change
        
        # Send the best blockchain to all nodes that are behind
        for node_id, depth in self.depth_responses.items():
            if depth < max_depth and node_id != self.node_id:
                print(f"[Node {self.node_id}] Sending updated blockchain to stale Node {node_id} "
                      f"(their depth={depth}, best depth={max_depth})")
                sync_msg = {
                    "type": "SYNC_RESPONSE",
                    "from": self.node_id,
                    "blockchain": best_blockchain_dict,
                    "depth": max_depth,
                }
                self.send_to_node(node_id, sync_msg)
        
        # Clear state
        self.depth_responses = {}
        self.received_blockchains = {}

    def initiate_leader_depth_collection(self) -> None:
        """
        Called when a node becomes leader - collect depth info from all nodes.
        """
        print(f"[Node {self.node_id}] Initiating depth collection as leader")
        
        with self.lock:
            self.awaiting_depth_responses = True
            self.depth_responses = {}
            if hasattr(self, 'received_blockchains'):
                self.received_blockchains = {}
        
        depth_request = {
            "type": "DEPTH_REQUEST",
            "from": self.node_id,
        }
        self.broadcast(depth_request)

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

    # ------------- Shutdown / Recovery -------------

    def shutdown(self) -> None:
        """
        Gracefully shutdown the node, persisting state before exit.
        """
        # Persist state before shutdown
        self._persist_paxos_state()
        
        self.running = False
        if self.server_socket is not None:
            try:
                self.server_socket.close()
            except OSError:
                pass

    def fix_process(self) -> None:
        """
        Restart the process after failure.
        Reloads state from disk and restarts the listener.
        Also initiates sync with other nodes to catch up on missed blocks.
        """
        print(f"[Node {self.node_id}] Fixing process - reloading state from disk...")
        
        with self.lock:
            # Reload blockchain and balances from disk
            self.blockchain = load_blockchain(self.node_id)
            self.balances = load_balances(self.node_id)
            
            # Reload Paxos state from disk
            self._restore_paxos_state()
        
        # Restart listener if it's not running
        if not self.running or self.server_socket is None:
            self.running = True
            self.start_listener_thread()
        
        print(f"[Node {self.node_id}] State reloaded. Blockchain depth={self.blockchain.height()}")
        print(f"[Node {self.node_id}] Requesting sync from other nodes to catch up...")
        
        # Request sync from a random node to catch up on any missed blocks
        self.request_sync_from_random_node()


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
