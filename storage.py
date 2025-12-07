import json
import os
from typing import Dict, Any, Set, Optional
from blockchain import Blockchain, Block


def _filename(node_id: int, prefix: str) -> str:
    return f"{prefix}_{node_id}.json"


def _load_json(fname: str) -> Optional[Dict]:
    if not os.path.exists(fname):
        return None
    try:
        with open(fname, "r") as f:
            return json.load(f)
    except Exception:
        return None


def _save_json(fname: str, data: Dict) -> None:
    with open(fname, "w") as f:
        json.dump(data, f, indent=2)


# ============ Balances ============

def init_balances(node_id: int, num_nodes: int, initial: int = 100) -> None:
    fname = _filename(node_id, "balances")
    balances = _load_json(fname) or {}
    for i in range(1, num_nodes + 1):
        balances.setdefault(str(i), initial)
    _save_json(fname, balances)


def load_balances(node_id: int) -> Dict[str, int]:
    return _load_json(_filename(node_id, "balances")) or {}


def save_balances(node_id: int, balances: Dict[str, int]) -> None:
    _save_json(_filename(node_id, "balances"), balances)


# ============ Blockchain ============

def init_blockchain(node_id: int) -> None:
    fname = _filename(node_id, "blockchain")
    if not os.path.exists(fname):
        _save_json(fname, {"blocks": []})


def load_blockchain(node_id: int) -> Blockchain:
    data = _load_json(_filename(node_id, "blockchain"))
    return Blockchain.from_dict(data) if data else Blockchain()


def save_blockchain(node_id: int, bc: Blockchain) -> None:
    _save_json(_filename(node_id, "blockchain"), bc.to_dict())


# ============ Paxos State ============

def init_paxos_state(node_id: int) -> None:
    fname = _filename(node_id, "paxos_state")
    if not os.path.exists(fname):
        _save_json(fname, {"paxos_states": {}, "decided_depths": [], "proposal_seq": 0})


def load_paxos_state(node_id: int) -> Optional[Dict]:
    return _load_json(_filename(node_id, "paxos_state"))


def save_paxos_state(node_id: int, paxos_states: Dict, decided: Set[int], seq: int) -> None:
    serialized = {}
    for depth, st in paxos_states.items():
        serialized[str(depth)] = {
            "promised_n": list(st.promised_n) if st.promised_n else None,
            "accepted_n": list(st.accepted_n) if st.accepted_n else None,
            "accepted_block": st.accepted_block.to_dict() if st.accepted_block else None,
        }
    _save_json(_filename(node_id, "paxos_state"), {
        "paxos_states": serialized, "decided_depths": list(decided), "proposal_seq": seq
    })


# ============ Helpers ============

def apply_block(block: Block, balances: Dict[str, int]) -> None:
    """Apply a decided block's transaction to balances."""
    tx = block.transaction
    s, r = str(tx.sender_id), str(tx.receiver_id)
    if s in balances and r in balances:
        balances[s] -= tx.amount
        balances[r] += tx.amount
