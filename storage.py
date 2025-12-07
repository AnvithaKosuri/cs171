import json
import os
from typing import Dict, Any, Set, List, Tuple, Optional

from blockchain import Blockchain, Block, Transaction


def balances_filename(node_id: int) -> str:
    return f"balances_{node_id}.json"


def blockchain_filename(node_id: int) -> str:
    return f"blockchain_{node_id}.json"


def paxos_state_filename(node_id: int) -> str:
    return f"paxos_state_{node_id}.json"


def init_balances_if_missing(node_id: int, num_nodes: int, initial_balance: int = 100) -> None:
    """
    Initialize balances file if it does not already exist.
    Each node starts with the same initial balance.
    If the file exists but has fewer accounts than num_nodes, it is expanded.
    """
    fname = balances_filename(node_id)
    balances: Dict[str, int] = {}
    if os.path.exists(fname):
        with open(fname, "r", encoding="utf-8") as f:
            try:
                balances = json.load(f)
            except Exception:
                balances = {}

    changed = False
    for i in range(1, num_nodes + 1):
        key = str(i)
        if key not in balances:
            balances[key] = initial_balance
            changed = True

    if not os.path.exists(fname) or changed:
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(balances, f, indent=2)


def load_balances(node_id: int) -> Dict[str, int]:
    fname = balances_filename(node_id)
    if not os.path.exists(fname):
        raise FileNotFoundError(f"Balances file {fname} not found.")
    with open(fname, "r", encoding="utf-8") as f:
        return json.load(f)


def save_balances(node_id: int, balances: Dict[str, int]) -> None:
    fname = balances_filename(node_id)
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(balances, f, indent=2)


def init_blockchain_if_missing(node_id: int) -> None:
    fname = blockchain_filename(node_id)
    if os.path.exists(fname):
        return
    bc = Blockchain([])
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(bc.to_dict(), f, indent=2)


def load_blockchain(node_id: int) -> Blockchain:
    fname = blockchain_filename(node_id)
    if not os.path.exists(fname):
        raise FileNotFoundError(f"Blockchain file {fname} not found.")
    with open(fname, "r", encoding="utf-8") as f:
        data: Any = json.load(f)
    return Blockchain.from_dict(data)


def save_blockchain(node_id: int, blockchain: Blockchain) -> None:
    fname = blockchain_filename(node_id)
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(blockchain.to_dict(), f, indent=2)


def apply_decided_block_to_balances(block: Block, balances: Dict[str, int]) -> None:
    """
    Apply the money transfer in a decided block to the balances map.
    Assumes balances keys are node ids as strings.
    """
    tx = block.transaction
    s = str(tx.sender_id)
    r = str(tx.receiver_id)
    amt = tx.amount

    if s not in balances or r not in balances:
        return

    balances[s] -= amt
    balances[r] += amt


def save_paxos_state(
    node_id: int,
    paxos_states: Dict[int, Any],
    decided_depths: Set[int],
    global_proposal_seq: int,
) -> None:
    """
    Save Paxos state to disk for crash recovery.
    """
    fname = paxos_state_filename(node_id)
    
    # Convert paxos_states to serializable format
    paxos_states_serialized: Dict[str, Any] = {}
    for depth, state in paxos_states.items():
        state_dict: Dict[str, Any] = {
            "promised_n": list(state.promised_n) if state.promised_n else None,
            "accepted_n": list(state.accepted_n) if state.accepted_n else None,
            "accepted_block": state.accepted_block.to_dict() if state.accepted_block else None,
        }
        paxos_states_serialized[str(depth)] = state_dict
    
    data = {
        "paxos_states": paxos_states_serialized,
        "decided_depths": list(decided_depths),
        "global_proposal_seq": global_proposal_seq,
    }
    
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_paxos_state(node_id: int) -> Optional[Dict[str, Any]]:
    """
    Load Paxos state from disk.
    Returns None if no state file exists.
    """
    fname = paxos_state_filename(node_id)
    if not os.path.exists(fname):
        return None
    
    try:
        with open(fname, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def init_paxos_state_if_missing(node_id: int) -> None:
    """
    Initialize an empty Paxos state file if it doesn't exist.
    """
    fname = paxos_state_filename(node_id)
    if os.path.exists(fname):
        return
    
    data = {
        "paxos_states": {},
        "decided_depths": [],
        "global_proposal_seq": 0,
    }
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
