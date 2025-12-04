import json
import os
from typing import Dict, Any

from blockchain import Blockchain, Block, Transaction


def balances_filename(node_id: int) -> str:
    return f"balances_{node_id}.json"


def blockchain_filename(node_id: int) -> str:
    return f"blockchain_{node_id}.json"


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
