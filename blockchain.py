import hashlib
import random
import string
from dataclasses import dataclass
from typing import Dict, Any, List, Optional


@dataclass
class Transaction:
    sender_id: int
    receiver_id: int
    amount: int

    def to_dict(self) -> Dict[str, Any]:
        return {"sender_id": self.sender_id, "receiver_id": self.receiver_id, "amount": self.amount}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Transaction":
        return Transaction(int(d["sender_id"]), int(d["receiver_id"]), int(d["amount"]))

    def as_string(self) -> str:
        return f"{self.sender_id}->{self.receiver_id}:{self.amount}"


@dataclass
class Block:
    index: int
    transaction: Transaction
    nonce: str
    prev_hash: str
    hash_value: str
    tentative: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index, "transaction": self.transaction.to_dict(),
            "nonce": self.nonce, "prev_hash": self.prev_hash,
            "hash_value": self.hash_value, "tentative": self.tentative,
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Block":
        return Block(
            index=int(d["index"]), transaction=Transaction.from_dict(d["transaction"]),
            nonce=d["nonce"], prev_hash=d["prev_hash"],
            hash_value=d["hash_value"], tentative=bool(d.get("tentative", False)),
        )

    @staticmethod
    def mine(index: int, tx: Transaction, prev_hash: str) -> "Block":
        """
        Find nonce such that SHA256(tx || nonce) ends with digit 0-4.
        Hash pointer = SHA256(prev_tx || prev_nonce || prev_hash)
        """
        tx_str = tx.as_string()
        while True:
            nonce = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
            h = hashlib.sha256(f"{tx_str}||{nonce}".encode()).hexdigest()
            if h[-1] in "01234":
                print(f"[Mining] Found valid nonce={nonce}, hash={h}")
                return Block(index=index, transaction=tx, nonce=nonce,
                           prev_hash=prev_hash, hash_value=h, tentative=True)


class Blockchain:
    def __init__(self, blocks: Optional[List[Block]] = None):
        self.blocks: List[Block] = blocks or []

    def to_dict(self) -> Dict[str, Any]:
        return {"blocks": [b.to_dict() for b in self.blocks]}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Blockchain":
        return Blockchain([Block.from_dict(bd) for bd in d.get("blocks", [])])

    def last_hash(self) -> str:
        return self.blocks[-1].hash_value if self.blocks else "GENESIS"

    def height(self) -> int:
        return len(self.blocks)

    def append_block(self, depth: int, block: Block) -> None:
        """Insert or replace block at depth."""
        if depth < len(self.blocks):
            self.blocks[depth] = block
        elif depth == len(self.blocks):
            self.blocks.append(block)
        else:
            # Pad with dummy blocks if needed
            while len(self.blocks) < depth:
                dummy = Block(len(self.blocks), Transaction(0, 0, 0), "0",
                            self.blocks[-1].hash_value if self.blocks else "GENESIS", "DUMMY", False)
                self.blocks.append(dummy)
            self.blocks.append(block)

    def print_chain(self) -> None:
        print("=" * 50)
        print("BLOCKCHAIN")
        print("=" * 50)
        if not self.blocks:
            print("(empty)")
        for b in self.blocks:
            status = "TENTATIVE" if b.tentative else "DECIDED"
            tx = b.transaction
            print(f"[{b.index}] {status} | Tx({tx.sender_id}->{tx.receiver_id}, ${tx.amount}) | "
                  f"nonce={b.nonce} | hash=...{b.hash_value[-8:]} | prev=...{b.prev_hash[-8:] if b.prev_hash != 'GENESIS' else 'GENESIS'}")
        print("=" * 50)

    def __len__(self) -> int:
        return len(self.blocks)
