import hashlib
from dataclasses import dataclass
from typing import Dict, Any, List, Optional


@dataclass
class Transaction:
    sender_id: int
    receiver_id: int
    amount: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "amount": self.amount,
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Transaction":
        return Transaction(
            sender_id=int(d["sender_id"]),
            receiver_id=int(d["receiver_id"]),
            amount=int(d["amount"]),
        )

    def as_string(self) -> str:
        return f"{self.sender_id}->{self.receiver_id}:{self.amount}"


@dataclass
class Block:
    index: int
    transaction: Transaction
    nonce: int
    prev_hash: str
    hash_value: str
    tentative: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "transaction": self.transaction.to_dict(),
            "nonce": self.nonce,
            "prev_hash": self.prev_hash,
            "hash_value": self.hash_value,
            "tentative": self.tentative,
        }

    @staticmethod
    def compute_hash(prev_hash: str, tx: Transaction, nonce: int) -> str:
        data = f"{prev_hash}|{tx.as_string()}|{nonce}".encode("utf-8")
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Block":
        tx = Transaction.from_dict(d["transaction"])
        return Block(
            index=int(d["index"]),
            transaction=tx,
            nonce=int(d["nonce"]),
            prev_hash=d["prev_hash"],
            hash_value=d["hash_value"],
            tentative=bool(d.get("tentative", False)),
        )

    @staticmethod
    def mine(index: int, tx: Transaction, prev_hash: str, difficulty: int = 2) -> "Block":
        """
        Very small proof-of-work just so nonce/hash look reasonable.
        Finds a nonce such that hash starts with `difficulty` zeros.
        """
        prefix = "0" * difficulty
        nonce = 0
        while True:
            h = Block.compute_hash(prev_hash, tx, nonce)
            if h.startswith(prefix):
                return Block(
                    index=index,
                    transaction=tx,
                    nonce=nonce,
                    prev_hash=prev_hash,
                    hash_value=h,
                    tentative=True,
                )
            nonce += 1


class Blockchain:
    def __init__(self, blocks: Optional[List[Block]] = None):
        self.blocks: List[Block] = blocks or []

    def to_dict(self) -> Dict[str, Any]:
        return {"blocks": [b.to_dict() for b in self.blocks]}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Blockchain":
        blocks_data = d.get("blocks", [])
        blocks = [Block.from_dict(bd) for bd in blocks_data]
        return Blockchain(blocks)

    def last_hash(self) -> str:
        if not self.blocks:
            return "GENESIS"
        return self.blocks[-1].hash_value

    def height(self) -> int:
        return len(self.blocks)

    def ensure_block_at_depth(self, depth: int, block: Block) -> None:
        """
        Insert or replace a block at the given depth.
        """
        if depth < len(self.blocks):
            self.blocks[depth] = block
        elif depth == len(self.blocks):
            self.blocks.append(block)
        else:
            # Depth should not skip; but if it does, pad with dummy decided blocks
            # (these won't affect balances because amount = 0).
            while len(self.blocks) < depth:
                dummy_tx = Transaction(sender_id=0, receiver_id=0, amount=0)
                dummy_block = Block(
                    index=len(self.blocks),
                    transaction=dummy_tx,
                    nonce=0,
                    prev_hash=self.blocks[-1].hash_value if self.blocks else "GENESIS",
                    hash_value="DUMMY",
                    tentative=False,
                )
                self.blocks.append(dummy_block)
            self.blocks.append(block)

    def print_chain(self) -> None:
        print("====== BLOCKCHAIN ======")
        if not self.blocks:
            print("(empty)")
        for b in self.blocks:
            status = "TENTATIVE" if b.tentative else "DECIDED"
            tx = b.transaction
            prev_str = b.prev_hash[:16] if b.prev_hash != "GENESIS" else "GENESIS"
            print(
                f"[{b.index}] {status} "
                f"Tx({tx.sender_id}->{tx.receiver_id}, {tx.amount}) "
                f"nonce={b.nonce} "
                f"hash={b.hash_value[:16]}... "
                f"prev={prev_str}"
            )
        print("==================")

    def __len__(self) -> int:
        return len(self.blocks)
