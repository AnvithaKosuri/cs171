from typing import Optional, Tuple
from blockchain import Block


class PaxosState:
    def __init__(self):
        # Ballots are tuples: (seq_num, proc_id, depth)
        self.promised_n: Optional[Tuple[int, int, int]] = None
        self.accepted_n: Optional[Tuple[int, int, int]] = None
        self.accepted_block: Optional[Block] = None


def compare_ballot(a: Tuple[int, int, int], b: Tuple[int, int, int]) -> int:
    """
    Return -1 if a < b, 0 if a == b, 1 if a > b (lexicographic compare).
    """
    if a < b:
        return -1
    if a > b:
        return 1
    return 0
