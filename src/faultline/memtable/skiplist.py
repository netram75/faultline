"""A skip list: a sorted linked list with express lanes.

Finding a key in a plain sorted linked list means walking it from the start.
A skip list adds extra levels on top. Every node is on level 0, about a
quarter of them are also on level 1, a quarter of those on level 2, and so on.

    level 2:  head ---------------------------> 40 ------------------> None
    level 1:  head ----------> 17 ------------> 40 ------> 63 -------> None
    level 0:  head --> 5 ----> 17 --> 23 -----> 40 --> 51 -> 63 -> 70 > None

A search starts at the top level and moves right while the next key is still
smaller than the one we want, then drops down a level. On average that takes
O(log n) steps, the same as a balanced tree, with much less code. LevelDB and
RocksDB use a skip list for their memtable for the same reason.

How tall each node is gets decided by a coin flip, so the list takes a seed.
Same seed, same shape, which keeps tests repeatable.
"""

import random
from collections.abc import Iterator

# Same numbers LevelDB uses. With a 1 in 4 chance per extra level, 12 levels
# stay fast up to about 4^12 = 16 million keys, far more than a memtable holds.
MAX_LEVEL = 12
_BRANCHING = 4


class _Node[V]:
    __slots__ = ("key", "next", "value")

    def __init__(self, key: bytes, value: V, height: int) -> None:
        self.key = key
        self.value = value
        # next[i] is the following node on level i
        self.next: list[_Node[V] | None] = [None] * height


class SkipList[V]:
    def __init__(self, seed: int = 0) -> None:
        self._rng = random.Random(seed)
        # The head is not a real node, just the first set of pointers.
        self._head: list[_Node[V] | None] = [None] * MAX_LEVEL
        self._height = 1  # levels in use right now
        self._len = 0

    def __len__(self) -> int:
        return self._len

    def put(self, key: bytes, value: V) -> None:
        """Insert a key, or replace its value if it is already there."""
        before = self._pointers_before(key)
        found = before[0][0]
        if found is not None and found.key == key:
            found.value = value
            return

        height = self._random_height()
        self._height = max(self._height, height)
        node = _Node(key, value, height)
        for level in range(height):
            node.next[level] = before[level][level]
            before[level][level] = node
        self._len += 1

    def get(self, key: bytes) -> V | None:
        """The value for `key`, or None if it is not in the list."""
        found = self._pointers_before(key)[0][0]
        if found is not None and found.key == key:
            return found.value
        return None

    def __iter__(self) -> Iterator[tuple[bytes, V]]:
        """Every (key, value), smallest key first."""
        return self.seek(b"")

    def seek(self, key: bytes) -> Iterator[tuple[bytes, V]]:
        """Every (key, value) from the first key >= `key` onward."""
        node = self._pointers_before(key)[0][0]
        while node is not None:
            yield node.key, node.value
            node = node.next[0]

    def _pointers_before(self, key: bytes) -> list[list[_Node[V] | None]]:
        """For each level, the pointer list of the last node with a smaller key.

        Those are exactly the pointers an insert has to change, and level 0's
        next node is where `key` is, or would be.
        """
        before = [self._head] * MAX_LEVEL
        pointers = self._head
        for level in range(self._height - 1, -1, -1):
            node = pointers[level]
            while node is not None and node.key < key:
                pointers = node.next
                node = pointers[level]
            before[level] = pointers
        return before

    def _random_height(self) -> int:
        height = 1
        while height < MAX_LEVEL and self._rng.randrange(_BRANCHING) == 0:
            height += 1
        return height
