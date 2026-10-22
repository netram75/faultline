"""The memtable: where new writes live until they are flushed to disk.

Every write goes to the WAL first, so it survives a crash, and then here, so
it can be read back fast. Once the memtable is big enough it gets written out
as one sorted file (an SSTable) and a fresh, empty memtable takes its place.
"""

from collections.abc import Iterator

from faultline.entry import TOMBSTONE, Tombstone, Value
from faultline.memtable.skiplist import SkipList

# Rough cost of one entry besides its key and value (node, pointers, list
# slots). It only has to be in the right ballpark, since size is used to
# decide when to flush, not to count memory exactly.
_ENTRY_OVERHEAD = 64


def _value_size(value: Value) -> int:
    return 0 if isinstance(value, Tombstone) else len(value)


class Memtable:
    def __init__(self, seed: int = 0) -> None:
        self._entries: SkipList[Value] = SkipList(seed)
        self._size = 0

    @property
    def size(self) -> int:
        """Approximate memory used, in bytes."""
        return self._size

    def __len__(self) -> int:
        return len(self._entries)

    def put(self, key: bytes, value: bytes) -> None:
        self._set(key, value)

    def delete(self, key: bytes) -> None:
        # Always store the tombstone, even for a key we have never seen here.
        # An older value for it may be on disk, and that one must stay hidden.
        self._set(key, TOMBSTONE)

    def get(self, key: bytes) -> Value | None:
        """The value, TOMBSTONE if the key was deleted, or None if this
        memtable knows nothing about the key and older data should be checked."""
        return self._entries.get(key)

    def __iter__(self) -> Iterator[tuple[bytes, Value]]:
        """Every entry in key order, tombstones included."""
        return iter(self._entries)

    def seek(self, key: bytes) -> Iterator[tuple[bytes, Value]]:
        return self._entries.seek(key)

    def _set(self, key: bytes, value: Value) -> None:
        old = self._entries.get(key)
        if old is None:
            self._size += len(key) + _ENTRY_OVERHEAD
        else:
            self._size -= _value_size(old)
        self._size += _value_size(value)
        self._entries.put(key, value)
