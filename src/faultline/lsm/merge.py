"""Merging sorted sources into one sorted stream, the core move of an LSM tree.

A key can be in several places at once: the memtable and any number of
SSTables. Each of those is sorted on its own. To read everything in key order
we merge them, the same way merge sort merges its halves, using a heap so it
costs O(log n) per entry for n sources.

When a key is in more than one source, the newest copy wins. Callers pass the
sources newest first, so "newest" simply means "lower position in the list".

Tombstones are kept by default. A tombstone must stay as long as an older
copy of its key might exist somewhere outside this merge, or that old value
would come back to life. Dropping them is only safe when the merge covers
the oldest data there is, or when the result goes to a user, who should
never see them.
"""

import heapq
from collections.abc import Iterable, Iterator, Sequence

from faultline.entry import Tombstone, Value

type _Source = Iterator[tuple[bytes, Value]]
# (key, source position, value, the source itself). The position breaks ties
# between equal keys, so the value and iterator are never compared.
type _HeapItem = tuple[bytes, int, Value, _Source]


def merge(
    sources: Sequence[Iterable[tuple[bytes, Value]]],
    drop_tombstones: bool = False,
) -> Iterator[tuple[bytes, Value]]:
    """Merge sorted sources, newest first, into one sorted stream with one
    entry per key."""
    heap: list[_HeapItem] = []
    for position, source in enumerate(sources):
        _push_next(heap, position, iter(source))

    last_key: bytes | None = None
    while heap:
        key, position, value, source = heapq.heappop(heap)
        _push_next(heap, position, source)

        if key == last_key:
            continue  # an older copy of a key we already handled
        last_key = key
        if drop_tombstones and isinstance(value, Tombstone):
            continue
        yield key, value


def _push_next(heap: list[_HeapItem], position: int, source: _Source) -> None:
    entry = next(source, None)
    if entry is not None:
        heapq.heappush(heap, (entry[0], position, entry[1], source))
