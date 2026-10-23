"""The Raft log: the list of commands every node agrees on, in order.

Entries are numbered from 1. Index 0 is a pretend entry before the first one,
with term 0. Having it means "the entry just before index i" always exists,
even for i = 1, which keeps the checks in node.py free of special cases.

Two facts about logs carry Raft's safety (Raft paper, section 5.3):

* If two logs have an entry with the same index and term, the entries are the
  same command.
* And then every entry before it is the same too.

So to check that a follower's log matches the leader's up to some point, it
is enough to compare one (index, term) pair. That is what matches() does.

On disk the log is a WAL (see wal/) of two kinds of records:

    APPEND    kind u8 = 1, index u64, term u64, the command
    TRUNCATE  kind u8 = 2, from_index u64

Opening the log replays them in order. Cutting entries off the end, which a
follower does when its log disagrees with the leader's, is just one more
record, so the file is only ever appended to. It never shrinks yet: that
needs snapshots, which come later.
"""

import struct
from collections.abc import Sequence
from typing import Self

from faultline.fs import FileSystem
from faultline.raft.messages import Entry
from faultline.wal import WalWriter, open_wal

_APPEND = 1
_TRUNCATE = 2
_APPEND_HEADER = struct.Struct("<BQQ")  # kind, index, term
_TRUNCATE_RECORD = struct.Struct("<BQ")  # kind, from_index


class CorruptLog(Exception):
    """A log record that can't be right: unknown, or out of order."""


class RaftLog:
    """The log. RaftLog() keeps it in memory only, which tests use.
    RaftLog.open() keeps it on disk, and append() and truncate_from() return
    only once the change is synced."""

    def __init__(self, wal: WalWriter | None = None) -> None:
        self._entries: list[Entry] = []
        self._wal = wal

    @classmethod
    def open(cls, fs: FileSystem, directory: str) -> Self:
        records, wal = open_wal(fs, directory)
        log = cls(wal)
        for record in records:
            log._replay(record)
        return log

    @property
    def last_index(self) -> int:
        return len(self._entries)

    @property
    def last_term(self) -> int:
        return self.term_at(self.last_index)

    def term_at(self, index: int) -> int:
        """Term of the entry at `index`. Index 0 has term 0."""
        if index == 0:
            return 0
        return self.entry(index).term

    def entry(self, index: int) -> Entry:
        if not 1 <= index <= self.last_index:
            raise IndexError(f"no entry {index}, the log ends at {self.last_index}")
        return self._entries[index - 1]

    def entries_from(self, index: int, max_count: int | None = None) -> tuple[Entry, ...]:
        """Entries from `index` to the end, or at most `max_count` of them."""
        if not 1 <= index <= self.last_index + 1:
            raise IndexError(f"no entry {index}, the log ends at {self.last_index}")
        end = self.last_index if max_count is None else min(self.last_index, index + max_count - 1)
        return tuple(self._entries[index - 1 : end])

    def matches(self, index: int, term: int) -> bool:
        """True if this log has an entry at `index` with `term`."""
        return index <= self.last_index and self.term_at(index) == term

    def is_up_to_date(self, last_index: int, last_term: int) -> bool:
        """True if a log that ends at (last_index, last_term) is at least as up
        to date as this one. Raft paper, section 5.4.1: the later last term
        wins, and if the last terms are equal, the longer log wins."""
        if last_term != self.last_term:
            return last_term > self.last_term
        return last_index >= self.last_index

    def append(self, entries: Sequence[Entry]) -> None:
        """Add entries to the end. On disk, one sync covers all of them."""
        if self._wal is not None and entries:
            for i, entry in enumerate(entries, start=self.last_index + 1):
                self._wal.append(_APPEND_HEADER.pack(_APPEND, i, entry.term) + entry.command)
            self._wal.sync()
        self._entries += entries

    def truncate_from(self, index: int) -> None:
        """Drop the entry at `index` and everything after it."""
        if not 1 <= index <= self.last_index + 1:
            raise IndexError(f"no entry {index}, the log ends at {self.last_index}")
        if self._wal is not None and index <= self.last_index:
            self._wal.append(_TRUNCATE_RECORD.pack(_TRUNCATE, index))
            self._wal.sync()
        del self._entries[index - 1 :]

    def close(self) -> None:
        if self._wal is not None:
            self._wal.close()

    def _replay(self, record: bytes) -> None:
        kind = record[0] if record else None
        if kind == _APPEND and len(record) >= _APPEND_HEADER.size:
            _, index, term = _APPEND_HEADER.unpack_from(record)
            if index != self.last_index + 1:
                raise CorruptLog(f"entry {index} comes after entry {self.last_index}")
            self._entries.append(Entry(term, record[_APPEND_HEADER.size :]))
        elif kind == _TRUNCATE and len(record) == _TRUNCATE_RECORD.size:
            _, index = _TRUNCATE_RECORD.unpack(record)
            if not 1 <= index <= self.last_index:
                raise CorruptLog(f"cut at entry {index}, but the log ends at {self.last_index}")
            del self._entries[index - 1 :]
        else:
            raise CorruptLog(f"bad log record of {len(record)} bytes")
