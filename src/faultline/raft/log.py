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

On disk the log is a WAL (see wal/) of three kinds of records:

    APPEND    kind u8 = 1, index u64, term u64, the command
    TRUNCATE  kind u8 = 2, from_index u64
    BASE      kind u8 = 3, index u64, term u64

Opening the log replays them in order. Cutting entries off the end, which a
follower does when its log disagrees with the leader's, is just one more
record, so the file is only ever appended to.

**Compaction** (Raft paper, section 7). Once the state machine has a
snapshot up to some index, the entries up to there are not needed any more,
and the log would otherwise grow forever. compact() writes a new WAL segment
that starts with a BASE record ("the log now starts after this index, which
had this term") followed by the entries after it, and then deletes the older
segments.

The new segment is written whole under a temporary name and renamed into
place (fs/atomic.py), so replay sees all of it or none of it. Written in
place, a crash could keep the BASE record but tear off entries after it,
and those entries may be committed. If old segments survive a crash, replay
goes through their entries and then reaches the BASE record, which throws
them away again.

After compaction, index base_index plays the part index 0 played before: a
pretend entry, now with term base_term, that "the entry just before" can
always point at.
"""

import struct
from collections.abc import Sequence
from typing import Self

from faultline.fs import FileSystem, replace_file
from faultline.raft.messages import Entry
from faultline.wal import WalWriter, open_wal, record
from faultline.wal.segments import list_segments, segment_path

_APPEND = 1
_TRUNCATE = 2
_BASE = 3
_APPEND_HEADER = struct.Struct("<BQQ")  # kind, index, term
_TRUNCATE_RECORD = struct.Struct("<BQ")  # kind, from_index
_BASE_RECORD = struct.Struct("<BQQ")  # kind, index, term


class CorruptLog(Exception):
    """A log record that can't be right: unknown, or out of order."""


class RaftLog:
    """The log. RaftLog() keeps it in memory only, which tests use.
    RaftLog.open() keeps it on disk, and append() and truncate_from() return
    only once the change is synced."""

    def __init__(
        self, wal: WalWriter | None = None, fs: FileSystem | None = None, directory: str = ""
    ) -> None:
        self._entries: list[Entry] = []
        # The last entry compacted away, see the top of this file
        self.base_index = 0
        self.base_term = 0
        self._wal = wal
        self._fs = fs
        self._directory = directory

    @classmethod
    def open(cls, fs: FileSystem, directory: str) -> Self:
        records, wal = open_wal(fs, directory)
        log = cls(wal, fs, directory)
        for payload in records:
            log._replay(payload)
        return log

    @property
    def last_index(self) -> int:
        return self.base_index + len(self._entries)

    @property
    def last_term(self) -> int:
        return self.term_at(self.last_index)

    def term_at(self, index: int) -> int:
        """Term of the entry at `index`. The base (index 0 before any
        compaction) has base_term."""
        if index == self.base_index:
            return self.base_term
        return self.entry(index).term

    def entry(self, index: int) -> Entry:
        if index <= self.base_index:
            raise IndexError(f"entry {index} was compacted, the log starts after {self.base_index}")
        if index > self.last_index:
            raise IndexError(f"no entry {index}, the log ends at {self.last_index}")
        return self._entries[index - self.base_index - 1]

    def entries_from(self, index: int, max_count: int | None = None) -> tuple[Entry, ...]:
        """Entries from `index` to the end, or at most `max_count` of them."""
        if not self.base_index < index <= self.last_index + 1:
            raise IndexError(
                f"no entry {index}, the log holds {self.base_index + 1} to {self.last_index}"
            )
        end = self.last_index if max_count is None else min(self.last_index, index + max_count - 1)
        return tuple(self._entries[index - self.base_index - 1 : end - self.base_index])

    def matches(self, index: int, term: int) -> bool:
        """True if this log has an entry at `index` with `term`. Compacted
        entries can't be checked, so they don't match here; node.py deals
        with them before asking."""
        return self.base_index <= index <= self.last_index and self.term_at(index) == term

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
        """Drop the entry at `index` and everything after it. Compacted
        entries can't be dropped: they are committed."""
        if not self.base_index < index <= self.last_index + 1:
            raise IndexError(
                f"no entry {index}, the log holds {self.base_index + 1} to {self.last_index}"
            )
        if self._wal is not None and index <= self.last_index:
            self._wal.append(_TRUNCATE_RECORD.pack(_TRUNCATE, index))
            self._wal.sync()
        del self._entries[index - self.base_index - 1 :]

    def compact(self, index: int, term: int) -> None:
        """Make (index, term) the new base: drop every entry up to it. Entries
        after it stay if this log has (index, term) itself. If it doesn't, the
        log disagrees with whoever sent the snapshot, and all of it goes
        (Raft paper, figure 13, steps 6 and 7)."""
        if index <= self.base_index:
            return
        kept = list(self.entries_from(index + 1)) if self.matches(index, term) else []
        if self._wal is not None:
            self._rewrite(index, term, kept)
        self.base_index, self.base_term = index, term
        self._entries = kept

    def _rewrite(self, index: int, term: int, kept: list[Entry]) -> None:
        """Start the log on disk over from (index, term). See the top of this file."""
        assert self._wal is not None and self._fs is not None
        fs, directory = self._fs, self._directory
        records = [_BASE_RECORD.pack(_BASE, index, term)]
        for i, entry in enumerate(kept, start=index + 1):
            records.append(_APPEND_HEADER.pack(_APPEND, i, entry.term) + entry.command)
        first = self._wal.segment + 1
        self._wal.close()
        # Left over from a rewrite a crash stopped. Never renamed, so never used.
        for name in fs.list_dir(directory):
            if name.endswith(".tmp"):
                fs.remove(f"{directory}/{name}")
        replace_file(fs, segment_path(directory, first), b"".join(map(record.encode, records)))
        self._wal = WalWriter(fs, directory, first)
        # Newest first. If a crash stops us halfway, what is left is a run of
        # old segments from the start, which replays cleanly up to the BASE.
        for number in reversed(list_segments(fs, directory)):
            if number < first:
                fs.remove(segment_path(directory, number))
        fs.sync_dir(directory)

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
            if not self.base_index < index <= self.last_index:
                raise CorruptLog(f"cut at entry {index}, but the log ends at {self.last_index}")
            del self._entries[index - self.base_index - 1 :]
        elif kind == _BASE and len(record) == _BASE_RECORD.size:
            _, index, term = _BASE_RECORD.unpack(record)
            if index < self.base_index:
                raise CorruptLog(f"base {index} is older than base {self.base_index}")
            # Whatever came before is in a snapshot now. The entries after the
            # base follow as APPEND records.
            self.base_index, self.base_term = index, term
            self._entries = []
        else:
            raise CorruptLog(f"bad log record of {len(record)} bytes")
