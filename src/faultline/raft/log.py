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

For now the log lives in memory. Day 5 saves it to disk.
"""

from collections.abc import Sequence

from faultline.raft.messages import Entry


class RaftLog:
    def __init__(self) -> None:
        self._entries: list[Entry] = []

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
        self._entries += entries

    def truncate_from(self, index: int) -> None:
        """Drop the entry at `index` and everything after it."""
        if not 1 <= index <= self.last_index + 1:
            raise IndexError(f"no entry {index}, the log ends at {self.last_index}")
        del self._entries[index - 1 :]
