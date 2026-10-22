"""What a key can point to inside the storage engine."""

from typing import final


@final
class Tombstone:
    """Marks a deleted key.

    We can't just forget a deleted key, because older copies of it may still
    be sitting in files on disk. The tombstone is newer than those copies, so
    it hides them until compaction can throw both away.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "TOMBSTONE"


TOMBSTONE = Tombstone()

type Value = bytes | Tombstone
