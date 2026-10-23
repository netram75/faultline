"""Compaction: merging tables into fewer, cleaner ones.

Every flush adds a level 0 table, and level 0 tables can overlap. Left alone,
reads get slower with each flush (one more table to check), and the disk
fills up with old values and tombstones that nobody can see any more.

Compaction fixes both. It merges a set of tables into new ones where each key
shows up once, with its newest value. The new tables go to level 1, where
tables never overlap, so a read only has to check one of them.

Tombstones are dropped here, and that is only safe because level 1 is the
bottom level. The merge has already used each tombstone to hide the older
copies of its key, and there is nothing older outside the merge left to hide.
"""

from faultline.entry import Value
from faultline.fs import FileSystem
from faultline.lsm.files import table_path
from faultline.lsm.manifest import TableInfo
from faultline.lsm.merge import merge
from faultline.sstable.reader import TableReader
from faultline.sstable.writer import TableWriter

DEFAULT_TABLE_SIZE = 2 * 1024 * 1024


def compact(
    fs: FileSystem,
    root: str,
    inputs: list[TableReader],
    first_number: int,
    table_size: int = DEFAULT_TABLE_SIZE,
) -> list[TableInfo]:
    """Merge `inputs` (newest first) into new level 1 tables.

    A new output table starts once the current one reaches about
    `table_size` bytes. Outputs are numbered from `first_number` up, come
    back in key order, and never overlap. If every key was deleted there are
    no outputs at all.
    """
    outputs: list[TableInfo] = []
    current: _Output | None = None
    for key, value in merge(inputs, drop_tombstones=True):
        if current is None:
            current = _Output(fs, root, first_number + len(outputs))
        current.add(key, value)
        if current.size >= table_size:
            outputs.append(current.finish())
            current = None
    if current is not None:
        outputs.append(current.finish())
    return outputs


class _Output:
    """One table being written, plus the key range the manifest needs."""

    def __init__(self, fs: FileSystem, root: str, number: int) -> None:
        self._number = number
        self._writer = TableWriter(fs, table_path(root, number))
        self._smallest: bytes | None = None
        self._largest = b""

    @property
    def size(self) -> int:
        return self._writer.size

    def add(self, key: bytes, value: Value) -> None:
        if self._smallest is None:
            self._smallest = key
        self._largest = key
        self._writer.add(key, value)

    def finish(self) -> TableInfo:
        assert self._smallest is not None, "an output table is only started for a key"
        size = self._writer.finish()
        return TableInfo(self._number, 1, self._smallest, self._largest, size)
