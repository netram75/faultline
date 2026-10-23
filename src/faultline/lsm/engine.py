"""The storage engine: a key-value store on one machine's disk.

This is where the pieces from the first two days meet:

    write: WAL first, so it survives a crash, then the memtable
    read:  the memtable, then the tables on disk, newest first

When the memtable gets big it is flushed: written out as a new table, after
which the WAL segments that held it are not needed any more.

Later, every node in the Raft cluster keeps its copy of the data in one
Engine.

Files in the engine's folder:

    MANIFEST          which tables are live (see manifest.py)
    wal/00000004.wal  the write-ahead log
    00000007.sst      tables
"""

import os
from dataclasses import dataclass, replace
from typing import Self

from faultline.entry import Tombstone, Value
from faultline.fs import FileSystem
from faultline.lsm import manifest
from faultline.lsm.batch import WriteBatch
from faultline.lsm.files import list_tables, table_path
from faultline.lsm.manifest import Manifest, TableInfo
from faultline.memtable import Memtable
from faultline.sstable.reader import TableReader
from faultline.sstable.writer import TableWriter
from faultline.wal import WalWriter, open_wal
from faultline.wal.segments import list_segments, segment_path
from faultline.wal.writer import DEFAULT_SEGMENT_SIZE


@dataclass(frozen=True)
class Options:
    memtable_size: int = 4 * 1024 * 1024  # flush once the memtable is about this big
    wal_segment_size: int = DEFAULT_SEGMENT_SIZE


class Engine:
    """A key-value store. Keys and values are bytes.

        engine = Engine.open(fs, "/data/node1")
        engine.put(b"name", b"faultline")
        engine.get(b"name")  # b"faultline"
        engine.close()

    Every write is synced before it returns. If a call raises because the
    disk failed, throw the engine away and open a new one, which recovers
    from what is on disk.
    """

    def __init__(
        self,
        fs: FileSystem,
        root: str,
        options: Options,
        state: Manifest,
        wal: WalWriter,
        memtable: Memtable,
    ) -> None:
        # Use Engine.open(), which does the recovery and then calls this
        self._fs = fs
        self._root = root
        self._options = options
        self._wal = wal
        self._memtable = memtable
        self._tables: dict[int, TableReader] = {}
        self._level0: list[TableInfo] = []
        self._manifest = Manifest()
        self._use_manifest(state)

    @classmethod
    def open(cls, fs: FileSystem, root: str, options: Options | None = None) -> Self:
        """Open the engine in `root`, creating it if needed, and recover
        everything that was synced before the last crash or close."""
        options = options or Options()
        if not fs.exists(root):
            fs.mkdir(root)
            fs.sync_dir(os.path.dirname(os.path.normpath(root)) or ".")
        state = manifest.load(fs, root) or Manifest()
        _remove_dead_files(fs, root, state)

        records, wal = open_wal(
            fs, _wal_dir(root), options.wal_segment_size, first_segment=state.wal_start
        )
        memtable = Memtable()
        for payload in records:
            WriteBatch.decode(payload).apply(memtable)
        return cls(fs, root, options, state, wal, memtable)

    def put(self, key: bytes, value: bytes) -> None:
        self.write(WriteBatch().put(key, value))

    def delete(self, key: bytes) -> None:
        self.write(WriteBatch().delete(key))

    def write(self, batch: WriteBatch) -> None:
        """Apply every change in the batch. After a crash it is there
        completely or not at all, and once this returns it is there for good."""
        if len(batch) == 0:
            return
        self._wal.append(batch.encode())
        # Sync before the memtable sees it. The other way round, a reader
        # could see a write that a crash then takes back.
        self._wal.sync()
        batch.apply(self._memtable)
        if self._memtable.size >= self._options.memtable_size:
            self._flush()

    def get(self, key: bytes) -> bytes | None:
        """The value for `key`, or None if it is not there."""
        value = self._find(key)
        if value is None or isinstance(value, Tombstone):
            return None
        return value

    def close(self) -> None:
        """Close the files. Nothing to flush: everything is in the WAL already."""
        self._wal.close()
        for table in self._tables.values():
            table.close()

    def _find(self, key: bytes) -> Value | None:
        # The first place that knows about the key has its newest value,
        # even if that value is a tombstone
        value = self._memtable.get(key)
        if value is not None:
            return value
        for info in self._level0:
            if info.smallest <= key <= info.largest:
                value = self._tables[info.number].get(key)
                if value is not None:
                    return value
        return None

    def _flush(self) -> None:
        """Write the memtable out as a level 0 table and start an empty one.

        The order of the steps is what makes this crash safe: nothing is
        deleted until the manifest that makes it unneeded is on disk.
        """
        # New writes go to a new segment from here on. So the segments before
        # it hold exactly what is in this memtable.
        self._wal.start_next_segment()

        number = self._manifest.next_file
        info = self._write_table(number)
        # If we crash before this save, the new table is an orphan that
        # open() deletes, and the old segments are still there to replay
        self._save_manifest(
            replace(
                self._manifest,
                next_file=number + 1,
                wal_start=self._wal.segment,
                tables=(*self._manifest.tables, info),
            )
        )
        _remove_old_segments(self._fs, self._root, self._wal.segment)
        self._memtable = Memtable()

    def _write_table(self, number: int) -> TableInfo:
        path = table_path(self._root, number)
        writer = TableWriter(self._fs, path)
        keys = []
        for key, value in self._memtable:
            # Tombstones go to disk too. An older table may still have the key.
            writer.add(key, value)
            keys.append(key)
        size = writer.finish()
        return TableInfo(number, 0, keys[0], keys[-1], size)

    def _save_manifest(self, state: Manifest) -> None:
        manifest.save(self._fs, self._root, state)
        self._use_manifest(state)

    def _use_manifest(self, state: Manifest) -> None:
        """Open any tables we don't have open yet and rebuild the read order."""
        self._manifest = state
        for info in state.tables:
            if info.number not in self._tables:
                self._tables[info.number] = TableReader(
                    self._fs, table_path(self._root, info.number)
                )
        # Newer tables have bigger numbers, and a newer table wins
        level0 = [info for info in state.tables if info.level == 0]
        self._level0 = sorted(level0, key=lambda info: info.number, reverse=True)


def _wal_dir(root: str) -> str:
    return os.path.join(root, "wal")


def _remove_dead_files(fs: FileSystem, root: str, state: Manifest) -> None:
    """Delete what a crash left behind: tables the manifest doesn't list (a
    flush that never finished) and segments whose writes are in a table."""
    live = {info.number for info in state.tables}
    for number in list_tables(fs, root):
        if number not in live:
            fs.remove(table_path(root, number))
    if fs.exists(_wal_dir(root)):
        _remove_old_segments(fs, root, state.wal_start)


def _remove_old_segments(fs: FileSystem, root: str, wal_start: int) -> None:
    # No sync_dir needed. If a crash brings a segment back, the next open
    # sees it is below wal_start and deletes it again.
    for number in list_segments(fs, _wal_dir(root)):
        if number < wal_start:
            fs.remove(segment_path(_wal_dir(root), number))
