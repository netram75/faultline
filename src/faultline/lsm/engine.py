"""The storage engine: a key-value store on one machine's disk.

This is where the pieces from the first two days meet:

    write: WAL first, so it survives a crash, then the memtable
    read:  the memtable, then the tables on disk

Later, every node in the Raft cluster keeps its copy of the data in one
Engine.

Files in the engine's folder:

    MANIFEST          which tables are live (see manifest.py)
    wal/00000004.wal  the write-ahead log
"""

import os
from dataclasses import dataclass
from typing import Self

from faultline.entry import Tombstone
from faultline.fs import FileSystem
from faultline.lsm import manifest
from faultline.lsm.batch import WriteBatch
from faultline.lsm.manifest import Manifest
from faultline.memtable import Memtable
from faultline.wal import WalWriter, open_wal
from faultline.wal.writer import DEFAULT_SEGMENT_SIZE


@dataclass(frozen=True)
class Options:
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
        self._manifest = state
        self._wal = wal
        self._memtable = memtable

    @classmethod
    def open(cls, fs: FileSystem, root: str, options: Options | None = None) -> Self:
        """Open the engine in `root`, creating it if needed, and recover
        everything that was synced before the last crash or close."""
        options = options or Options()
        if not fs.exists(root):
            fs.mkdir(root)
            fs.sync_dir(os.path.dirname(os.path.normpath(root)) or ".")
        state = manifest.load(fs, root) or Manifest()

        records, wal = open_wal(fs, os.path.join(root, "wal"), options.wal_segment_size)
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

    def get(self, key: bytes) -> bytes | None:
        """The value for `key`, or None if it is not there."""
        value = self._memtable.get(key)
        if value is None or isinstance(value, Tombstone):
            return None
        return value

    def close(self) -> None:
        """Close the files. Nothing to flush: everything is in the WAL already."""
        self._wal.close()
