"""Writing an SSTable."""

import os

from faultline.entry import Value
from faultline.fs import FileSystem
from faultline.sstable.block import BlockBuilder
from faultline.sstable.bloom import DEFAULT_BITS_PER_KEY, build_filter
from faultline.sstable.format import BLOCK_HANDLE, Footer, add_checksum

DEFAULT_BLOCK_SIZE = 4096


class TableWriter:
    """Writes one SSTable from entries handed over in key order.

        writer = TableWriter(fs, "/data/00000007.sst")
        for key, value in memtable:
            writer.add(key, value)
        writer.finish()

    Nothing is safe until finish() returns. If the process dies before that,
    the half-written file is junk and whoever opens the folder next should
    delete it.
    """

    def __init__(
        self,
        fs: FileSystem,
        path: str,
        block_size: int = DEFAULT_BLOCK_SIZE,
        bits_per_key: int = DEFAULT_BITS_PER_KEY,
    ) -> None:
        if fs.exists(path):
            raise FileExistsError(f"{path}: an SSTable is written once and never changed")
        self._fs = fs
        self._path = path
        self._file = fs.open_append(path)
        self._block_size = block_size
        self._bits_per_key = bits_per_key
        self._block = BlockBuilder()
        self._index = BlockBuilder()
        # Every key, for the bloom filter at the end. Fine for tables made
        # from one memtable. LevelDB keeps a filter per 2 KB of data instead,
        # which we can switch to if tables ever get big.
        self._keys: list[bytes] = []
        self._offset = 0

    def add(self, key: bytes, value: Value) -> None:
        if self._keys and key <= self._keys[-1]:
            raise ValueError(f"keys must go up: {key!r} came after {self._keys[-1]!r}")
        self._block.add(key, value)
        self._keys.append(key)
        if self._block.size >= self._block_size:
            self._flush_block()

    def finish(self) -> int:
        """Write the filter, index and footer, then make the file durable.

        Returns the size of the file in bytes.
        """
        self._flush_block()

        bloom = add_checksum(build_filter(self._keys, self._bits_per_key))
        filter_offset = self._write(bloom)
        index = self._index.finish()
        index_offset = self._write(index)
        footer = Footer(filter_offset, len(bloom), index_offset, len(index), len(self._keys))
        self._write(footer.encode())

        self._file.sync()
        self._file.close()
        # Without this the file's name can vanish in a crash, even though
        # its bytes were synced.
        self._fs.sync_dir(os.path.dirname(self._path))
        return self._offset

    def _flush_block(self) -> None:
        last_key = self._block.last_key
        if last_key is None:
            return  # nothing in it
        raw = self._block.finish()
        offset = self._write(raw)
        # Index entry: the block's last key. A reader looking for key K wants
        # the first block whose last key is >= K, which is a binary search.
        self._index.add(last_key, BLOCK_HANDLE.pack(offset, len(raw)))
        self._block = BlockBuilder()

    def _write(self, data: bytes) -> int:
        """Append data and return the offset it was written at."""
        offset = self._offset
        self._file.write(data)
        self._offset += len(data)
        return offset
