"""Reading an SSTable."""

from bisect import bisect_left

from faultline.entry import Value
from faultline.fs import FileSystem
from faultline.sstable.block import decode_block
from faultline.sstable.bloom import BloomFilter
from faultline.sstable.errors import CorruptTable
from faultline.sstable.format import BLOCK_HANDLE, FOOTER_SIZE, Footer, strip_checksum


class TableReader:
    """Looks up keys in one SSTable.

    Opening reads the footer, the index and the bloom filter into memory.
    After that, get() reads at most one data block from disk.
    """

    def __init__(self, fs: FileSystem, path: str) -> None:
        self.path = path
        self._file = fs.open_read(path)
        size = self._file.size
        if size < FOOTER_SIZE:
            raise CorruptTable(f"{path}: {size} bytes is too small to be an SSTable")
        footer = Footer.decode(self._read(size - FOOTER_SIZE, FOOTER_SIZE))

        # A damaged footer could point anywhere. Check before trusting it.
        if not (
            footer.filter_offset + footer.filter_size <= footer.index_offset
            and footer.index_offset + footer.index_size <= size - FOOTER_SIZE
        ):
            raise CorruptTable(f"{path}: footer points outside the file")

        filter_raw = self._read(footer.filter_offset, footer.filter_size)
        self._bloom = BloomFilter(strip_checksum(filter_raw, "bloom filter"))

        # Keep the index as two plain lists so lookups are one bisect call
        self._last_keys: list[bytes] = []
        self._blocks: list[tuple[int, int]] = []
        for last_key, handle in decode_block(self._read(footer.index_offset, footer.index_size)):
            if not isinstance(handle, bytes) or len(handle) != BLOCK_HANDLE.size:
                raise CorruptTable(f"{path}: bad index entry for {last_key!r}")
            self._last_keys.append(last_key)
            self._blocks.append(BLOCK_HANDLE.unpack(handle))

        self.entry_count = footer.entry_count

    def get(self, key: bytes) -> Value | None:
        """The value, TOMBSTONE if the key was deleted, or None if the key is
        not in this table."""
        if not self._bloom.may_contain(key):
            return None

        # The first block whose last key is >= key is the only one it can be in
        i = bisect_left(self._last_keys, key)
        if i == len(self._last_keys):
            return None

        entries = self._read_block(i)
        j = bisect_left(entries, key, key=lambda entry: entry[0])
        if j < len(entries) and entries[j][0] == key:
            return entries[j][1]
        return None

    def close(self) -> None:
        self._file.close()

    def _read_block(self, i: int) -> list[tuple[bytes, Value]]:
        offset, size = self._blocks[i]
        return decode_block(self._read(offset, size))

    def _read(self, offset: int, size: int) -> bytes:
        data = self._file.read_at(offset, size)
        if len(data) != size:
            raise CorruptTable(f"{self.path}: wanted {size} bytes at {offset}, file ended")
        return data
