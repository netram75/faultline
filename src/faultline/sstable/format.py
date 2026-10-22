"""How an SSTable file is laid out.

    +----------------+
    | data block 0   |  sorted entries, about 4 KB per block
    | data block 1   |
    | ...            |
    +----------------+
    | bloom filter   |  one filter covering every key in the file
    +----------------+
    | index block    |  last key of each data block -> where that block is
    +----------------+
    | footer         |  where the filter and index are; fixed size, always last
    +----------------+

A reader starts at the end. It reads the footer, then the index and the
filter, and keeps those in memory. After that, finding a key costs at most
one data block read, and usually zero if the bloom filter says no.

SSTables are written once, synced, and never changed again. That is the
"immutable" in "sorted string table": no locks and no partial updates.
"""

import struct
import zlib
from dataclasses import dataclass

from faultline.sstable.errors import CorruptTable

MAGIC = b"FLTSST01"  # "faultline sstable", format version 1

# Where a block is in the file: offset and size in bytes
BLOCK_HANDLE = struct.Struct("<QI")

_FOOTER_FIELDS = struct.Struct("<QIQIQ")
_CHECKSUM = struct.Struct("<I")
FOOTER_SIZE = _FOOTER_FIELDS.size + _CHECKSUM.size + len(MAGIC)


def add_checksum(data: bytes) -> bytes:
    return data + _CHECKSUM.pack(zlib.crc32(data))


def strip_checksum(raw: bytes, what: str) -> bytes:
    """Check the CRC32 at the end of `raw` and return the data before it."""
    if len(raw) < _CHECKSUM.size:
        raise CorruptTable(f"{what} is only {len(raw)} bytes")
    data = raw[: -_CHECKSUM.size]
    (checksum,) = _CHECKSUM.unpack(raw[-_CHECKSUM.size :])
    if zlib.crc32(data) != checksum:
        raise CorruptTable(f"{what} checksum does not match")
    return data


@dataclass(frozen=True)
class Footer:
    filter_offset: int
    filter_size: int
    index_offset: int
    index_size: int
    entry_count: int

    def encode(self) -> bytes:
        fields = _FOOTER_FIELDS.pack(
            self.filter_offset,
            self.filter_size,
            self.index_offset,
            self.index_size,
            self.entry_count,
        )
        return add_checksum(fields) + MAGIC

    @classmethod
    def decode(cls, raw: bytes) -> "Footer":
        if len(raw) != FOOTER_SIZE:
            raise CorruptTable(f"footer should be {FOOTER_SIZE} bytes, got {len(raw)}")
        if raw[-len(MAGIC) :] != MAGIC:
            raise CorruptTable("not an SSTable, or the footer is damaged (bad magic)")
        fields = strip_checksum(raw[: -len(MAGIC)], "footer")
        return cls(*_FOOTER_FIELDS.unpack(fields))
