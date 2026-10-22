"""Data blocks: the small chunks an SSTable is made of.

An SSTable is split into blocks of about 4 KB. To look up a key we only read
and check the one block it can be in, not the whole file.

Inside a block, entries are stored one after another, sorted by key:

    +--------+------------+--------------+-------+---------+
    | kind   | key length | value length |  key  |  value  |
    | 1 byte | 4 bytes    | 4 bytes      |       |         |
    +--------+------------+--------------+-------+---------+

kind is 1 for a normal value and 0 for a tombstone (which has no value).
The block ends with a CRC32 of everything before it, so a damaged block is
caught when it is read instead of returning wrong data.

LevelDB also shares common key prefixes between neighbours to save space.
We skip that for now to keep the format easy to read.
"""

import struct
import zlib

from faultline.entry import TOMBSTONE, Tombstone, Value
from faultline.sstable.errors import CorruptTable

_ENTRY_HEADER = struct.Struct("<BII")  # kind, key length, value length
_CHECKSUM = struct.Struct("<I")
_PUT = 1
_DELETE = 0


class BlockBuilder:
    def __init__(self) -> None:
        self._buf = bytearray()
        self._last_key: bytes | None = None

    @property
    def size(self) -> int:
        """Bytes the block will take once finished, checksum included."""
        return len(self._buf) + _CHECKSUM.size

    @property
    def last_key(self) -> bytes | None:
        return self._last_key

    def add(self, key: bytes, value: Value) -> None:
        # Readers binary search on keys, so a single out of order key would
        # quietly break lookups. Better to fail loudly here.
        if self._last_key is not None and key <= self._last_key:
            raise ValueError(f"keys must go up: {key!r} came after {self._last_key!r}")
        if isinstance(value, Tombstone):
            self._buf += _ENTRY_HEADER.pack(_DELETE, len(key), 0)
            self._buf += key
        else:
            self._buf += _ENTRY_HEADER.pack(_PUT, len(key), len(value))
            self._buf += key + value
        self._last_key = key

    def finish(self) -> bytes:
        data = bytes(self._buf)
        return data + _CHECKSUM.pack(zlib.crc32(data))


def decode_block(raw: bytes) -> list[tuple[bytes, Value]]:
    """Check a finished block and return its entries in key order."""
    if len(raw) < _CHECKSUM.size:
        raise CorruptTable(f"block is only {len(raw)} bytes")
    data = raw[: -_CHECKSUM.size]
    (checksum,) = _CHECKSUM.unpack(raw[-_CHECKSUM.size :])
    if zlib.crc32(data) != checksum:
        raise CorruptTable("block checksum does not match")

    entries: list[tuple[bytes, Value]] = []
    offset = 0
    while offset < len(data):
        if offset + _ENTRY_HEADER.size > len(data):
            raise CorruptTable(f"entry header cut off at offset {offset}")
        kind, key_len, value_len = _ENTRY_HEADER.unpack_from(data, offset)
        offset += _ENTRY_HEADER.size
        end = offset + key_len + value_len
        if end > len(data):
            raise CorruptTable(f"entry runs past the end of the block at offset {offset}")

        key = data[offset : offset + key_len]
        if kind == _PUT:
            entries.append((key, data[offset + key_len : end]))
        elif kind == _DELETE and value_len == 0:
            entries.append((key, TOMBSTONE))
        else:
            raise CorruptTable(f"bad entry kind {kind} at offset {offset}")
        offset = end
    return entries
