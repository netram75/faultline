"""The manifest: which tables make up the database right now.

The folder can hold tables that are not part of the database: one that was
half written when the power went, or an old one that compaction replaced but
did not get to delete. So we never trust what is in the folder. A table is
live only if the manifest lists it.

The manifest is small, so we rewrite all of it on every change instead of
keeping a log of edits like LevelDB does. The new copy goes to MANIFEST.tmp,
is synced, and is then renamed over MANIFEST (see fs/atomic.py). A rename is
atomic, so after a crash we see the old manifest or the new one, never half
of each.

Format, numbers little endian:

    magic "FLTMAN01"
    next_file u64, wal_start u64, table count u32
    for each table: number u64, level u8, size u64,
                    smallest key length u32, smallest key,
                    largest key length u32, largest key
    CRC32 of everything above, u32
"""

import os
import struct
import zlib
from dataclasses import dataclass

from faultline.fs import FileSystem, replace_file

MANIFEST_NAME = "MANIFEST"
_MAGIC = b"FLTMAN01"
_HEADER = struct.Struct("<QQI")  # next_file, wal_start, table count
_TABLE = struct.Struct("<QBQ")  # number, level, size
_LENGTH = struct.Struct("<I")
_CRC = struct.Struct("<I")


class CorruptManifest(Exception):
    """The manifest file is damaged, or is not a manifest at all."""


@dataclass(frozen=True)
class TableInfo:
    """What the engine needs to know about a table without opening it."""

    number: int
    level: int
    smallest: bytes
    largest: bytes
    size: int


@dataclass(frozen=True)
class Manifest:
    next_file: int = 1  # number for the next table we write
    wal_start: int = 1  # first WAL segment whose writes are not in a table yet
    tables: tuple[TableInfo, ...] = ()

    def encode(self) -> bytes:
        parts = [_MAGIC, _HEADER.pack(self.next_file, self.wal_start, len(self.tables))]
        for t in self.tables:
            parts.append(_TABLE.pack(t.number, t.level, t.size))
            parts += [_LENGTH.pack(len(t.smallest)), t.smallest]
            parts += [_LENGTH.pack(len(t.largest)), t.largest]
        body = b"".join(parts)
        return body + _CRC.pack(zlib.crc32(body))

    @classmethod
    def decode(cls, data: bytes) -> "Manifest":
        if len(data) < len(_MAGIC) + _CRC.size or not data.startswith(_MAGIC):
            raise CorruptManifest("not a manifest")
        body, crc = data[: -_CRC.size], data[-_CRC.size :]
        if zlib.crc32(body) != _CRC.unpack(crc)[0]:
            raise CorruptManifest("checksum does not match")
        # The checksum passed, so these are bytes we wrote. A parse error now
        # would mean a bug, not damage, but we report it the same way.
        try:
            return _parse(body[len(_MAGIC) :])
        except (struct.error, ValueError) as err:
            raise CorruptManifest(str(err)) from err


def save(fs: FileSystem, directory: str, manifest: Manifest) -> None:
    """Replace the saved manifest. When this returns, the new one is durable."""
    replace_file(fs, os.path.join(directory, MANIFEST_NAME), manifest.encode())


def load(fs: FileSystem, directory: str) -> Manifest | None:
    """The saved manifest, or None if there is none yet (a brand new engine)."""
    path = os.path.join(directory, MANIFEST_NAME)
    if not fs.exists(path):
        return None
    return Manifest.decode(fs.read_file(path))


def _parse(data: bytes) -> Manifest:
    next_file, wal_start, count = _HEADER.unpack_from(data, 0)
    offset = _HEADER.size
    tables = []
    for _ in range(count):
        number, level, size = _TABLE.unpack_from(data, offset)
        offset += _TABLE.size
        smallest, offset = _read_key(data, offset)
        largest, offset = _read_key(data, offset)
        tables.append(TableInfo(number, level, smallest, largest, size))
    if offset != len(data):
        raise ValueError("extra bytes after the last table")
    return Manifest(next_file, wal_start, tuple(tables))


def _read_key(data: bytes, offset: int) -> tuple[bytes, int]:
    (length,) = _LENGTH.unpack_from(data, offset)
    start = offset + _LENGTH.size
    end = start + length
    if end > len(data):
        raise ValueError("a key runs past the end of the manifest")
    return data[start:end], end
