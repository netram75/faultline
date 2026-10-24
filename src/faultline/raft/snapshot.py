"""A Raft node's snapshot: the state machine's data up to some log index.

Raft paper, section 7. The log can't grow forever. Once the state machine
has everything up to index i, it hands Raft a snapshot of its data at i, and
Raft keeps that instead of the entries up to i. Raft never looks inside the
data. It only needs to keep it safe and, later, send it to a follower that
is so far behind that the leader no longer has the entries it is missing.

The snapshot is saved before the log entries it replaces are dropped. A crash
in between leaves both, which is fine. The other way round, a crash could
leave neither.

The file is replaced as a whole (see fs/atomic.py). Format, numbers little
endian:

    magic "FLTSNP01", index u64, term u64, the data, CRC32 of all that u32
"""

import os
import struct
import zlib
from dataclasses import dataclass

from faultline.fs import FileSystem, replace_file

SNAPSHOT_NAME = "SNAPSHOT"
_MAGIC = b"FLTSNP01"
_HEADER = struct.Struct("<QQ")  # index, term
_CRC = struct.Struct("<I")


class CorruptSnapshot(Exception):
    """The saved snapshot is damaged."""


@dataclass(frozen=True)
class Snapshot:
    index: int  # the last log entry the data includes
    term: int  # that entry's term
    data: bytes  # the state machine's, Raft doesn't look inside

    def encode(self) -> bytes:
        body = _MAGIC + _HEADER.pack(self.index, self.term) + self.data
        return body + _CRC.pack(zlib.crc32(body))

    @classmethod
    def decode(cls, data: bytes) -> "Snapshot":
        if len(data) < len(_MAGIC) + _HEADER.size + _CRC.size or not data.startswith(_MAGIC):
            raise CorruptSnapshot("not a snapshot")
        body, (crc,) = data[: -_CRC.size], _CRC.unpack(data[-_CRC.size :])
        if zlib.crc32(body) != crc:
            raise CorruptSnapshot("checksum does not match")
        index, term = _HEADER.unpack_from(body, len(_MAGIC))
        return cls(index, term, body[len(_MAGIC) + _HEADER.size :])


def save(fs: FileSystem, directory: str, snapshot: Snapshot) -> None:
    """Replace the saved snapshot. When this returns, it is durable."""
    replace_file(fs, os.path.join(directory, SNAPSHOT_NAME), snapshot.encode())


def load(fs: FileSystem, directory: str) -> Snapshot | None:
    path = os.path.join(directory, SNAPSHOT_NAME)
    if not fs.exists(path):
        return None
    return Snapshot.decode(fs.read_file(path))
