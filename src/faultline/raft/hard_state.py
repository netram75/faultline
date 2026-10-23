"""The two numbers a Raft node must never forget: its term and its vote.

The Raft paper (figure 2) calls these persistent state, along with the log.
If a node forgot its vote after a restart, it could vote twice in one term,
two candidates could both win, and the cluster would have two leaders. If it
forgot its term, it could go back in time and listen to a leader that was
already replaced.

So both are saved, and synced, before the node acts on them. A vote is on
disk before the reply that grants it is sent.

The file is replaced as a whole each time (see fs/atomic.py). Format,
numbers little endian:

    magic "FLTRFT01", term u64, has_vote u8, voted_for u32, CRC32 of all that u32
"""

import os
import struct
import zlib
from dataclasses import dataclass

from faultline.fs import FileSystem, replace_file
from faultline.raft.messages import NodeId

STATE_NAME = "RAFT_STATE"
_MAGIC = b"FLTRFT01"
_FIELDS = struct.Struct("<QBI")  # term, has_vote, voted_for
_CRC = struct.Struct("<I")
_SIZE = len(_MAGIC) + _FIELDS.size + _CRC.size


class CorruptState(Exception):
    """The saved term and vote are damaged."""


@dataclass(frozen=True)
class HardState:
    term: int = 0
    voted_for: NodeId | None = None  # who we voted for in this term, if anyone

    def encode(self) -> bytes:
        has_vote = self.voted_for is not None
        body = _MAGIC + _FIELDS.pack(self.term, has_vote, self.voted_for or 0)
        return body + _CRC.pack(zlib.crc32(body))

    @classmethod
    def decode(cls, data: bytes) -> "HardState":
        if len(data) != _SIZE or not data.startswith(_MAGIC):
            raise CorruptState("not a saved Raft state")
        body, (crc,) = data[: -_CRC.size], _CRC.unpack(data[-_CRC.size :])
        if zlib.crc32(body) != crc:
            raise CorruptState("checksum does not match")
        term, has_vote, voted_for = _FIELDS.unpack(body[len(_MAGIC) :])
        if has_vote not in (0, 1):
            raise CorruptState(f"bad vote flag {has_vote}")
        return cls(term, voted_for if has_vote else None)


def save(fs: FileSystem, directory: str, state: HardState) -> None:
    """Replace the saved state. When this returns, it is durable."""
    replace_file(fs, os.path.join(directory, STATE_NAME), state.encode())


def load(fs: FileSystem, directory: str) -> HardState:
    """The saved state, or term 0 with no vote for a node that never saved one."""
    path = os.path.join(directory, STATE_NAME)
    if not fs.exists(path):
        return HardState()
    return HardState.decode(fs.read_file(path))
