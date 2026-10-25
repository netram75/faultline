"""Frames: how messages between nodes are cut out of a TCP stream.

TCP hands over a stream of bytes with no edges in it. One read may hold half
a message, or three. So each Raft message (raft/messages.py) goes inside a
frame that says how long it is:

    length u32, little endian
    then that many bytes: the encoded message

FrameReader keeps the bytes that came so far and hands back each payload
once all of it is there, the same way resp.Parser does for clients.

No checksum here. TCP already checks every packet, and decode() in
raft/messages.py rejects anything that is not a message.
"""

import struct

_LENGTH = struct.Struct("<I")

# A snapshot travels in one message, so frames can be big. But a length
# past this means the other side is broken or is not a faultline node, and
# we should not sit there waiting for gigabytes.
MAX_FRAME = 64 * 1024 * 1024


class FrameError(Exception):
    """The stream is not frames we understand. Close the connection."""


def frame(payload: bytes) -> bytes:
    if len(payload) > MAX_FRAME:
        raise FrameError(f"message of {len(payload)} bytes is too big to send")
    return _LENGTH.pack(len(payload)) + payload


class FrameReader:
    """Feed it bytes as they arrive, get back the payloads that are complete."""

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[bytes]:
        self._buffer += data
        payloads: list[bytes] = []
        while len(self._buffer) >= _LENGTH.size:
            (length,) = _LENGTH.unpack_from(self._buffer)
            if length > MAX_FRAME:
                raise FrameError(f"frame of {length} bytes is too big")
            end = _LENGTH.size + length
            if len(self._buffer) < end:
                break
            payloads.append(bytes(self._buffer[_LENGTH.size : end]))
            del self._buffer[:end]
        return payloads

    @property
    def waiting(self) -> int:
        """Bytes received that are not part of a whole frame yet."""
        return len(self._buffer)
