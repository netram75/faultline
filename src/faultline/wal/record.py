"""How one WAL record is laid out on disk.

    +------------+------------+--------------------+
    |  checksum  |   length   |      payload       |
    |  4 bytes   |  4 bytes   |   `length` bytes   |
    +------------+------------+--------------------+

Both numbers are unsigned 32-bit, little-endian. The checksum is CRC32 over
the length bytes and the payload, so a damaged length gets caught too, not
just damaged data.
"""

import struct
import zlib

_HEADER = struct.Struct("<II")
HEADER_SIZE = _HEADER.size

# Real records are far smaller than this. If a length field says more, the
# field itself is garbage, and we should not try to read 4 GB because of it.
MAX_PAYLOAD_SIZE = 16 * 1024 * 1024


class RecordError(Exception):
    """A record could not be read."""


class TornRecord(RecordError):
    """The bytes end before the record does. Usually a crash in the middle of a write."""


class CorruptRecord(RecordError):
    """The bytes are all there, but they are not what was written."""


def encode(payload: bytes) -> bytes:
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ValueError(f"payload is {len(payload)} bytes, the limit is {MAX_PAYLOAD_SIZE}")
    length = struct.pack("<I", len(payload))
    checksum = zlib.crc32(payload, zlib.crc32(length))
    return struct.pack("<I", checksum) + length + payload


def decode(buf: bytes, offset: int = 0) -> tuple[bytes, int]:
    """Read the record that starts at `offset`.

    Returns the payload and the offset where the next record starts.
    """
    header_end = offset + HEADER_SIZE
    if header_end > len(buf):
        raise TornRecord(f"offset {offset}: only {len(buf) - offset} bytes left, need a header")

    checksum, length = _HEADER.unpack_from(buf, offset)
    if length > MAX_PAYLOAD_SIZE:
        raise CorruptRecord(f"offset {offset}: length {length} is over the limit")

    end = header_end + length
    if end > len(buf):
        left = len(buf) - header_end
        raise TornRecord(f"offset {offset}: record needs {length} bytes, only {left} left")

    payload = buf[header_end:end]
    if zlib.crc32(payload, zlib.crc32(buf[offset + 4 : header_end])) != checksum:
        raise CorruptRecord(f"offset {offset}: checksum does not match")

    return payload, end
