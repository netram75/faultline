"""RESP, the Redis wire protocol, so any Redis client can talk to faultline.

Every value starts with one byte that says what it is, and lines end with
\r\n:

    +OK\r\n                   simple string
    -ERR no such key\r\n      error
    :42\r\n                   integer
    $5\r\nhello\r\n           bulk string: length, then that many bytes
    $-1\r\n                   null (a missing value)
    *2\r\n$3\r\nGET\r\n$1\r\nk\r\n
                              array: a count, then that many values

A client sends each command as an array of bulk strings, like the last
line above (GET k). Bulk strings carry their length, so they can hold any
bytes, \r\n included.

In Python:

    bytes           bulk string
    None            null
    int             integer
    SimpleString    simple string
    RespError       error
    list            array

TCP is a stream, so a value can arrive in pieces, or several at once.
Parser keeps the bytes that came so far and hands back each value once all
of it is there.
"""

import re
from dataclasses import dataclass

CRLF = b"\r\n"

# Limits, so a bad or hostile client can't make us hold on to huge amounts
# of memory waiting for the rest of a value
MAX_LINE = 64 * 1024
MAX_BULK = 64 * 1024 * 1024
MAX_ITEMS = 1024 * 1024
MAX_DEPTH = 32

_INTEGER = re.compile(rb"-?[0-9]{1,19}")


@dataclass(frozen=True)
class SimpleString:
    text: str


@dataclass(frozen=True)
class RespError:
    message: str


type Value = bytes | int | SimpleString | RespError | list[Value] | None


class ProtocolError(Exception):
    """The bytes are not RESP. The connection can't be trusted after this."""


class _Incomplete(Exception):
    """The rest of the value has not arrived yet."""


def encode(value: Value) -> bytes:
    match value:
        case None:
            return b"$-1\r\n"
        case bool():
            # True is an int in Python. RESP2 has no booleans, so say no
            # instead of quietly sending :1
            raise TypeError("RESP has no booleans")
        case int():
            return b":%d\r\n" % value
        case bytes():
            return b"$%d\r\n%s\r\n" % (len(value), value)
        case SimpleString(text):
            return b"+" + _one_line(text) + CRLF
        case RespError(message):
            return b"-" + _one_line(message) + CRLF
        case list():
            return b"*%d\r\n" % len(value) + b"".join(encode(item) for item in value)
    raise TypeError(f"can't send a {type(value).__name__} over RESP")


class Parser:
    """Feed it bytes as they arrive, get back the values that are complete."""

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[Value]:
        self._buffer += data
        values: list[Value] = []
        while self._buffer:
            try:
                value, end = _parse(self._buffer, 0, 0)
            except _Incomplete:
                break
            del self._buffer[:end]
            values.append(value)
        return values

    @property
    def waiting(self) -> int:
        """Bytes received that are not part of a whole value yet."""
        return len(self._buffer)


def _parse(buffer: bytearray, pos: int, depth: int) -> tuple[Value, int]:
    """Read one value starting at pos. Returns it and where it ends."""
    line, pos = _line(buffer, pos)
    kind, rest = line[:1], line[1:]
    if kind == b"+":
        return SimpleString(_text(rest)), pos
    if kind == b"-":
        return RespError(_text(rest)), pos
    if kind == b":":
        return _integer(rest), pos
    if kind == b"$":
        length = _integer(rest)
        if length == -1:
            return None, pos
        if not 0 <= length <= MAX_BULK:
            raise ProtocolError(f"bad bulk string length {length}")
        end = pos + length
        if len(buffer) < end + 2:
            raise _Incomplete
        if buffer[end : end + 2] != CRLF:
            raise ProtocolError("bulk string is longer than its length")
        return bytes(buffer[pos:end]), end + 2
    if kind == b"*":
        count = _integer(rest)
        if count == -1:
            return None, pos
        if not 0 <= count <= MAX_ITEMS:
            raise ProtocolError(f"bad array length {count}")
        if depth == MAX_DEPTH:
            raise ProtocolError("arrays nested too deep")
        items: list[Value] = []
        for _ in range(count):
            item, pos = _parse(buffer, pos, depth + 1)
            items.append(item)
        return items, pos
    raise ProtocolError(f"unknown type byte {kind!r}")


def _line(buffer: bytearray, pos: int) -> tuple[bytes, int]:
    end = buffer.find(CRLF, pos)
    if end == -1:
        if len(buffer) - pos > MAX_LINE:
            raise ProtocolError("line too long")
        raise _Incomplete
    return bytes(buffer[pos:end]), end + 2


def _integer(data: bytes) -> int:
    # Stricter than int(), which would also take " 7", "+7" and "1_000"
    if not _INTEGER.fullmatch(data):
        raise ProtocolError(f"bad integer {data!r}")
    return int(data)


def _text(data: bytes) -> str:
    try:
        return data.decode()
    except UnicodeDecodeError as e:
        raise ProtocolError("text is not UTF-8") from e


def _one_line(text: str) -> bytes:
    if "\r" in text or "\n" in text:
        raise ValueError("a simple string or error can't hold \\r or \\n")
    return text.encode()
