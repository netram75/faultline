"""The commands the database puts in the Raft log.

Every change to the data is one command. The leader adds it to the log, Raft
copies it to the others, and once it is committed every node applies it to
its own copy of the data, in the same order. Same commands, same order, same
data on every node.

Reads are not commands. They don't change anything, so they don't need to go
through the log (see ReadIndex, later).

Each command also says which client sent it and that client's sequence
number. A client that gets no answer sends the same command again, with the
same number. The number is how the database spots the repeat and makes sure
it runs once, not twice (client sessions, Raft thesis section 6.3).

Format, numbers little endian:

    client u64, seq u64, kind u8, then
    put:             key length u32, key, value length u32, value
    delete:          key length u32, key
    compare and set: key length u32, key, has_expected u8,
                     [expected length u32, expected], value length u32, value
"""

import struct
from dataclasses import dataclass

type ClientId = int


@dataclass(frozen=True)
class Put:
    key: bytes
    value: bytes


@dataclass(frozen=True)
class Delete:
    key: bytes


@dataclass(frozen=True)
class CompareAndSet:
    """Set `key` to `value`, but only if it is `expected` right now.
    expected=None means "only if the key is missing"."""

    key: bytes
    expected: bytes | None
    value: bytes


type Operation = Put | Delete | CompareAndSet


@dataclass(frozen=True)
class Command:
    client: ClientId
    seq: int  # 1, 2, 3, ... for each client. A retry keeps its number.
    op: Operation


class CorruptCommand(Exception):
    """The bytes are not a command we wrote."""


_HEADER = struct.Struct("<QQB")  # client, seq, kind
_LENGTH = struct.Struct("<I")
_FLAG = struct.Struct("<B")

_PUT = 1
_DELETE = 2
_CAS = 3


def encode(command: Command) -> bytes:
    op = command.op
    match op:
        case Put():
            parts = [_HEADER.pack(command.client, command.seq, _PUT), *_blob(op.key)]
            parts += _blob(op.value)
        case Delete():
            parts = [_HEADER.pack(command.client, command.seq, _DELETE), *_blob(op.key)]
        case CompareAndSet():
            parts = [_HEADER.pack(command.client, command.seq, _CAS), *_blob(op.key)]
            if op.expected is None:
                parts.append(_FLAG.pack(0))
            else:
                parts += [_FLAG.pack(1), *_blob(op.expected)]
            parts += _blob(op.value)
    return b"".join(parts)


def decode(data: bytes) -> Command:
    reader = _Reader(data)
    client, seq, kind = reader.unpack(_HEADER)
    op: Operation
    if kind == _PUT:
        op = Put(reader.blob(), reader.blob())
    elif kind == _DELETE:
        op = Delete(reader.blob())
    elif kind == _CAS:
        key = reader.blob()
        (has_expected,) = reader.unpack(_FLAG)
        if has_expected not in (0, 1):
            raise CorruptCommand(f"bad flag {has_expected}")
        expected = reader.blob() if has_expected else None
        op = CompareAndSet(key, expected, reader.blob())
    else:
        raise CorruptCommand(f"unknown command kind {kind}")
    if not reader.done:
        raise CorruptCommand("extra bytes after the command")
    return Command(client, seq, op)


def _blob(data: bytes) -> list[bytes]:
    return [_LENGTH.pack(len(data)), data]


class _Reader:
    """Walks through the bytes of a command and complains if they run out."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._offset = 0

    @property
    def done(self) -> bool:
        return self._offset == len(self._data)

    def take(self, length: int) -> bytes:
        end = self._offset + length
        if end > len(self._data):
            raise CorruptCommand("command ends too early")
        chunk = self._data[self._offset : end]
        self._offset = end
        return chunk

    def unpack(self, layout: struct.Struct) -> tuple[int, ...]:
        return layout.unpack(self.take(layout.size))

    def blob(self) -> bytes:
        (length,) = self.unpack(_LENGTH)
        return self.take(length)
