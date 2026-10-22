"""Write batches: several changes that go in together or not at all.

A batch is what the engine writes to the WAL. One batch becomes one WAL
record, and a WAL record is read back whole or not at all (it has its own
checksum). So after a crash we never see half a batch. Raft will lean on
this later: one log entry turns into one batch.

Format, numbers little endian:

    count u32
    then for each change:
        kind u8 (1 = put, 0 = delete), key length u32, key
        and for a put only: value length u32, value
"""

import struct
from collections.abc import Iterator
from typing import Self

from faultline.entry import TOMBSTONE, Tombstone, Value
from faultline.memtable import Memtable

_PUT = 1
_DELETE = 0
_COUNT = struct.Struct("<I")
_CHANGE = struct.Struct("<BI")  # kind, key length
_LENGTH = struct.Struct("<I")


class CorruptBatch(Exception):
    """The bytes are not a batch we wrote."""


class WriteBatch:
    """A list of puts and deletes, applied in the order they were added.

    Calls can be chained: WriteBatch().put(b"a", b"1").delete(b"b")
    """

    def __init__(self) -> None:
        self._changes: list[tuple[bytes, Value]] = []

    def put(self, key: bytes, value: bytes) -> Self:
        self._changes.append((key, value))
        return self

    def delete(self, key: bytes) -> Self:
        self._changes.append((key, TOMBSTONE))
        return self

    def __len__(self) -> int:
        return len(self._changes)

    def __iter__(self) -> Iterator[tuple[bytes, Value]]:
        return iter(self._changes)

    def apply(self, memtable: Memtable) -> None:
        # A key can show up twice in one batch. Going in order means the
        # last change to it wins, same as if they were separate writes.
        for key, value in self._changes:
            if isinstance(value, Tombstone):
                memtable.delete(key)
            else:
                memtable.put(key, value)

    def encode(self) -> bytes:
        parts = [_COUNT.pack(len(self._changes))]
        for key, value in self._changes:
            if isinstance(value, Tombstone):
                parts += [_CHANGE.pack(_DELETE, len(key)), key]
            else:
                parts += [_CHANGE.pack(_PUT, len(key)), key, _LENGTH.pack(len(value)), value]
        return b"".join(parts)

    @classmethod
    def decode(cls, data: bytes) -> Self:
        reader = _Reader(data)
        batch = cls()
        (count,) = reader.unpack(_COUNT)
        for _ in range(count):
            kind, key_length = reader.unpack(_CHANGE)
            key = reader.take(key_length)
            if kind == _PUT:
                (value_length,) = reader.unpack(_LENGTH)
                batch.put(key, reader.take(value_length))
            elif kind == _DELETE:
                batch.delete(key)
            else:
                raise CorruptBatch(f"unknown change kind {kind}")
        if not reader.done:
            raise CorruptBatch("extra bytes after the last change")
        return batch


class _Reader:
    """Walks through the bytes of a batch and complains if they run out."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._offset = 0

    @property
    def done(self) -> bool:
        return self._offset == len(self._data)

    def take(self, length: int) -> bytes:
        end = self._offset + length
        if end > len(self._data):
            raise CorruptBatch("batch ends too early")
        chunk = self._data[self._offset : end]
        self._offset = end
        return chunk

    def unpack(self, layout: struct.Struct) -> tuple[int, ...]:
        return layout.unpack(self.take(layout.size))
