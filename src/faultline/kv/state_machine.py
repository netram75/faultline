"""The state machine: the data each node builds by applying committed commands.

Raft only agrees on a list of commands. This is where those commands turn
into data. Every node applies the same committed commands in the same order
to its own storage engine, so every node ends up with the same data.

The engine holds more than the user's keys. It also remembers the index of
the last command applied, so that after a restart we know where to carry on.
The keys are kept apart by a one-byte prefix, so a user key can never clash
with ours:

    b"d" + key       the user's data
    b"s" + client    the client's session: its last seq and that result
    b"m/applied"     index of the last applied command, 8 bytes, big endian

The most important line in this file is the one that writes the data change
and the new applied index in **one** WriteBatch. A batch is atomic (see
lsm/batch.py), so after a crash both are there or neither is. If they were
two writes, a crash between them would either lose the command or apply it
twice after the restart.

**Sessions** (Raft thesis, section 6.3). A client that gets no answer can't
tell whether its command was lost or only the answer was. So it sends the
same command again, with the same sequence number. If the first one did run,
a plain "apply it again" would run it twice: a compare-and-set would now
fail, an increment would count twice. So for every client we remember the
last seq we ran and its result. A command with that seq is a repeat: it gets
the saved result and changes nothing. The session is written in the same
batch as the change, for the same reason as the applied index.

Clients send one command at a time and wait for its answer, so remembering
the last one per client is enough.
"""

import struct
from dataclasses import dataclass
from typing import Self

from faultline.fs import FileSystem
from faultline.kv.commands import ClientId, Command, CompareAndSet, Delete, Operation, Put, decode
from faultline.lsm import Engine, Options, WriteBatch

DATA = b"d"
SESSION = b"s"
APPLIED_KEY = b"m/applied"

_SESSION = struct.Struct(">QBB")  # seq, ok, has_value, then the value
_LENGTH = struct.Struct("<I")


@dataclass(frozen=True)
class Result:
    """What a command did. For compare-and-set, ok says whether it matched,
    and value is the key's value after it ran."""

    ok: bool
    value: bytes | None = None


class StateMachine:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        saved = engine.get(APPLIED_KEY)
        self.applied_index = 0 if saved is None else int.from_bytes(saved, "big")

    @classmethod
    def open(cls, fs: FileSystem, directory: str, options: Options | None = None) -> Self:
        return cls(Engine.open(fs, directory, options))

    def get(self, key: bytes) -> bytes | None:
        return self._engine.get(DATA + key)

    def apply(self, index: int, command: bytes) -> Result | None:
        """Apply the command at log index `index`. Returns its result, or
        None for the empty command a new leader adds (a no-op) and for a
        command older than the client's latest, which nobody waits for.

        Indexes must come one after another. The caller skips anything at or
        below applied_index, which it already applied before a restart.
        """
        if index != self.applied_index + 1:
            raise ValueError(f"applying {index} after {self.applied_index}")
        batch = WriteBatch()
        result = None if command == b"" else self._run_once(decode(command), batch)
        # Same batch as the change itself. See the top of this file.
        batch.put(APPLIED_KEY, index.to_bytes(8, "big"))
        self._engine.write(batch)
        self.applied_index = index
        return result

    def snapshot(self) -> bytes:
        """Everything in the engine, sessions and applied index included, as
        bytes Raft can keep (see raft/snapshot.py). Format: for each key in
        order, key length u32, key, value length u32, value.

        It holds all the data in memory at once. Fine at the sizes this
        project runs at; a real database streams it in chunks.
        """
        parts = []
        for key, value in self._engine.scan():
            parts += [_LENGTH.pack(len(key)), key, _LENGTH.pack(len(value)), value]
        return b"".join(parts)

    def restore(self, data: bytes) -> None:
        """Throw everything away and load a snapshot made by snapshot(), most
        likely on the leader. Used when this node fell so far behind that the
        leader no longer had the log entries it was missing.

        One batch, so a crash leaves the old data or the new, never a mix.
        """
        pairs = _decode_pairs(data)
        batch = WriteBatch()
        for key, _ in self._engine.scan():
            batch.delete(key)
        for key, value in pairs:
            batch.put(key, value)
        self._engine.write(batch)
        self.applied_index = int.from_bytes(dict(pairs).get(APPLIED_KEY, b""), "big")

    def close(self) -> None:
        self._engine.close()

    def _run_once(self, command: Command, batch: WriteBatch) -> Result | None:
        """Run the command, unless this client already sent it. See the top of
        this file."""
        key = _session_key(command.client)
        saved = self._engine.get(key)
        if saved is not None:
            last_seq, last_result = _decode_session(saved)
            if command.seq == last_seq:
                return last_result  # a retry: same answer, nothing runs again
            if command.seq < last_seq:
                return None  # older still: the client has moved on
        result = self._run(command.op, batch)
        batch.put(key, _encode_session(command.seq, result))
        return result

    def _run(self, op: Operation, batch: WriteBatch) -> Result:
        """Work out what `op` does and add its change to `batch`."""
        match op:
            case Put():
                batch.put(DATA + op.key, op.value)
                return Result(ok=True)
            case Delete():
                batch.delete(DATA + op.key)
                return Result(ok=True)
            case CompareAndSet():
                current = self.get(op.key)
                if current != op.expected:
                    return Result(ok=False, value=current)
                batch.put(DATA + op.key, op.value)
                return Result(ok=True, value=op.value)


def _session_key(client: ClientId) -> bytes:
    return SESSION + client.to_bytes(8, "big")


def _encode_session(seq: int, result: Result) -> bytes:
    header = _SESSION.pack(seq, result.ok, result.value is not None)
    return header + (result.value or b"")


def _decode_session(data: bytes) -> tuple[int, Result]:
    seq, ok, has_value = _SESSION.unpack_from(data)
    value = data[_SESSION.size :] if has_value else None
    return seq, Result(bool(ok), value)


def _decode_pairs(data: bytes) -> list[tuple[bytes, bytes]]:
    pairs = []
    offset = 0
    while offset < len(data):
        key, offset = _read_blob(data, offset)
        value, offset = _read_blob(data, offset)
        pairs.append((key, value))
    return pairs


def _read_blob(data: bytes, offset: int) -> tuple[bytes, int]:
    (length,) = _LENGTH.unpack_from(data, offset)
    start = offset + _LENGTH.size
    if start + length > len(data):
        raise ValueError("snapshot data ends too early")
    return data[start : start + length], start + length
