"""The state machine: the data each node builds by applying committed commands.

Raft only agrees on a list of commands. This is where those commands turn
into data. Every node applies the same committed commands in the same order
to its own storage engine, so every node ends up with the same data.

The engine holds more than the user's keys. It also remembers the index of
the last command applied, so that after a restart we know where to carry on.
The keys are kept apart by a one-byte prefix, so a user key can never clash
with ours:

    b"d" + key       the user's data
    b"m/applied"     index of the last applied command, 8 bytes, big endian

The most important line in this file is the one that writes the data change
and the new applied index in **one** WriteBatch. A batch is atomic (see
lsm/batch.py), so after a crash both are there or neither is. If they were
two writes, a crash between them would either lose the command or apply it
twice after the restart.
"""

from dataclasses import dataclass
from typing import Self

from faultline.fs import FileSystem
from faultline.kv.commands import CompareAndSet, Delete, Operation, Put, decode
from faultline.lsm import Engine, Options, WriteBatch

DATA = b"d"
APPLIED_KEY = b"m/applied"


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
        None for the empty command a new leader adds (a no-op).

        Indexes must come one after another. The caller skips anything at or
        below applied_index, which it already applied before a restart.
        """
        if index != self.applied_index + 1:
            raise ValueError(f"applying {index} after {self.applied_index}")
        batch = WriteBatch()
        result = None if command == b"" else self._run(decode(command).op, batch)
        # Same batch as the change itself. See the top of this file.
        batch.put(APPLIED_KEY, index.to_bytes(8, "big"))
        self._engine.write(batch)
        self.applied_index = index
        return result

    def close(self) -> None:
        self._engine.close()

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
