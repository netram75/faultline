"""The history: every client operation, when it started and how it ended.

This is what the linearizability checker (checker/) reads. It does not look inside
the cluster at all. It only asks: could a single copy of the data, doing one
operation at a time, have given exactly these answers? That property is
called linearizability, and it is what a client of a strongly consistent
database expects.

Each operation gives two events. An "invoke" when the client starts it, and
one of these when it ends (see client.py):

    ok      it happened, with this result
    fail    it surely did not happen
    info    nobody knows: it may have happened, any time after the invoke

Events are kept in the order they happened, which is time order. One event
per line of JSON (JSON lines). A compare-and-set by client 1 that started at
1840 ms and matched at 1857 ms:

    {"client": 1, "type": "invoke", "op": "cas", "key": "k0", "value": "1-7",
     "expected": "2-3", "result": null, "time": 1840}
    {"client": 1, "type": "ok", "op": "cas", "key": "k0", "value": "1-7",
     "expected": "2-3", "result": true, "time": 1857}

(each on one line in the file).

For a get that ended ok, value is the value read (null if the key was
missing). For a put or a cas, value is the value it writes. result is true
or false for a cas that ended ok (did the compare match), true for a put
that ended ok, and null otherwise.
"""

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Literal

from faultline.kv.commands import ClientId
from faultline.kv.state_machine import Result
from faultline.sim.client import Kind, Operation, Outcome
from faultline.sim.loop import EventLoop

type EventType = Literal["invoke", "ok", "fail", "info"]

_END: dict[Outcome, EventType] = {Outcome.OK: "ok", Outcome.FAIL: "fail", Outcome.UNKNOWN: "info"}


@dataclass(frozen=True)
class Event:
    client: ClientId
    type: EventType
    op: Kind
    key: str
    value: str | None
    expected: str | None
    result: bool | None
    time: int


class History:
    def __init__(self, loop: EventLoop | None = None) -> None:
        self.events: list[Event] = []
        self._loop = loop

    def invoke(self, client: ClientId, op: Operation) -> None:
        value = _text(op.value) if op.kind != "get" else None
        self._add(
            Event(client, "invoke", op.kind, _key(op), value, _expected(op), None, self._now())
        )

    def complete(
        self, client: ClientId, op: Operation, outcome: Outcome, result: Result | bytes | None
    ) -> None:
        value = _text(op.value)
        ok_result: bool | None = None
        if outcome is Outcome.OK:
            if op.kind == "get":
                assert not isinstance(result, Result)
                value = _text(result)
            else:
                assert isinstance(result, Result)
                ok_result = result.ok
        self._add(
            Event(
                client,
                _END[outcome],
                op.kind,
                _key(op),
                value,
                _expected(op),
                ok_result,
                self._now(),
            )
        )

    def to_jsonl(self) -> str:
        return "".join(json.dumps(asdict(event)) + "\n" for event in self.events)

    @classmethod
    def from_jsonl(cls, text: str) -> "History":
        history = cls()
        for line in text.splitlines():
            if line.strip():
                history.events.append(Event(**json.loads(line)))
        return history

    def save(self, path: str) -> None:
        with open(path, "w") as f:
            f.write(self.to_jsonl())

    def digest(self) -> str:
        """A short fingerprint of the whole history. Two runs with the same
        seed must give the same one."""
        return hashlib.sha256(self.to_jsonl().encode()).hexdigest()[:16]

    def _add(self, event: Event) -> None:
        self.events.append(event)

    def _now(self) -> int:
        return 0 if self._loop is None else self._loop.now


def _text(data: bytes | None) -> str | None:
    return None if data is None else data.decode("ascii")


def _key(op: Operation) -> str:
    return op.key.decode("ascii")


def _expected(op: Operation) -> str | None:
    return _text(op.expected) if op.kind == "cas" else None
