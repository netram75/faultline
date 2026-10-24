"""A simulated client: picks operations, finds the leader, retries.

Each client does one operation at a time, over and over: a put, a
compare-and-set or a get, on a small set of keys shared with the other
clients (a few keys means lots of clashes, which is where bugs hide).

To run an operation it asks a node. A node that is down or is not the leader
says no at once, and the client asks another one. A node that says yes may
still never answer: it may crash, lose leadership, or the answer may be lost.
So every try has a timeout, after which the client asks again, maybe another
node. A write keeps its seq on every try, so the cluster runs it at most once
(sessions, see kv/state_machine.py).

Every operation ends one of three ways:

* **ok:** an answer came back. For a write, the result says what happened.
* **fail:** it surely did not happen. A read that got no answer never
  changed anything, and a write that no node ever accepted never got into
  any log.
* **unknown:** the client gave up waiting on a write that some node did
  accept. It may have happened or not, and may still happen later. The
  checker (Day 8) has to allow both.
"""

import random
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Literal

from faultline.kv.commands import ClientId, Command, CompareAndSet, Put
from faultline.kv.state_machine import Result
from faultline.raft.messages import NodeId
from faultline.sim.loop import EventLoop

type Kind = Literal["put", "cas", "get"]

# A read's ctx carries the client id in its top bits, so the simulation knows
# whom an answer is for
CTX_SHIFT = 40


class Outcome(Enum):
    OK = "ok"
    FAIL = "fail"
    UNKNOWN = "info"  # the name Jepsen uses for "may or may not have happened"


@dataclass(frozen=True)
class Operation:
    kind: Kind
    key: bytes
    value: bytes | None = None  # put and cas: the new value
    expected: bytes | None = None  # cas: the value it must find (None: missing)


@dataclass
class ClientTimeouts:
    attempt: int = 150  # ms to wait for an answer before asking again
    give_up: int = 1500  # ms after which a write is unknown and a read failed
    max_think: int = 30  # ms between two operations, at most


class SimClient:
    def __init__(
        self,
        loop: EventLoop,
        client_id: ClientId,
        node_ids: Sequence[NodeId],
        submit: Callable[[NodeId, Command], bool],
        read: Callable[[NodeId, bytes, int], bool],
        keys: Sequence[bytes],
        timeouts: ClientTimeouts | None = None,
    ) -> None:
        self.id = client_id
        self.counts = {outcome: 0 for outcome in Outcome}
        # Called when an operation ends: (operation, outcome, value read or
        # result, start time, end time). The history (history.py) hooks in here.
        self.on_done: Callable[[Operation, Outcome, Result | bytes | None, int, int], None] = (
            lambda *args: None
        )
        self._loop = loop
        self._rng = random.Random(loop.seed_for(f"client{client_id}"))
        self._nodes = list(node_ids)
        self._submit = submit
        self._read = read
        self._keys = list(keys)
        self._timeouts = timeouts or ClientTimeouts()
        self._seq = 0
        self._reads = 0
        self._values_made = 0
        self._seen: dict[bytes, bytes | None] = {}  # last value seen per key, for cas
        self._op: Operation | None = None
        self._command: Command | None = None
        self._ctxs: set[int] = set()  # every ctx tried for the current read
        self._attempt = 0
        self._accepted = False
        self._started = 0
        self._leader_guess: NodeId | None = None

    def start(self) -> None:
        self._loop.after(self._rng.randint(0, self._timeouts.max_think), self._begin)

    def answer(self, command: Command, result: Result) -> None:
        """A node's answer to a write. Late answers to older ones are ignored."""
        if self._command is not None and command.seq == self._command.seq:
            self._finish(Outcome.OK, result)

    def answer_read(self, ctx: int, value: bytes | None) -> None:
        # An answer to any try of the current read will do: each one was a
        # real read of the database at some moment while we waited.
        if self._op is not None and self._op.kind == "get" and ctx in self._ctxs:
            self._finish(Outcome.OK, value)

    def _begin(self) -> None:
        self._op = self._pick()
        self._started = self._loop.now
        self._accepted = False
        self._ctxs = set()
        self._command = None
        if self._op.kind == "put":
            assert self._op.value is not None
            self._command = self._new_command(Put(self._op.key, self._op.value))
        elif self._op.kind == "cas":
            assert self._op.value is not None
            self._command = self._new_command(
                CompareAndSet(self._op.key, self._op.expected, self._op.value)
            )
        self._try()

    def _pick(self) -> Operation:
        key = self._rng.choice(self._keys)
        roll = self._rng.random()
        if roll < 0.4:
            return Operation("get", key)
        if roll < 0.7:
            return Operation("put", key, self._new_value())
        # Expect the value we last saw, so a good share of these succeed
        return Operation("cas", key, self._new_value(), self._seen.get(key))

    def _new_value(self) -> bytes:
        # Unique in the whole run, which the checker relies on
        self._values_made += 1
        return b"%d-%d" % (self.id, self._values_made)

    def _new_command(self, op: Put | CompareAndSet) -> Command:
        self._seq += 1
        return Command(self.id, self._seq, op)

    def _try(self) -> None:
        """Send the current operation to a node, and start a timer for it."""
        self._attempt += 1
        node = self._leader_guess or self._rng.choice(self._nodes)
        self._loop.after(self._rng.randint(1, 5), partial(self._arrive, node, self._attempt))
        self._loop.after(self._timeouts.attempt, partial(self._timed_out, self._attempt))

    def _arrive(self, node: NodeId, attempt: int) -> None:
        if attempt != self._attempt or self._op is None:
            return
        if self._command is not None:
            accepted = self._submit(node, self._command)
            self._accepted = self._accepted or accepted
        else:
            self._reads += 1
            ctx = (self.id << CTX_SHIFT) | self._reads
            self._ctxs.add(ctx)
            accepted = self._read(node, self._op.key, ctx)
        if accepted:
            self._leader_guess = node
        else:
            # A quick no: ask someone else soon, without waiting for the timeout
            self._leader_guess = None
            self._attempt += 1
            self._loop.after(self._rng.randint(1, 10), self._retry_now)

    def _retry_now(self) -> None:
        if self._op is not None:
            self._give_up_or_try()

    def _timed_out(self, attempt: int) -> None:
        if attempt != self._attempt or self._op is None:
            return
        self._leader_guess = None
        self._give_up_or_try()

    def _give_up_or_try(self) -> None:
        if self._loop.now - self._started < self._timeouts.give_up:
            self._try()
        elif self._command is not None and self._accepted:
            self._finish(Outcome.UNKNOWN, None)
        else:
            self._finish(Outcome.FAIL, None)

    def _finish(self, outcome: Outcome, result: Result | bytes | None) -> None:
        op = self._op
        assert op is not None
        self._op = None
        self._command = None
        self._attempt += 1  # timers for this operation now do nothing
        self.counts[outcome] += 1
        if outcome is Outcome.OK:
            self._remember(op, result)
        self.on_done(op, outcome, result, self._started, self._loop.now)
        self._loop.after(self._rng.randint(0, self._timeouts.max_think), self._begin)

    def _remember(self, op: Operation, result: Result | bytes | None) -> None:
        if op.kind == "get":
            assert not isinstance(result, Result)
            self._seen[op.key] = result
        elif isinstance(result, Result) and result.ok:
            self._seen[op.key] = op.value
        elif isinstance(result, Result):
            self._seen[op.key] = result.value  # a failed cas tells us the value
