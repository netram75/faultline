"""A chaos client: does one operation at a time over the Redis protocol,
and writes down in a History what it asked and what it got.

The same operations and values as the simulator's clients (sim/client.py):
puts, compare-and-sets and gets on a few shared keys, every value written
is new ("3-17" is client 3's 17th value). So the checker can tell exactly
which write a read saw.

How each try ends decides what goes in the history:

    +OK, :1, :0, a value     ok, with that answer
    NOTLEADER, TRYAGAIN      the node did not take it. Ask another node,
                             until the operation's time runs out: then fail.
    TIMEOUT, or the
    connection breaks        for a read: fail, a read changes nothing.
    after a write was sent   for a write: unknown ("info"). It may have
                             happened, and may still happen later. It is
                             never sent again: on a new connection it would
                             be a new session, and could happen twice.
"""

import asyncio
import random
import time

from faultline.kv.state_machine import Result
from faultline.net.client import Client
from faultline.net.node import Address
from faultline.net.resp import RespError, SimpleString, Value
from faultline.raft.messages import NodeId
from faultline.sim.client import Operation, Outcome
from faultline.sim.history import History


class WallClockHistory(History):
    """A History with real time: milliseconds since the run started."""

    def __init__(self) -> None:
        super().__init__()
        self._start = time.monotonic()

    def _now(self) -> int:
        return int((time.monotonic() - self._start) * 1000)


class ChaosClient:
    def __init__(
        self,
        client_id: int,
        clients: dict[NodeId, Address],
        history: History,
        keys: list[bytes],
        rng: random.Random,
        reply_timeout: float = 3.0,
        op_timeout: float = 5.0,
    ) -> None:
        self.id = client_id
        self._clients = clients
        self._history = history
        self._keys = keys
        self._rng = rng
        self._reply_timeout = reply_timeout
        self._op_timeout = op_timeout
        self._connections: dict[NodeId, Client] = {}
        self._guess: NodeId | None = None
        self._values_made = 0
        self._seen: dict[bytes, bytes | None] = {}

    async def run(self, until: float) -> None:
        """Do operations until time.monotonic() reaches `until`."""
        try:
            while time.monotonic() < until:
                op = self._pick()
                self._history.invoke(self.id, op)
                outcome, result = await self.do(op)
                self._history.complete(self.id, op, outcome, result)
                if outcome is Outcome.OK:
                    self._remember(op, result)
                await asyncio.sleep(self._rng.uniform(0, 0.02))
        finally:
            for connection in self._connections.values():
                await connection.close()

    def _pick(self) -> Operation:
        key = self._rng.choice(self._keys)
        roll = self._rng.random()
        if roll < 0.4:
            return Operation("get", key)
        if roll < 0.7:
            return Operation("put", key, self._new_value())
        return Operation("cas", key, self._new_value(), self._seen.get(key))

    def _new_value(self) -> bytes:
        self._values_made += 1
        return b"%d-%d" % (self.id, self._values_made)

    async def do(self, op: Operation) -> tuple[Outcome, Result | bytes | None]:
        """Run one operation to its end, asking other nodes when told to."""
        deadline = time.monotonic() + self._op_timeout
        while time.monotonic() < deadline:
            node = self._guess or self._rng.choice(list(self._clients))
            connection = await self._connect(node)
            if connection is None:
                self._guess = None
                await asyncio.sleep(0.05)
                continue
            try:
                reply = await asyncio.wait_for(connection.call(*_command(op)), self._reply_timeout)
            except (OSError, TimeoutError):
                # Sent, but no answer: the node died, froze, or is slow. The
                # connection may still deliver that answer later, so it can't
                # be used again.
                await self._drop(node)
                if op.kind != "get":
                    return Outcome.UNKNOWN, None
                continue
            if isinstance(reply, RespError):
                kind = reply.message.split()[0]
                if kind == "NOTLEADER":
                    self._guess = _leader_from(reply.message, self._clients)
                    continue
                if kind == "TRYAGAIN":
                    self._guess = None
                    await asyncio.sleep(0.05)
                    continue
                if kind == "TIMEOUT" and op.kind != "get":
                    return Outcome.UNKNOWN, None
                if kind == "TIMEOUT":
                    continue
                raise AssertionError(f"unexpected error from node {node}: {reply.message}")
            self._guess = node
            return Outcome.OK, _result(op, reply)
        # Out of time, and no node ever took it
        return Outcome.FAIL, None

    async def _connect(self, node: NodeId) -> Client | None:
        if node not in self._connections:
            try:
                self._connections[node] = await asyncio.wait_for(
                    Client.connect(self._clients[node]), self._reply_timeout
                )
            except (OSError, TimeoutError):
                return None
        return self._connections[node]

    async def _drop(self, node: NodeId) -> None:
        connection = self._connections.pop(node, None)
        if connection is not None:
            await connection.close()
        self._guess = None

    def _remember(self, op: Operation, result: Result | bytes | None) -> None:
        if op.kind == "get":
            assert not isinstance(result, Result)
            self._seen[op.key] = result
        elif isinstance(result, Result) and result.ok:
            self._seen[op.key] = op.value


def _command(op: Operation) -> tuple[bytes, ...]:
    if op.kind == "get":
        return (b"GET", op.key)
    assert op.value is not None
    if op.kind == "put":
        return (b"SET", op.key, op.value)
    if op.expected is None:
        return (b"SETNX", op.key, op.value)
    return (b"CAS", op.key, op.expected, op.value)


def _result(op: Operation, reply: Value) -> Result | bytes | None:
    if op.kind == "get":
        assert reply is None or isinstance(reply, bytes), reply
        return reply
    if op.kind == "put":
        assert reply == SimpleString("OK"), reply
        return Result(ok=True)
    assert reply in (0, 1), reply
    return Result(ok=reply == 1)


def _leader_from(message: str, clients: dict[NodeId, Address]) -> NodeId | None:
    # "NOTLEADER leader is node 2 at 127.0.0.1:6402"
    words = message.split()
    if len(words) >= 5 and words[4].isdigit() and int(words[4]) in clients:
        return int(words[4])
    return None
