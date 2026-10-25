"""The client port: Redis clients connect here and send commands.

Each connection is read with resp.Parser. Every command becomes a request
(requests.py) and gets its answer in the order it came, so clients can send
several at once (pipelining).

Reads go through ReadIndex (raft/read_index.py), so a GET never sees old
data, even from a leader that was just replaced and doesn't know it yet.
Writes go into the Raft log and are answered once applied.

Sessions: every connection is a client of its own, with a random 64-bit id,
and numbers its writes 1, 2, 3, ... (see kv/state_machine.py). The server
does not retry anything. If a client gets a timeout and sends the write
again on a new connection, that is a new session, and the write may happen
twice. Real Redis is the same.

Answers:

    PING            +PONG, or the message back
    INFO            numbers about this node, as text (see metrics.py)
    GET             the value, or null if the key is missing
    SET             +OK
    DEL             :1 (faultline does not track whether the key was there)
    SETNX, CAS      :1 if it set the value, :0 if not

Errors:

    -NOTLEADER leader is node 2 at 127.0.0.1:6402    ask that node instead
    -TRYAGAIN no leader right now                    an election is going on
    -TIMEOUT ...                                     no answer in time
    -ERR ...                                         a bad command

A TIMEOUT on a write means nobody knows if it happened. It may still
happen later. That is the "info" outcome the checker deals with.
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass

from faultline.kv.commands import Command, CompareAndSet, Delete, Operation, Put
from faultline.kv.state_machine import Result
from faultline.net.metrics import Metrics, info_text
from faultline.net.node import Address, NodeRunner
from faultline.net.requests import BadRequest, Get, Info, Ping, Request, parse_request
from faultline.net.resp import Parser, ProtocolError, RespError, SimpleString, Value, encode
from faultline.raft.messages import NodeId

log = logging.getLogger("faultline")


@dataclass
class _Session:
    client: int
    seq: int = 0


class ClientServer:
    def __init__(
        self, runner: NodeRunner, clients: dict[NodeId, Address], timeout: float = 2.0
    ) -> None:
        """`clients` has the client address of every node, so we can tell a
        client where the leader is."""
        self.runner = runner
        self.clients = clients
        self.timeout = timeout
        self.metrics = Metrics()
        self._server: asyncio.Server | None = None
        self._connections: set[asyncio.StreamWriter] = set()
        self._stopping = False

    async def start(self) -> None:
        address = self.clients[self.runner.id]
        self._server = await asyncio.start_server(self._serve, address.host, address.port)
        log.info("node %d: clients on %s", self.runner.id, address)

    async def stop(self) -> None:
        self._stopping = True
        if self._server is not None:
            self._server.close()
        for writer in list(self._connections):
            writer.close()
        if self._server is not None:
            # Waits for every connection to close, like NodeRunner.stop()
            await self._server.wait_closed()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self._stopping:
            writer.close()
            return
        self._connections.add(writer)
        self.metrics.connections += 1
        session = _Session(random.SystemRandom().getrandbits(64))
        parser = Parser()
        try:
            while data := await reader.read(64 * 1024):
                for value in parser.feed(data):
                    writer.write(encode(await self._answer(value, session)))
                await writer.drain()
        except ProtocolError as e:
            # Like Redis: say why, then hang up. We can't find the start of
            # the next command in a broken stream.
            writer.write(encode(RespError(f"ERR Protocol error: {e}")))
        except ConnectionError:
            pass
        finally:
            self._connections.discard(writer)
            self.metrics.connections -= 1
            writer.close()

    async def _answer(self, value: Value, session: _Session) -> Value:
        try:
            request = parse_request(value)
        except BadRequest as e:
            self.metrics.record("bad", 0, "ERR")
            return RespError(str(e))
        started = time.monotonic()
        reply = await self._run(request, session)
        error = reply.message.split()[0] if isinstance(reply, RespError) else None
        self.metrics.record(_name(request), time.monotonic() - started, error)
        return reply

    async def _run(self, request: Request, session: _Session) -> Value:
        if not self.runner.running:
            return RespError("TRYAGAIN the node is stopping")

        match request:
            case Ping(message):
                return SimpleString("PONG") if message is None else message
            case Info():
                return info_text(self.metrics, self.runner).encode()
            case Get(key):
                read = self.runner.read(key)
                if read is None:
                    return self._not_leader()
                try:
                    return await self._wait(read)
                except TimeoutError:
                    return RespError("TIMEOUT no answer in time, try again")
            case op:
                return await self._write(op, session)

    async def _write(self, op: Operation, session: _Session) -> Value:
        session.seq += 1
        future = self.runner.submit(Command(session.client, session.seq, op))
        if future is None:
            return self._not_leader()  # it surely did not happen
        try:
            result = await self._wait(future)
        except TimeoutError:
            return RespError("TIMEOUT no answer in time, the write may or may not have happened")
        return _write_reply(op, result)

    async def _wait[T](self, future: "asyncio.Future[T]") -> T:
        """Wait for an answer, up to the timeout. TimeoutError if none came,
        or if the node stopped first."""
        await asyncio.wait([future], timeout=self.timeout)
        if not future.done() or future.cancelled():
            future.cancel()
            raise TimeoutError
        return future.result()

    def _not_leader(self) -> RespError:
        leader = self.runner.kv.raft.leader
        if leader is None:
            return RespError("TRYAGAIN no leader right now")
        if leader == self.runner.id:
            # A brand new leader can't serve reads until it has committed
            # an entry of its own term, which takes one round trip
            return RespError("TRYAGAIN the leader is just starting")
        return RespError(f"NOTLEADER leader is node {leader} at {self.clients[leader]}")


def _name(request: Request) -> str:
    match request:
        case CompareAndSet(expected=None):
            return "setnx"
        case CompareAndSet():
            return "cas"
        case Put():
            return "set"
        case Delete():
            return "del"
    return type(request).__name__.lower()  # ping, get, info


def _write_reply(op: Operation, result: Result) -> Value:
    match op:
        case Put():
            return SimpleString("OK")
        case Delete():
            return 1
        case CompareAndSet():
            return 1 if result.ok else 0
