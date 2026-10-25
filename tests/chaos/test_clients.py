"""How a chaos client turns answers into history: against fake nodes that
answer exactly what each test says."""

import asyncio
import random

from faultline.chaos.clients import ChaosClient, WallClockHistory
from faultline.kv.state_machine import Result
from faultline.net.node import Address
from faultline.net.resp import Parser, RespError, SimpleString, Value, encode
from faultline.sim.client import Operation, Outcome
from tests.net.helpers import free_ports, run

CLOSE = "close"  # hang up instead of answering


class FakeNode:
    """Answers each command it gets with the next answer on its list."""

    def __init__(self, answers: list[Value | str]) -> None:
        self.answers = answers
        self.got: list[Value] = []
        [port] = free_ports(1)
        self.address = Address("127.0.0.1", port)
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._serve, self.address.host, self.address.port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        parser = Parser()
        while data := await reader.read(1024):
            for command in parser.feed(data):
                self.got.append(command)
                answer = self.answers.pop(0) if self.answers else RespError("TRYAGAIN")
                if answer == CLOSE:
                    writer.close()
                    return
                assert not isinstance(answer, str)
                writer.write(encode(answer))
        writer.close()


def do(op: Operation, *nodes: FakeNode) -> tuple[Outcome, Result | bytes | None]:
    async def body() -> tuple[Outcome, Result | bytes | None]:
        for node in nodes:
            await node.start()
        try:
            client = ChaosClient(
                1,
                {i: node.address for i, node in enumerate(nodes, start=1)},
                WallClockHistory(),
                [b"k"],
                random.Random(0),
                reply_timeout=0.5,
                op_timeout=1.0,
            )
            return await client.do(op)
        finally:
            for node in nodes:
                await node.stop()

    return run(body)


PUT = Operation("put", b"k", b"1-1")


def test_ok() -> None:
    assert do(PUT, FakeNode([SimpleString("OK")])) == (Outcome.OK, Result(ok=True))


def test_follows_the_leader() -> None:
    # Whichever node the client tries first, it gets sent to the other at
    # least once before node 2 says yes
    one = FakeNode([RespError("NOTLEADER leader is node 2 at x:1")] * 5)
    two = FakeNode([RespError("NOTLEADER leader is node 1 at x:1"), SimpleString("OK")])

    assert do(PUT, one, two) == (Outcome.OK, Result(ok=True))
    assert one.got
    assert two.got[-1] == [b"SET", b"k", b"1-1"]


def test_a_write_that_timed_out_is_unknown_and_not_sent_again() -> None:
    node = FakeNode([RespError("TIMEOUT no answer in time"), SimpleString("OK")])

    assert do(PUT, node) == (Outcome.UNKNOWN, None)
    assert len(node.got) == 1


def test_a_write_cut_off_after_sending_is_unknown() -> None:
    node = FakeNode([CLOSE, SimpleString("OK")])

    assert do(PUT, node) == (Outcome.UNKNOWN, None)
    assert len(node.got) == 1


def test_a_read_that_timed_out_is_asked_again() -> None:
    node = FakeNode([RespError("TIMEOUT no answer in time"), b"1-1"])

    assert do(Operation("get", b"k"), node) == (Outcome.OK, b"1-1")


def test_nobody_takes_it_means_fail() -> None:
    node = FakeNode([RespError("TRYAGAIN no leader right now")] * 100)

    assert do(PUT, node) == (Outcome.FAIL, None)


def test_compare_and_set() -> None:
    cas = Operation("cas", b"k", b"1-2", b"1-1")
    setnx = Operation("cas", b"k", b"1-2", None)

    assert do(cas, FakeNode([0])) == (Outcome.OK, Result(ok=False))
    matched = FakeNode([1])
    assert do(cas, matched) == (Outcome.OK, Result(ok=True))
    assert matched.got == [[b"CAS", b"k", b"1-1", b"1-2"]]
    nx = FakeNode([1])
    assert do(setnx, nx) == (Outcome.OK, Result(ok=True))
    assert nx.got == [[b"SETNX", b"k", b"1-2"]]
