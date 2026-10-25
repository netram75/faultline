"""Helpers for tests that run real nodes on this machine."""

import asyncio
import socket
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

from faultline.net.node import Address, NodeRunner
from faultline.raft.messages import NodeId
from faultline.raft.node import Role


def free_ports(count: int) -> list[int]:
    """Ports nobody is using right now. Hold them all open at once, so we
    don't get the same one twice."""
    sockets = [socket.socket() for _ in range(count)]
    try:
        for s in sockets:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


def run[T](test: Callable[[], Awaitable[T]], timeout: float = 30) -> T:
    """Run an async test with a time limit, so a bug can't hang the suite."""

    async def limited() -> T:
        return await asyncio.wait_for(test(), timeout)

    return asyncio.run(limited())


async def wait_for(condition: Callable[[], bool], timeout: float = 10, what: str = "") -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what or condition}")
        await asyncio.sleep(0.01)


class LocalCluster:
    """A few NodeRunners on 127.0.0.1, all in this event loop."""

    def __init__(
        self, tmp_path: Path, size: int = 3, addresses: dict[NodeId, Address] | None = None
    ) -> None:
        ids = list(range(1, size + 1))
        if addresses is None:
            ports = free_ports(size)
            addresses = {i: Address("127.0.0.1", port) for i, port in zip(ids, ports, strict=True)}
        self.addresses = addresses
        self.runners = {i: self.new_runner(i, tmp_path) for i in ids}

    def new_runner(self, i: NodeId, tmp_path: Path) -> NodeRunner:
        return NodeRunner(i, self.addresses, str(tmp_path / f"node{i}"))

    async def start(self) -> None:
        for runner in self.runners.values():
            await runner.start()

    async def stop(self) -> None:
        for runner in self.runners.values():
            if runner.running:
                await runner.stop()

    def leader(self) -> NodeId | None:
        leaders = [
            i
            for i, runner in self.runners.items()
            if runner.running and runner.kv.raft.role is Role.LEADER
        ]
        return leaders[0] if len(leaders) == 1 else None

    async def wait_for_leader(self, timeout: float = 10) -> NodeId:
        await wait_for(lambda: self.leader() is not None, timeout, "a leader")
        leader = self.leader()
        assert leader is not None
        return leader


async def read(runner: NodeRunner, key: bytes, timeout: float = 10) -> bytes | None:
    """A safe read, once the node can serve one. A brand new leader can't
    until it has committed an entry of its own term."""
    future = None

    def started() -> bool:
        nonlocal future
        future = runner.read(key)
        return future is not None

    await wait_for(started, timeout, "the node to take reads")
    assert future is not None
    return await asyncio.wait_for(future, timeout)
