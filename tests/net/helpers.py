"""Helpers for tests that run real nodes on this machine."""

__all__ = ["LocalCluster", "free_ports", "leader_client", "read", "run", "wait_for"]

import asyncio
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

from faultline.net.client import Client
from faultline.net.local import free_ports
from faultline.net.node import Address, NodeRunner
from faultline.net.resp import RespError
from faultline.net.server import ClientServer
from faultline.raft.messages import NodeId
from faultline.raft.node import Role


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
    """A few nodes on 127.0.0.1, all in this event loop: for each one a
    NodeRunner and a ClientServer in front of it."""

    def __init__(
        self,
        tmp_path: Path,
        size: int = 3,
        addresses: dict[NodeId, Address] | None = None,
        timeout: float = 2.0,
    ) -> None:
        ids = list(range(1, size + 1))
        ports = free_ports(2 * size)
        if addresses is None:
            addresses = {i: Address("127.0.0.1", ports[i - 1]) for i in ids}
        self.addresses = addresses
        self.clients = {i: Address("127.0.0.1", ports[size + i - 1]) for i in ids}
        self.tmp_path = tmp_path
        self.timeout = timeout
        self.runners: dict[NodeId, NodeRunner] = {}
        self.servers: dict[NodeId, ClientServer] = {}
        self._started: set[NodeId] = set()
        for i in ids:
            self._make(i)

    def _make(self, i: NodeId) -> None:
        runner = NodeRunner(i, self.addresses, str(self.tmp_path / f"node{i}"))
        self.runners[i] = runner
        self.servers[i] = ClientServer(runner, self.clients, self.timeout)

    async def start(self) -> None:
        for i in self.runners:
            await self.start_node(i)

    async def start_node(self, i: NodeId) -> None:
        """Start node i. If it ran before, it starts again from its disk."""
        if self.runners[i].running:
            return
        if i in self._started:
            self._make(i)  # a fresh runner on the same disk, like a restarted process
        self._started.add(i)
        await self.runners[i].start()
        await self.servers[i].start()

    async def stop_node(self, i: NodeId) -> None:
        if self.runners[i].running:
            await self.servers[i].stop()
            await self.runners[i].stop()

    async def stop(self) -> None:
        for i in self.runners:
            await self.stop_node(i)

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


async def leader_client(cluster: LocalCluster) -> Client:
    """A client on the leader, once the leader can serve reads."""
    leader = await cluster.wait_for_leader()
    client = await Client.connect(cluster.clients[leader])
    await wait_for_reads(client)
    return client


async def wait_for_reads(client: Client) -> None:
    ready = False

    async def check() -> None:
        nonlocal ready
        ready = not isinstance(await client.call("GET", "warm-up"), RespError)

    while not ready:
        await check()
        if not ready:
            await asyncio.sleep(0.01)
