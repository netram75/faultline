import asyncio
from pathlib import Path

import pytest

from faultline.kv.commands import Command, Put
from faultline.kv.state_machine import Result
from faultline.net.node import Address, NodeRunner
from tests.net.helpers import LocalCluster, free_ports, read, run, wait_for


def test_address() -> None:
    assert Address.parse("127.0.0.1:7001") == Address("127.0.0.1", 7001)
    assert str(Address("localhost", 80)) == "localhost:80"
    with pytest.raises(ValueError, match="host:port"):
        Address.parse("7001")


def test_a_cluster_of_one_answers_at_once(tmp_path: Path) -> None:
    async def body() -> None:
        [port] = free_ports(1)
        runner = NodeRunner(1, {1: Address("127.0.0.1", port)}, str(tmp_path / "node1"))
        await runner.start()
        try:
            await wait_for(lambda: runner.kv.raft.role.value == "leader", what="a leader")
            future = runner.submit(Command(1, 1, Put(b"k", b"v")))
            assert future is not None
            assert await asyncio.wait_for(future, 5) == Result(ok=True)
            assert await read(runner, b"k") == b"v"
        finally:
            await runner.stop()

    run(body)


def test_three_nodes_over_tcp(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path)
        await cluster.start()
        try:
            leader = await cluster.wait_for_leader()
            runner = cluster.runners[leader]
            future = runner.submit(Command(1, 1, Put(b"k", b"v")))
            assert future is not None
            assert await asyncio.wait_for(future, 5) == Result(ok=True)

            # The others get it too, over the network
            await wait_for(
                lambda: all(r.kv.machine.get(b"k") == b"v" for r in cluster.runners.values()),
                what="every node to apply the write",
            )
            # Only the leader takes writes
            follower = next(r for i, r in cluster.runners.items() if i != leader)
            assert follower.submit(Command(1, 2, Put(b"k", b"w"))) is None
            assert follower.read(b"k") is None
        finally:
            await cluster.stop()

    run(body)


def test_a_new_leader_when_the_old_one_stops(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path)
        await cluster.start()
        try:
            old = await cluster.wait_for_leader()
            await cluster.runners[old].stop()

            new = await cluster.wait_for_leader()
            assert new != old
            future = cluster.runners[new].submit(Command(1, 1, Put(b"k", b"v")))
            assert future is not None
            assert await asyncio.wait_for(future, 5) == Result(ok=True)

            # The old one comes back on the same disk and catches up
            cluster.runners[old] = cluster.new_runner(old, tmp_path)
            await cluster.runners[old].start()
            await wait_for(
                lambda: cluster.runners[old].kv.machine.get(b"k") == b"v",
                what="the old leader to catch up",
            )
        finally:
            await cluster.stop()

    run(body)


def test_data_survives_a_full_restart(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path)
        await cluster.start()
        try:
            leader = await cluster.wait_for_leader()
            for seq in range(1, 6):
                future = cluster.runners[leader].submit(Command(1, seq, Put(b"k", b"%d" % seq)))
                assert future is not None
                await asyncio.wait_for(future, 5)
        finally:
            await cluster.stop()

        again = LocalCluster(tmp_path, addresses=cluster.addresses)
        await again.start()
        try:
            leader = await again.wait_for_leader()
            assert await read(again.runners[leader], b"k") == b"5"
        finally:
            await again.stop()

    run(body)


def test_a_write_to_a_lost_leader_never_answers(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path)
        await cluster.start()
        try:
            leader = await cluster.wait_for_leader()
            # Cut the leader off: stop the other two
            for i, runner in cluster.runners.items():
                if i != leader:
                    await runner.stop()
            future = cluster.runners[leader].submit(Command(1, 1, Put(b"k", b"v")))
            assert future is not None
            # No majority, so no commit and no answer. The caller's timeout
            # is what ends the wait.
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(future, 0.5)
        finally:
            await cluster.stop()

    run(body)
