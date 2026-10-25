"""A real cluster: three faultline processes on this machine, talked to over
the Redis protocol, killed with kill -9 and frozen with SIGSTOP."""

import asyncio
from pathlib import Path

from faultline.net.client import Client
from faultline.net.local import LocalProcesses
from faultline.net.resp import RespError, SimpleString, Value
from tests.net.helpers import run


async def call(cluster: LocalProcesses, *args: str) -> Value:
    """Send a command to whichever node leads right now."""
    leader = await cluster.leader()
    client = await Client.connect(cluster.clients[leader])
    try:
        return await client.call(*args)
    finally:
        await client.close()


def test_writes_survive_kill_9_and_restarts(tmp_path: Path) -> None:
    with LocalProcesses(str(tmp_path)) as cluster:
        cluster.start_all()

        async def body() -> None:
            for n in range(10):
                assert await call(cluster, "SET", f"k{n}", f"v{n}") == SimpleString("OK")

            # kill -9 the leader. The other two elect a new one and still have
            # every write that was answered.
            old = await cluster.leader()
            cluster.kill(old)
            new = await cluster.leader()
            assert new != old
            for n in range(10):
                assert await call(cluster, "GET", f"k{n}") == f"v{n}".encode()
            assert await call(cluster, "SET", "after", "kill") == SimpleString("OK")

            # Bring it back, then stop everything politely and start it all
            # again: the data is on disk
            cluster.start(old)
            await asyncio.sleep(0.5)
            for i in cluster.ids:
                assert cluster.stop(i) == 0
            cluster.start_all()
            assert await call(cluster, "GET", "after") == b"kill"
            assert await call(cluster, "GET", "k9") == b"v9"

        run(body, timeout=60)


def test_a_frozen_leader_never_gives_a_stale_read(tmp_path: Path) -> None:
    with LocalProcesses(str(tmp_path)) as cluster:
        cluster.start_all()

        async def body() -> None:
            assert await call(cluster, "SET", "k", "old") == SimpleString("OK")
            old = await cluster.leader()
            stale = await Client.connect(cluster.clients[old])

            # Freeze the leader. The others stop hearing from it, elect a new
            # one, and the key changes there.
            cluster.pause(old)
            new = await cluster.leader()
            assert new != old
            assert await call(cluster, "SET", "k", "new") == SimpleString("OK")

            # Ask the old leader before it wakes up. When it does, it still
            # thinks it leads, and its own copy of k is "old". Without ReadIndex
            # that is what it would answer.
            pending = asyncio.create_task(stale.call("GET", "k"))
            await asyncio.sleep(0.1)
            cluster.resume(old)
            reply = await pending
            assert reply != b"old"
            assert reply == b"new" or isinstance(reply, RespError)
            # In practice it answers NOTLEADER: when it wakes, the new
            # leader's messages are already waiting in its socket, it reads
            # those first, sees the newer term and steps down. So this test
            # can't force the dangerous moment. The simulator can, and its
            # leader-read bug shows the checker catching it (BUGS.md).
            await stale.close()

        run(body, timeout=60)
