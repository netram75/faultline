import asyncio
from pathlib import Path

from faultline.net.client import Client
from faultline.net.resp import RespError, SimpleString
from tests.net.helpers import LocalCluster, leader_client, run, wait_for


def test_commands(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path)
        await cluster.start()
        try:
            client = await leader_client(cluster)
            assert await client.call("PING") == SimpleString("PONG")
            assert await client.call("PING", "hi") == b"hi"
            assert await client.call("GET", "k") is None
            assert await client.call("SET", "k", "v1") == SimpleString("OK")
            assert await client.call("GET", "k") == b"v1"
            assert await client.call("CAS", "k", "nope", "v2") == 0
            assert await client.call("CAS", "k", "v1", "v2") == 1
            assert await client.call("GET", "k") == b"v2"
            assert await client.call("SETNX", "k", "v3") == 0
            assert await client.call("DEL", "k") == 1
            assert await client.call("SETNX", "k", "v3") == 1
            assert await client.call("GET", "k") == b"v3"
            await client.close()
        finally:
            await cluster.stop()

    run(body)


def test_a_bad_command_leaves_the_connection_open(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path, size=1)
        await cluster.start()
        try:
            client = await leader_client(cluster)
            assert await client.call("FLUSHALL") == RespError("ERR unknown command 'FLUSHALL'")
            assert await client.call("GET") == RespError(
                "ERR wrong number of arguments for 'get' command"
            )
            assert await client.call("PING") == SimpleString("PONG")
            await client.close()
        finally:
            await cluster.stop()

    run(body)


def test_pipelined_commands_are_answered_in_order(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path, size=1)
        await cluster.start()
        try:
            client = await leader_client(cluster)
            replies = await client.pipeline(
                [("SET", "k", "1"), ("GET", "k"), ("SET", "k", "2"), ("GET", "k"), ("PING",)]
            )
            assert replies == [
                SimpleString("OK"),
                b"1",
                SimpleString("OK"),
                b"2",
                SimpleString("PONG"),
            ]
            await client.close()
        finally:
            await cluster.stop()

    run(body)


def test_a_follower_points_to_the_leader(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path)
        await cluster.start()
        try:
            leader = await cluster.wait_for_leader()
            # Followers learn who leads from the first heartbeat
            await wait_for(
                lambda: all(r.kv.raft.leader == leader for r in cluster.runners.values()),
                what="every node to know the leader",
            )
            follower = next(i for i in cluster.runners if i != leader)
            client = await Client.connect(cluster.clients[follower])
            expected = RespError(f"NOTLEADER leader is node {leader} at {cluster.clients[leader]}")
            assert await client.call("SET", "k", "v") == expected
            assert await client.call("GET", "k") == expected
            await client.close()
        finally:
            await cluster.stop()

    run(body)


def test_no_leader_means_try_again(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path)
        # Only one of three: it can never win an election
        await cluster.start_node(1)
        try:
            client = await Client.connect(cluster.clients[1])
            assert await client.call("SET", "k", "v") == RespError("TRYAGAIN no leader right now")
            await client.close()
        finally:
            await cluster.stop()

    run(body)


def test_a_write_with_no_majority_times_out(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path, timeout=0.3)
        await cluster.start()
        try:
            client = await leader_client(cluster)
            leader = await cluster.wait_for_leader()
            for i in cluster.runners:
                if i != leader:
                    await cluster.stop_node(i)
            reply = await client.call("SET", "k", "v")
            assert isinstance(reply, RespError)
            assert reply.message.startswith("TIMEOUT")
            assert "may or may not" in reply.message
            await client.close()
        finally:
            await cluster.stop()

    run(body)


def test_byte_at_a_time(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path, size=1)
        await cluster.start()
        try:
            await leader_client(cluster)
            reader, writer = await asyncio.open_connection(
                cluster.clients[1].host, cluster.clients[1].port
            )
            for byte in b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n":
                writer.write(bytes([byte]))
                await writer.drain()
            assert await reader.readexactly(5) == b"+OK\r\n"
            writer.close()
        finally:
            await cluster.stop()

    run(body)


def test_garbage_gets_an_error_and_the_connection_closes(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path, size=1)
        await cluster.start()
        try:
            reader, writer = await asyncio.open_connection(
                cluster.clients[1].host, cluster.clients[1].port
            )
            writer.write(b"?what\r\n")
            reply = await reader.read(1000)
            assert reply.startswith(b"-ERR Protocol error")
            assert await reader.read(1000) == b""  # closed
            writer.close()
        finally:
            await cluster.stop()

    run(body)


def test_each_connection_is_its_own_session(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path, size=1)
        await cluster.start()
        try:
            one = await leader_client(cluster)
            two = await Client.connect(cluster.clients[1])
            # Both send their first write, so both use seq 1. With one shared
            # client id, the second would be taken for a retry of the first.
            assert await one.call("SET", "a", "1") == SimpleString("OK")
            assert await two.call("SET", "b", "2") == SimpleString("OK")
            assert await one.call("GET", "b") == b"2"
            await one.close()
            await two.close()
        finally:
            await cluster.stop()

    run(body)
