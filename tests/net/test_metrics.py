import asyncio
from pathlib import Path

from faultline.net.local import free_ports
from faultline.net.metrics import BUCKETS, Histogram, MetricsServer, prometheus_text
from faultline.net.node import Address
from faultline.net.resp import SimpleString
from tests.net.helpers import LocalCluster, leader_client, run


def test_histogram_buckets() -> None:
    histogram = Histogram()
    for seconds in (0.0005, 0.001, 0.003, 10.0):
        histogram.observe(seconds)

    assert histogram.counts[BUCKETS.index(0.001)] == 2  # the edge itself is in
    assert histogram.counts[BUCKETS.index(0.005)] == 1
    assert sum(histogram.counts) == 3  # 10 s is past the last edge
    assert histogram.total == 4


async def http_get(address: Address, path: str) -> bytes:
    reader, writer = await asyncio.open_connection(address.host, address.port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    reply = await reader.read()
    writer.close()
    return reply


def test_info_and_metrics(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path, size=1)
        await cluster.start()
        [port] = free_ports(1)
        address = Address("127.0.0.1", port)
        server = cluster.servers[1]
        scraper = MetricsServer(address, lambda: prometheus_text(server.metrics, server.runner))
        await scraper.start()
        try:
            client = await leader_client(cluster)
            assert await client.call("SET", "k", "v") == SimpleString("OK")
            await client.call("NOPE")

            info = await client.call("INFO")
            assert isinstance(info, bytes)
            lines = info.decode().split("\r\n")
            assert "role:leader" in lines
            assert "is_leader:1" in lines
            assert "requests_set:1" in lines
            assert "client_connections:1" in lines
            assert "errors_err:1" in lines

            reply = await http_get(address, "/metrics")
            head, _, text = reply.partition(b"\r\n\r\n")
            assert head.startswith(b"HTTP/1.1 200 OK")
            assert b"faultline_is_leader 1" in text
            assert b'faultline_requests_total{command="set"} 1' in text
            assert b'faultline_request_seconds_bucket{command="set",le="+Inf"} 1' in text
            assert b'faultline_request_seconds_count{command="set"} 1' in text

            assert (await http_get(address, "/")).startswith(b"HTTP/1.1 404")
            await client.close()
        finally:
            await scraper.stop()
            await cluster.stop()

    run(body)


def test_buckets_add_up(tmp_path: Path) -> None:
    async def body() -> None:
        cluster = LocalCluster(tmp_path, size=1)
        await cluster.start()
        try:
            client = await leader_client(cluster)
            for n in range(20):
                await client.call("SET", "k", str(n))
            text = prometheus_text(cluster.servers[1].metrics, cluster.runners[1])
            buckets = [
                int(line.rsplit(" ", 1)[1])
                for line in text.splitlines()
                if line.startswith('faultline_request_seconds_bucket{command="set"')
            ]
            # Each bucket counts everything up to its edge, so they never go
            # down, and the last one (+Inf) is all of them
            assert buckets == sorted(buckets)
            assert buckets[-1] == 20
            await client.close()
        finally:
            await cluster.stop()

    run(body)
