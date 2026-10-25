"""How fast is faultline? Start a real cluster on this machine and measure.

    python -m faultline.bench --seconds 10 --clients 8

Starts a cluster of real processes (net/local.py), finds the leader, and
runs `clients` connections against it, each doing one command at a time as
fast as answers come back:

    writes   SET with a 100 byte value
    reads    GET (a safe read: one heartbeat round with a majority)
    mixed    half and half

It prints how many answers came per second and how long they took. The
numbers are for this machine only. Every write is fsynced twice on each
node (the Raft log, then the storage engine), and on macOS that is
F_FULLFSYNC, which is slow on purpose: it waits until the data is really on
the drive.
"""

import argparse
import asyncio
import random
import sys
import tempfile
import time
from dataclasses import dataclass, field

from faultline.net.client import Client
from faultline.net.local import LocalProcesses
from faultline.net.resp import RespError

MODES = ("writes", "reads", "mixed")


@dataclass
class BenchResult:
    mode: str
    clients: int
    seconds: float
    latencies: list[float] = field(default_factory=list)  # of the answers that came
    errors: int = 0

    @property
    def per_second(self) -> float:
        return len(self.latencies) / self.seconds

    def percentile(self, p: float) -> float:
        ordered = sorted(self.latencies)
        return ordered[min(len(ordered) - 1, int(p / 100 * len(ordered)))] if ordered else 0.0

    def line(self) -> str:
        ms = [self.percentile(p) * 1000 for p in (50, 99)]
        return (
            f"{self.mode:7} {self.clients:3} clients  {self.per_second:8.0f} ops/s"
            f"  p50 {ms[0]:6.1f} ms  p99 {ms[1]:6.1f} ms  errors {self.errors}"
        )


async def bench(cluster: LocalProcesses, mode: str, clients: int, seconds: float) -> BenchResult:
    leader = await cluster.leader()
    result = BenchResult(mode, clients, seconds)
    value = b"x" * 100

    async def worker(seed: int) -> None:
        rng = random.Random(seed)
        client = await Client.connect(cluster.clients[leader])
        try:
            while time.monotonic() < until:
                key = b"key%d" % rng.randrange(100)
                read = mode == "reads" or (mode == "mixed" and rng.random() < 0.5)
                started = time.perf_counter()
                reply = await (client.call("GET", key) if read else client.call("SET", key, value))
                if isinstance(reply, RespError):
                    result.errors += 1
                else:
                    result.latencies.append(time.perf_counter() - started)
        finally:
            await client.close()

    until = time.monotonic() + seconds
    await asyncio.gather(*(worker(seed) for seed in range(clients)))
    return result


async def run(
    modes: list[str], clients: list[int], seconds: float, nodes: int
) -> list[BenchResult]:
    results = []
    with (
        tempfile.TemporaryDirectory(prefix="faultline-bench-") as directory,
        LocalProcesses(directory, nodes) as cluster,
    ):
        cluster.start_all()
        await _fill(cluster)
        for mode in modes:
            for count in clients:
                result = await bench(cluster, mode, count, seconds)
                print(result.line(), flush=True)
                results.append(result)
    return results


async def _fill(cluster: LocalProcesses) -> None:
    """Write the 100 keys once, so reads find something."""
    leader = await cluster.leader()

    async def write(first: int) -> None:
        client = await Client.connect(cluster.clients[leader])
        for n in range(first, 100, 10):
            await client.call("SET", b"key%d" % n, b"x" * 100)
        await client.close()

    await asyncio.gather(*(write(first) for first in range(10)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m faultline.bench", description=__doc__)
    parser.add_argument("--seconds", type=float, default=10, help="per measurement")
    parser.add_argument("--clients", default="1,8", help="connections, e.g. 1,8,32")
    parser.add_argument("--modes", default=",".join(MODES), help="any of: writes,reads,mixed")
    parser.add_argument("--nodes", type=int, default=3)
    args = parser.parse_args(argv)
    modes = [m for m in args.modes.split(",") if m]
    if not set(modes) <= set(MODES):
        parser.error(f"modes must be among {', '.join(MODES)}")
    clients = [int(c) for c in args.clients.split(",")]
    asyncio.run(run(modes, clients, args.seconds, args.nodes))
    return 0


if __name__ == "__main__":
    sys.exit(main())
