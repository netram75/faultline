"""Break a real cluster and check what the clients saw.

    python -m faultline.chaos --seconds 30

1. Start a cluster of real faultline processes on this machine
   (net/local.py), each with its own data folder.
2. Run a few clients against it over the Redis protocol. They write down
   every operation and how it ended (clients.py).
3. Meanwhile, a nemesis hurts one node at a time:

       kill     kill -9, then start it again a moment later
       pause    SIGSTOP, then SIGCONT: a frozen process, timers and all
       leader   the same, but always picks the node that leads right now

   One at a time, so a majority is always there: the cluster should keep
   working, more slowly. With two nodes down, nothing can commit, and the
   run would only show that.
4. At the end, the linearizability checker (checker/) reads the history.

Unlike the simulator this is not repeatable: real processes, real time,
real scheduling. The seed only picks the operations and the faults. What it
adds is the real thing: real sockets, real fsync, real kill -9.
"""

import asyncio
import contextlib
import random
import tempfile
import time
from dataclasses import dataclass, field

from faultline.chaos.clients import ChaosClient, WallClockHistory
from faultline.checker import Report, check
from faultline.net.local import LocalProcesses
from faultline.raft.messages import NodeId
from faultline.sim.history import History

FAULTS = ("kill", "pause", "leader")


@dataclass
class ChaosResult:
    history: History
    report: Report
    faults: list[str] = field(default_factory=list)  # what the nemesis did, and when

    @property
    def counts(self) -> dict[str, int]:
        ends = [event.type for event in self.history.events if event.type != "invoke"]
        return {kind: ends.count(kind) for kind in ("ok", "fail", "info")}


async def run_chaos(
    seconds: float,
    nodes: int = 3,
    clients: int = 3,
    keys: int = 3,
    seed: int = 1,
    directory: str | None = None,
    faults: tuple[str, ...] = FAULTS,
) -> ChaosResult:
    rng = random.Random(seed)
    with tempfile.TemporaryDirectory(prefix="faultline-chaos-") as temp:
        cluster = LocalProcesses(directory or temp, nodes, timeout=1.0)
        try:
            cluster.start_all()
            await cluster.leader()
            history = WallClockHistory()
            names = [b"k%d" % n for n in range(keys)]
            until = time.monotonic() + seconds
            workers = [
                ChaosClient(i, cluster.clients, history, names, random.Random(rng.random()))
                for i in range(1, clients + 1)
            ]
            done: list[str] = []
            nemesis = asyncio.create_task(_nemesis(cluster, rng, until, faults, done))
            await asyncio.gather(*(worker.run(until) for worker in workers))
            await nemesis
        finally:
            cluster.close()
    return ChaosResult(history, check(history), done)


async def _nemesis(
    cluster: LocalProcesses,
    rng: random.Random,
    until: float,
    faults: tuple[str, ...],
    log: list[str],
) -> None:
    start = time.monotonic()
    await asyncio.sleep(rng.uniform(0.3, 1.0))
    while faults and time.monotonic() < until - 1:
        fault = rng.choice(faults)
        node: NodeId = rng.choice(cluster.ids)
        if fault == "leader":
            with contextlib.suppress(TimeoutError):
                node = await cluster.leader(timeout=3)
        hurt = rng.uniform(0.3, 1.5)
        at = time.monotonic() - start
        which = f"node {node}" + (" (the leader)" if fault == "leader" else "")
        if fault == "pause" or (fault == "leader" and rng.random() < 0.5):
            log.append(f"{at:6.2f}s pause {which} for {hurt:.1f}s")
            cluster.pause(node)
            await asyncio.sleep(hurt)
            cluster.resume(node)
        else:
            log.append(f"{at:6.2f}s kill -9 {which}, back after {hurt:.1f}s")
            cluster.kill(node)
            await asyncio.sleep(hurt)
            cluster.start(node)
        # Let it rejoin before the next fault, so two are never down at once
        await asyncio.sleep(rng.uniform(0.5, 1.5))
