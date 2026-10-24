"""Run one whole simulation from one seed, and sum it up.

A run has two parts:

1. **Trouble** (`seconds` long): clients work while the nemesis breaks
   things (see nemesis.py).
2. **Calm** (`calm_seconds` long): the nemesis heals everything and stops.
   The cluster must start answering again. If no operation succeeds in the
   whole calm part, the run is stuck: the cluster lost liveness, which is a
   bug too, even though nothing unsafe happened.

Safety is checked all the way through (checks.py). A broken rule stops the
run on the spot with SafetyViolation.

At the end, the whole history goes through the linearizability checker
(see checker/). The safety checks look inside Raft. The checker looks only
at what clients saw, which is what a user of the database cares about.

Everything comes from the seed. The same seed gives the same run, event for
event, which the history digest at the end makes easy to see.

The seed also picks Raft's settings: a short or long election timeout, and
pre-vote and check quorum on or off. Short timeouts mean many elections,
some of them with two candidates in the same term. That is where bugs about
votes hide, and with the default settings they almost never come up.
"""

import random
from dataclasses import dataclass

from faultline.checker.check import Verdict, check
from faultline.raft.node import Config
from faultline.sim.bugs import switched_on
from faultline.sim.client import Outcome
from faultline.sim.nemesis import PROFILES, Nemesis
from faultline.sim.network import NetworkFaults
from faultline.sim.world import World


@dataclass(frozen=True)
class Summary:
    seed: int
    faults: str
    bug: str | None
    seconds: int
    ok: int
    failed: int
    unknown: int
    ok_while_calm: int
    highest_term: int
    crashes: int
    power_cuts: int
    messages: int
    dropped: int
    snapshots: int
    checks: int
    linearizable: Verdict
    history_digest: str

    @property
    def stuck(self) -> bool:
        return self.ok_while_calm == 0

    @property
    def problems(self) -> list[str]:
        problems = ["STUCK"] if self.stuck else []
        if self.linearizable == "bad":
            problems.append("NOT LINEARIZABLE")
        return problems

    def lines(self) -> list[str]:
        return [
            f"seed {self.seed}, {self.seconds} s of {self.faults} faults, then calm"
            + (f", with bug {self.bug} switched on" if self.bug else ""),
            f"operations: {self.ok} ok, {self.failed} failed, {self.unknown} unknown",
            f"ok while calm: {self.ok_while_calm}" + ("  <- STUCK" if self.stuck else ""),
            f"highest term: {self.highest_term}, crashes: {self.crashes}, "
            f"power cuts: {self.power_cuts}, snapshots taken: {self.snapshots}",
            f"messages: {self.messages} sent, {self.dropped} dropped",
            f"safety checks passed: {self.checks}",
            f"linearizable: {self.linearizable}"
            + ("  <- NOT LINEARIZABLE" if self.linearizable == "bad" else ""),
            f"history digest: {self.history_digest}",
        ]


def simulate(
    seed: int,
    seconds: int = 30,
    faults: str = "heavy",
    nodes: int = 5,
    clients: int = 3,
    keys: int = 3,
    calm_seconds: int = 5,
    bug: str | None = None,
) -> tuple[Summary, World, Nemesis]:
    """Run one simulation. Raises SafetyViolation if a Raft rule breaks.
    bug switches on one of the known bugs in bugs.py for this run."""
    lossy = faults != "none"
    network = NetworkFaults(
        min_latency=1,
        max_latency=10,
        drop_rate=0.01 if lossy else 0.0,
        duplicate_rate=0.01 if lossy else 0.0,
    )
    world = World(seed, nodes, clients, keys, config=_config_for(seed), faults=network)
    nemesis = Nemesis(world, PROFILES[faults])
    with switched_on(bug):
        world.start()
        nemesis.start()
        world.run_until(seconds * 1000)

        nemesis.calm_down()
        ok_before_calm = _count(world, Outcome.OK)
        world.run_until((seconds + calm_seconds) * 1000)

    summary = Summary(
        seed=seed,
        faults=faults,
        bug=bug,
        seconds=seconds,
        ok=_count(world, Outcome.OK),
        failed=_count(world, Outcome.FAIL),
        unknown=_count(world, Outcome.UNKNOWN),
        ok_while_calm=_count(world, Outcome.OK) - ok_before_calm,
        highest_term=max(n.kv.raft.term for n in world.nodes.values() if n.kv is not None),
        crashes=sum(n.crashes for n in world.nodes.values()),
        power_cuts=sum(n.power_cuts for n in world.nodes.values()),
        messages=world.network.sent,
        dropped=world.network.dropped,
        snapshots=sum(
            1 for n in world.nodes.values() if n.kv is not None and n.kv.raft.snapshot is not None
        ),
        checks=world.checker.checks,
        linearizable=check(world.history).verdict,
        history_digest=world.history.digest(),
    )
    return summary, world, nemesis


def _config_for(seed: int) -> Config:
    rng = random.Random(f"config/{seed}")
    election_ticks = rng.choice([3, 5, 10, 10])
    return Config(
        election_ticks=election_ticks,
        heartbeat_ticks=1 if election_ticks < 10 else 2,
        pre_vote=rng.random() < 0.7,
        check_quorum=rng.random() < 0.7,
    )


def _count(world: World, outcome: Outcome) -> int:
    return sum(client.counts[outcome] for client in world.clients.values())
