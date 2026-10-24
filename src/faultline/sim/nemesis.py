"""The nemesis: the part of the simulation whose only job is to cause trouble.

The name comes from Jepsen, the tool that found bugs in most well-known
distributed databases. Every so often (mean_gap milliseconds on average) the
nemesis picks one fault, weighted by the profile, and does it:

* **partition:** split the network. Either one node is cut off from the
  rest, or the nodes split into a minority and a majority, or two sides with
  one node in both (a bridge, which confuses naive leader election). It heals
  after a while.
* **crash:** a running node loses power between two steps, and comes back
  after a while.
* **power cut:** a running node loses power in the middle of a disk call,
  somewhere in its next 50 (halfway through a flush, a compaction, a
  snapshot). It comes back after a while.
* **loss burst:** for a while, half of all messages are lost.
* **clock skew:** one node's clock starts running up to 30% fast or slow.

At most max_down nodes are down at once. With a majority always able to
come back, the cluster can always make progress again, so a run that stops
making progress is a bug too (though a run is still checked for safety
either way).

Everything the nemesis does goes into its log, with the time, so a failed
run can be read like a story.
"""

import random
from dataclasses import dataclass
from functools import partial

from faultline.raft.messages import NodeId
from faultline.sim.world import World


@dataclass(frozen=True)
class Faults:
    mean_gap: int  # ms between two faults, on average. 0 means no faults at all.
    partition: float = 1.0  # how often each kind is picked, relative to the others
    crash: float = 1.0
    power_cut: float = 1.0
    loss_burst: float = 1.0
    clock_skew: float = 1.0
    max_down: int = 2
    shortest: int = 200  # how long a partition, burst or outage lasts, in ms
    longest: int = 3000


PROFILES = {
    "none": Faults(mean_gap=0),
    "light": Faults(mean_gap=3000, power_cut=0.5, clock_skew=0.5),
    "heavy": Faults(mean_gap=500),
}


class Nemesis:
    def __init__(self, world: World, faults: Faults) -> None:
        self.faults = faults
        self.log: list[tuple[int, str]] = []
        self._world = world
        self._loop = world.loop
        self._rng = random.Random(self._loop.seed_for("nemesis"))
        self._normal_drop_rate = world.network.faults.drop_rate
        # Bumped by every new partition or burst, so an older one's "heal
        # now" timer doesn't end a newer one early
        self._partition_generation = 0
        self._burst_generation = 0

    def start(self) -> None:
        if self.faults.mean_gap > 0:
            self._loop.after(self._gap(), self._strike)

    def calm_down(self) -> None:
        """Undo everything: heal the network, start every node, fix the
        clocks. Faults that were scheduled still happen; stop calling start()
        to stop new ones. Used at the end of a run so clients can finish."""
        self.faults = Faults(mean_gap=0)
        self._partition_generation += 1
        self._burst_generation += 1
        self._world.network.heal()
        self._world.network.faults.drop_rate = self._normal_drop_rate
        for node in self._world.nodes.values():
            node.skew = 1.0
            node.disk.cancel_power_cut()
            node.start()
        self._note("calm: network healed, every node started, clocks fixed")

    def _strike(self) -> None:
        if self.faults.mean_gap == 0:
            return
        kinds = ["partition", "crash", "power_cut", "loss_burst", "clock_skew"]
        weights = [getattr(self.faults, kind) for kind in kinds]
        kind = self._rng.choices(kinds, weights)[0]
        getattr(self, f"_{kind}")()
        self._loop.after(self._gap(), self._strike)

    def _partition(self) -> None:
        ids = sorted(self._world.nodes)
        self._rng.shuffle(ids)
        shape = self._rng.choice(["isolate", "split", "bridge"])
        if shape == "isolate":
            groups = [{ids[0]}, set(ids[1:])]
        elif shape == "split":
            cut = len(ids) // 2
            groups = [set(ids[:cut]), set(ids[cut:])]
        else:
            middle = len(ids) // 2
            groups = [set(ids[: middle + 1]), set(ids[middle:])]
        self._world.network.partition(*groups)
        self._partition_generation += 1
        self._note(f"partition ({shape}): {' | '.join(str(sorted(g)) for g in groups)}")
        self._loop.after(self._duration(), partial(self._heal, self._partition_generation))

    def _heal(self, generation: int) -> None:
        if generation == self._partition_generation:
            self._world.network.heal()
            self._note("network healed")

    def _crash(self) -> None:
        node = self._pick_up_node()
        if node is None:
            return
        keep_names = self._rng.random() < 0.5
        self._world.nodes[node].crash(keep_names)
        self._note(f"crash node {node}" + (" (folder changes kept)" if keep_names else ""))
        self._loop.after(self._duration(), partial(self._restart, node))

    def _power_cut(self) -> None:
        node = self._pick_up_node()
        if node is None:
            return
        steps = self._rng.randrange(50)
        self._world.nodes[node].cut_power(steps, keep_names=self._rng.random() < 0.5)
        self._note(f"power cut on node {node} within {steps} disk calls")
        self._loop.after(self._duration(), partial(self._restart, node))

    def _restart(self, node: NodeId) -> None:
        sim_node = self._world.nodes[node]
        if not sim_node.up:
            sim_node.start()
            self._note(f"restart node {node}")

    def _loss_burst(self) -> None:
        self._world.network.faults.drop_rate = 0.5
        self._burst_generation += 1
        self._note("loss burst: half of all messages lost")
        self._loop.after(self._duration(), partial(self._end_burst, self._burst_generation))

    def _end_burst(self, generation: int) -> None:
        if generation == self._burst_generation:
            self._world.network.faults.drop_rate = self._normal_drop_rate
            self._note("loss burst over")

    def _clock_skew(self) -> None:
        node = self._rng.choice(sorted(self._world.nodes))
        skew = round(self._rng.uniform(0.7, 1.3), 2)
        self._world.nodes[node].skew = skew
        self._note(f"node {node} clock now runs at {skew}x the normal tick time")

    def _pick_up_node(self) -> NodeId | None:
        nodes = self._world.nodes
        down = sum(1 for node in nodes.values() if not node.up)
        if down >= self.faults.max_down:
            return None
        return self._rng.choice([i for i in sorted(nodes) if nodes[i].up])

    def _gap(self) -> int:
        return max(1, round(self._rng.expovariate(1 / self.faults.mean_gap)))

    def _duration(self) -> int:
        return self._rng.randint(self.faults.shortest, self.faults.longest)

    def _note(self, what: str) -> None:
        self.log.append((self._loop.now, what))
