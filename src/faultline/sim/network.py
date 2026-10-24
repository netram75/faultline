"""The simulated network between the nodes.

Real networks are not kind, and Raft has to cope with all of this:

* **Latency.** Every message takes a random time to arrive, between
  min_latency and max_latency milliseconds. Two messages sent one after the
  other can arrive the other way round, so reordering comes for free.
* **Loss.** A message is dropped with probability drop_rate.
* **Duplicates.** A message arrives twice with probability duplicate_rate,
  each copy with its own delay.
* **Partitions.** The nodes are split into groups, and only nodes in the
  same group can talk. A node can be in two groups at once, a bridge: it
  talks to both sides, but the sides can't talk to each other. A node in no
  group is cut off from everyone. A message already on the wire when the
  split happens is lost too.

Every message is encoded to bytes when it is sent and decoded when it
arrives, exactly like over a real socket. So the wire format gets tested on
every message of every run.
"""

import random
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial

from faultline.raft.messages import Message, NodeId, decode, encode
from faultline.sim.loop import EventLoop


@dataclass
class NetworkFaults:
    min_latency: int = 1  # milliseconds
    max_latency: int = 10
    drop_rate: float = 0.0
    duplicate_rate: float = 0.0


class Network:
    def __init__(
        self,
        loop: EventLoop,
        deliver: Callable[[Message], None],
        faults: NetworkFaults | None = None,
    ) -> None:
        self.faults = faults or NetworkFaults()
        self.sent = 0
        self.delivered = 0
        self.dropped = 0
        self.duplicated = 0
        self._loop = loop
        self._rng = random.Random(loop.seed_for("network"))
        self._deliver = deliver
        self._groups: list[frozenset[NodeId]] | None = None  # None: no split

    def send(self, message: Message) -> None:
        self.sent += 1
        data = encode(message)
        if not self.can_talk(message.sender, message.receiver):
            self.dropped += 1
            return
        if self._rng.random() < self.faults.drop_rate:
            self.dropped += 1
            return
        copies = 1
        if self._rng.random() < self.faults.duplicate_rate:
            copies = 2
            self.duplicated += 1
        for _ in range(copies):
            delay = self._rng.randint(self.faults.min_latency, self.faults.max_latency)
            self._loop.after(delay, partial(self._arrive, data))

    def partition(self, *groups: set[NodeId]) -> None:
        """Split the network. Only nodes in the same group can talk."""
        self._groups = [frozenset(group) for group in groups]

    def heal(self) -> None:
        self._groups = None

    @property
    def groups(self) -> list[frozenset[NodeId]] | None:
        return self._groups

    def can_talk(self, a: NodeId, b: NodeId) -> bool:
        if self._groups is None:
            return True
        return any(a in group and b in group for group in self._groups)

    def _arrive(self, data: bytes) -> None:
        message = decode(data)
        if not self.can_talk(message.sender, message.receiver):
            self.dropped += 1  # the network split while it was on its way
            return
        self.delivered += 1
        self._deliver(message)
