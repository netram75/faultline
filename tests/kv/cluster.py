"""A few KVNodes on an in-memory network, for tests.

Like tests/raft/cluster.py, but with the database on top. Messages can be
lost, repeated and reordered, the network can be split, and nodes can crash
and come back with their disks. Every running node's data is compared at
the end with check_same_data().
"""

import random

from faultline.fs.fake import FakeFileSystem
from faultline.kv.commands import Command
from faultline.kv.server import KVNode
from faultline.kv.state_machine import Result
from faultline.lsm import Options
from faultline.raft.messages import Message, NodeId
from faultline.raft.node import Config, Role

# Tiny sizes, so flushes and compactions happen all the time
SMALL = Options(memtable_size=1024, l0_limit=2, table_size=2048)


class KVCluster:
    def __init__(
        self,
        size: int,
        seed: int,
        config: Config | None = None,
        drop_rate: float = 0.0,
        repeat_rate: float = 0.0,
        snapshot_every: int = 1000,
    ) -> None:
        self.rng = random.Random(seed)
        self.snapshot_every = snapshot_every
        self.ids = list(range(1, size + 1))
        self.config = config or Config()
        self.drop_rate = drop_rate
        self.repeat_rate = repeat_rate
        self.disks = {i: FakeFileSystem(seed * 100 + i) for i in self.ids}
        self.nodes = {i: self._open(i) for i in self.ids}
        self.down: set[NodeId] = set()
        self.groups: list[set[NodeId]] = [set(self.ids)]
        self.in_flight: list[Message] = []
        self.results: list[tuple[Command, Result]] = []
        self.read_results: dict[int, bytes | None] = {}

    def tick(self, rounds: int = 1) -> None:
        for _ in range(rounds):
            for i in self.ids:
                if i not in self.down:
                    self.nodes[i].tick()
                    self._collect(i)
            self._deliver()

    def leader(self) -> NodeId | None:
        leaders = [
            i for i in self.ids if i not in self.down and self.nodes[i].raft.role is Role.LEADER
        ]
        if not leaders:
            return None
        return max(leaders, key=lambda i: self.nodes[i].raft.term)

    def wait_for_leader(self, max_rounds: int = 500) -> NodeId:
        for _ in range(max_rounds):
            leader = self.leader()
            # A leader that can serve reads: its no-op is committed
            if leader is not None and self.nodes[leader].raft.commit_index > 0:
                node = self.nodes[leader].raft
                if node.log.term_at(node.commit_index) == node.term:
                    return leader
            self.tick()
        raise AssertionError(f"no leader after {max_rounds} rounds")

    def submit(self, i: NodeId, command: Command) -> bool:
        ok = self.nodes[i].submit(command)
        self._collect(i)
        return ok

    def read(self, i: NodeId, key: bytes, ctx: int) -> bool:
        ok = self.nodes[i].read(key, ctx)
        self._collect(i)
        return ok

    def partition(self, *groups: set[NodeId]) -> None:
        rest = set(self.ids) - set().union(*groups)
        self.groups = [*groups, rest]

    def heal(self) -> None:
        self.groups = [set(self.ids)]

    def crash(self, i: NodeId) -> None:
        self.down.add(i)
        self.disks[i].crash(keep_names=self.rng.random() < 0.5)

    def restart(self, i: NodeId) -> None:
        self.down.discard(i)
        self.nodes[i] = self._open(i)
        self._collect(i)

    def check_same_data(self, keys: list[bytes]) -> None:
        """Every running node that applied as far as the leader has the same
        value for every key."""
        running = [self.nodes[i] for i in self.ids if i not in self.down]
        top = max(node.machine.applied_index for node in running)
        caught_up = [node for node in running if node.machine.applied_index == top]
        for key in keys:
            values = {node.id: node.machine.get(key) for node in caught_up}
            assert len(set(values.values())) == 1, f"nodes disagree on {key!r}: {values}"

    def _open(self, i: NodeId) -> KVNode:
        seed = self.rng.randrange(2**32)
        return KVNode.open(
            self.disks[i], f"/node{i}", i, self.ids, self.config, seed, SMALL, self.snapshot_every
        )

    def can_talk(self, a: NodeId, b: NodeId) -> bool:
        if a in self.down or b in self.down:
            return False
        return any(a in group and b in group for group in self.groups)

    def _collect(self, i: NodeId) -> None:
        node = self.nodes[i]
        self.in_flight += node.take_messages()
        self.results += node.take_results()
        self.read_results.update(node.take_read_results())

    def _deliver(self) -> None:
        messages, self.in_flight = self.in_flight, []
        self.rng.shuffle(messages)
        for message in messages:
            if not self.can_talk(message.sender, message.receiver):
                continue
            if self.rng.random() < self.drop_rate:
                continue
            copies = 2 if self.rng.random() < self.repeat_rate else 1
            for _ in range(copies):
                self.nodes[message.receiver].step(message)
            self._collect(message.receiver)
