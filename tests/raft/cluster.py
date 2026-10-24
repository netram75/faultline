"""A small in-memory network for testing whole Raft clusters.

This is not the fault simulator (that comes later). It is just enough to
run a few nodes from one seed and be mean to them: lose, delay, repeat and
reorder messages, cut the network into parts, and crash and restart nodes
with their disks.

While it runs it checks three of Raft's safety rules (Raft paper, figure 3):

* Election safety: never more than one leader in a term.
* State machine safety: if two nodes apply an entry at the same index, it is
  the same entry. Nodes apply as soon as they can, and every entry applied
  anywhere is remembered and compared.
* Leader completeness: a new leader has every entry that was committed
  before it, at the same index.

check_logs() checks the fourth, log matching, which is too slow to run after
every message.
"""

import random

from faultline.fs.fake import FakeFileSystem
from faultline.raft.messages import Entry, Message, NodeId
from faultline.raft.node import Config, Node, Role


class Cluster:
    def __init__(
        self,
        size: int,
        seed: int,
        config: Config | None = None,
        drop_rate: float = 0.0,
        delay_rate: float = 0.0,
        repeat_rate: float = 0.0,
        reboot_rate: float = 0.0,
    ) -> None:
        self.rng = random.Random(seed)
        self.ids = list(range(1, size + 1))
        self.config = config or Config()
        self.drop_rate = drop_rate
        self.delay_rate = delay_rate
        self.repeat_rate = repeat_rate
        # Chance that a node crashes and comes straight back right after it
        # handles a message. Its replies are already on their way, so this is
        # where a node that forgets what it promised gets caught.
        self.reboot_rate = reboot_rate
        self.disks = {i: FakeFileSystem(seed * 100 + i) for i in self.ids}
        self.nodes = {i: self._open(i) for i in self.ids}
        self.down: set[NodeId] = set()
        self.groups: list[set[NodeId]] = [set(self.ids)]
        self.in_flight: list[Message] = []
        self.leaders_by_term: dict[int, set[NodeId]] = {}
        self.committed: dict[int, Entry] = {}  # every entry any node applied

    def tick(self, rounds: int = 1) -> None:
        """Let time pass: every running node ticks, then messages move."""
        for _ in range(rounds):
            for i in self.ids:
                if i not in self.down:
                    self.nodes[i].tick()
                    self._collect(i)
            self._deliver()

    def deliver(self) -> None:
        """Move the messages in flight once, without time passing."""
        self._deliver()

    def elect(self, i: NodeId, max_rounds: int = 200) -> None:
        """Make node `i` win an election. Only it ticks, so only it times
        out, and messages move until it is leader."""
        for _ in range(max_rounds):
            if self.nodes[i].role is Role.LEADER:
                return
            self.nodes[i].tick()
            self._collect(i)
            self._deliver()
        raise AssertionError(f"node {i} did not win an election")

    def partition(self, *groups: set[NodeId]) -> None:
        """Only nodes in the same group can talk. Nodes left out of every
        group form one more group, so partition({3}) cuts node 3 off from
        the rest."""
        rest = set(self.ids) - set().union(*groups)
        self.groups = [*groups, rest]

    def heal(self) -> None:
        self.groups = [set(self.ids)]

    def crash(self, i: NodeId, keep_names: bool = False) -> None:
        self.down.add(i)
        self.disks[i].crash(keep_names)

    def restart(self, i: NodeId) -> None:
        self.down.discard(i)
        self.nodes[i] = self._open(i)

    def propose(self, command: bytes) -> int | None:
        """Give a command to the current leader, if there is one."""
        leader = self.leader()
        if leader is None:
            return None
        index = self.nodes[leader].propose(command)
        self._collect(leader)
        return index

    def check_logs(self) -> None:
        """Log matching (Raft paper, section 5.3): if two logs have an entry
        with the same index and term, they are the same up to that index."""
        logs = [self.nodes[i].log for i in self.ids if i not in self.down]
        for a in logs:
            for b in logs:
                for index in range(min(a.last_index, b.last_index), 0, -1):
                    if a.term_at(index) == b.term_at(index):
                        assert a.entries_from(1, index) == b.entries_from(1, index)
                        break

    def leader(self) -> NodeId | None:
        """The running node that is leader in the newest term, if any."""
        leaders = [i for i in self.ids if i not in self.down and self.nodes[i].role is Role.LEADER]
        if not leaders:
            return None
        return max(leaders, key=lambda i: self.nodes[i].term)

    def wait_for_leader(self, max_rounds: int = 500) -> NodeId:
        for _ in range(max_rounds):
            leader = self.leader()
            if leader is not None and self._everyone_follows(leader):
                return leader
            self.tick()
        raise AssertionError(f"no leader after {max_rounds} rounds")

    def can_talk(self, a: NodeId, b: NodeId) -> bool:
        if a in self.down or b in self.down:
            return False
        return any(a in group and b in group for group in self.groups)

    def _open(self, i: NodeId) -> Node:
        # A new seed for every start, like a real restart picks new timeouts
        seed = self.rng.randrange(2**32)
        return Node.open(self.disks[i], f"/node{i}", i, self.ids, self.config, seed)

    def _everyone_follows(self, leader: NodeId) -> bool:
        term = self.nodes[leader].term
        reachable = [i for i in self.ids if self.can_talk(i, leader) or i == leader]
        return all(self.nodes[i].leader == leader and self.nodes[i].term == term for i in reachable)

    def _collect(self, i: NodeId) -> None:
        self.in_flight += self.nodes[i].take_messages()
        self._check_election_safety()
        for index, entry in self.nodes[i].take_committed():
            first = self.committed.setdefault(index, entry)
            assert first == entry, f"node {i} applied {entry} at {index}, another applied {first}"

    def _deliver(self) -> None:
        messages, self.in_flight = self.in_flight, []
        self.rng.shuffle(messages)
        for message in messages:
            if self.rng.random() < self.delay_rate:
                self.in_flight.append(message)  # arrives in a later round
                continue
            if not self.can_talk(message.sender, message.receiver):
                continue
            if self.rng.random() < self.drop_rate:
                continue
            copies = 2 if self.rng.random() < self.repeat_rate else 1
            for _ in range(copies):
                self.nodes[message.receiver].step(message)
            self._collect(message.receiver)
            if self.rng.random() < self.reboot_rate:
                self.crash(message.receiver, keep_names=self.rng.random() < 0.5)
                self.restart(message.receiver)

    def _check_election_safety(self) -> None:
        for i in self.ids:
            node = self.nodes[i]
            if i not in self.down and node.role is Role.LEADER:
                leaders = self.leaders_by_term.setdefault(node.term, set())
                if i in leaders:
                    continue
                leaders.add(i)
                assert len(leaders) == 1, f"two leaders in term {node.term}: {sorted(leaders)}"
                # A new leader. It must have everything committed so far.
                for index, entry in self.committed.items():
                    assert index <= node.log.last_index and node.log.entry(index) == entry, (
                        f"leader {i} of term {node.term} is missing committed entry {index}"
                    )
