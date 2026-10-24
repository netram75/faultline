"""Safety checks that run all the time while the simulation runs.

After every step a node takes (a tick, a message, a request), the checker
looks inside it and checks Raft's safety rules (Raft paper, figure 3):

* **Election safety:** at most one leader per term, ever.
* **State machine safety:** once an entry is committed at some index, every
  node that commits that index has exactly that entry there. Nodes apply
  only committed entries, so this means no two nodes ever apply different
  commands at the same index.
* **Leader completeness:** a node that becomes leader already has every
  entry committed before it. If it didn't, a committed write would be lost.

Checking inside the run, not only at the end, means a violation is caught
the moment it happens, with the simulated time. A run is one seed, so the
same seed replays it exactly, up to that moment, as many times as it takes
to understand it.

Entries inside a node's snapshot can't be looked at any more. They were
checked when they were committed, so they are skipped. Everything else a node
commits after a restart is checked again from the start: recovery is exactly
where a committed entry could quietly change.
"""

from faultline.kv.server import KVNode
from faultline.raft.messages import Entry, NodeId
from faultline.raft.node import Role
from faultline.sim.loop import EventLoop
from faultline.sim.node import SimNode


class SafetyViolation(Exception):
    """A Raft safety rule was broken. The message says when, and the seed."""


class SafetyChecker:
    def __init__(self, loop: EventLoop, nodes: dict[NodeId, SimNode]) -> None:
        self.checks = 0
        self._loop = loop
        self._nodes = nodes
        self._leaders: dict[int, NodeId] = {}  # term -> its leader, once checked
        self._committed: dict[int, Entry] = {}  # index -> the entry committed there
        # How far each node's commits were checked, for the KVNode it runs now
        self._checked: dict[NodeId, tuple[KVNode, int]] = {}

    def check(self, node_id: NodeId) -> None:
        node = self._nodes[node_id].kv
        if node is None:
            return
        self.checks += 1
        raft = node.raft
        if raft.role is Role.LEADER:
            self._check_leader(node_id)
        # Every newly committed entry on this node must match what was
        # committed at that index anywhere before
        checked_kv, checked = self._checked.get(node_id, (node, 0))
        if checked_kv is not node:
            checked = 0  # restarted: check its commits again
        start = max(raft.log.base_index, checked) + 1
        for index in range(start, raft.commit_index + 1):
            entry = raft.log.entry(index)
            first = self._committed.setdefault(index, entry)
            if entry != first:
                self._fail(
                    f"node {node_id} committed {entry} at index {index}, another node {first}"
                )
        self._checked[node_id] = (node, max(checked, raft.commit_index))

    def _check_leader(self, node_id: NodeId) -> None:
        node = self._nodes[node_id].kv
        assert node is not None
        raft = node.raft
        first = self._leaders.get(raft.term)
        if first == node_id:
            return  # checked when it became leader
        if first is not None:
            self._fail(f"two leaders in term {raft.term}: nodes {first} and {node_id}")
        self._leaders[raft.term] = node_id
        # A new leader: it must have every entry committed so far
        for index, entry in self._committed.items():
            if index <= raft.log.base_index:
                continue
            if index > raft.log.last_index or raft.log.entry(index) != entry:
                self._fail(f"new leader {node_id} (term {raft.term}) lacks committed entry {index}")

    def _fail(self, what: str) -> None:
        raise SafetyViolation(f"seed {self._loop.seed}, at {self._loop.now} ms: {what}")
