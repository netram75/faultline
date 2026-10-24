"""A node whose log was compacted after a snapshot (Raft paper, section 7)."""

import pytest

from faultline.fs.fake import FakeFileSystem
from faultline.raft.messages import AppendReply, AppendRequest, Entry, Message
from faultline.raft.node import Config, Node, Role
from tests.raft.cluster import Cluster

CONFIG = Config(election_ticks=10, heartbeat_ticks=2, pre_vote=False, check_quorum=False)


def solo_with_entries(fs: FakeFileSystem, count: int) -> Node:
    node = Node.open(fs, "/solo", 1, [1], CONFIG)
    while node.role is not Role.LEADER:
        node.tick()
    for i in range(count):
        node.propose(b"cmd%d" % i)
    node.take_committed()
    return node


def test_compact_saves_the_snapshot_and_drops_the_log() -> None:
    fs = FakeFileSystem()
    node = solo_with_entries(fs, 5)  # no-op at 1, then 2..6

    node.compact(4, b"state at 4")

    assert node.snapshot is not None
    assert (node.snapshot.index, node.snapshot.term, node.snapshot.data) == (4, 1, b"state at 4")
    assert node.log.base_index == 4
    assert node.log.last_index == 6


def test_cant_snapshot_what_was_not_applied() -> None:
    node = solo_with_entries(FakeFileSystem(), 2)

    with pytest.raises(ValueError, match="only 3 was applied"):
        node.compact(4, b"")


def test_restart_starts_after_the_snapshot() -> None:
    fs = FakeFileSystem()
    node = solo_with_entries(fs, 5)
    node.compact(4, b"state at 4")
    fs.crash()

    node = Node.open(fs, "/solo", 1, [1], CONFIG)

    assert node.snapshot is not None and node.snapshot.data == b"state at 4"
    # Up to the snapshot is committed and applied. The rest is handed out
    # again once this node learns it is committed.
    assert (node.commit_index, node.last_applied) == (4, 4)
    while node.role is not Role.LEADER:
        node.tick()
    assert [index for index, _ in node.take_committed()] == [5, 6, 7]


def test_follower_skips_entries_it_has_in_its_snapshot() -> None:
    fs = FakeFileSystem()
    node = Node.open(fs, "/n2", 2, [1, 2, 3], CONFIG)
    entries = tuple(Entry(1, b"%d" % i) for i in range(1, 6))
    node.step(Message(1, 2, 1, AppendRequest(0, 0, entries, commit_index=5)))
    node.take_committed()
    node.compact(3, b"state at 3")
    node.take_messages()

    # A late copy of the first append arrives. Its first three entries are
    # in our snapshot now and can't be checked against the log.
    node.step(Message(1, 2, 1, AppendRequest(0, 0, entries, commit_index=5)))

    [reply] = node.take_messages()
    assert reply.body == AppendReply(success=True, match_index=5)
    assert node.log.last_index == 5


@pytest.mark.parametrize("seed", range(5))
def test_cluster_keeps_working_while_logs_are_compacted(seed: int) -> None:
    cluster = Cluster(3, seed, CONFIG)
    cluster.wait_for_leader()
    for round_ in range(60):
        cluster.propose(b"cmd%d" % round_)
        cluster.tick()
        if round_ % 10 == 9:
            for node in cluster.nodes.values():
                node.compact(node.last_applied, b"state")

    cluster.tick(10)
    cluster.check_logs()
    assert all(node.log.base_index > 30 for node in cluster.nodes.values())
