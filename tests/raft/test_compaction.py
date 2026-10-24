"""A node whose log was compacted after a snapshot (Raft paper, section 7)."""

import pytest

from faultline.fs.fake import FakeFileSystem
from faultline.raft.messages import AppendReply, AppendRequest, Entry, InstallSnapshot, Message
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


def follower() -> Node:
    node = Node.open(FakeFileSystem(), "/n2", 2, [1, 2, 3], CONFIG)
    entries = (Entry(1, b"a"), Entry(1, b"b"), Entry(2, b"c"))
    node.step(Message(1, 2, 2, AppendRequest(0, 0, entries, commit_index=1)))
    node.take_messages()
    node.take_committed()
    return node


def test_follower_installs_a_snapshot() -> None:
    node = follower()

    node.step(Message(1, 2, 2, InstallSnapshot(last_index=10, last_term=2, data=b"state at 10")))

    assert node.take_messages() == [Message(2, 1, 2, AppendReply(success=True, match_index=10))]
    assert (node.log.base_index, node.log.last_index) == (10, 10)
    assert (node.commit_index, node.last_applied) == (10, 10)
    installed = node.take_snapshot()
    assert installed is not None and installed.data == b"state at 10"
    assert node.take_snapshot() is None  # handed out once
    assert node.take_committed() == []


def test_follower_keeps_entries_after_a_snapshot_it_agrees_with() -> None:
    node = follower()

    node.step(Message(1, 2, 2, InstallSnapshot(last_index=2, last_term=1, data=b"state at 2")))

    assert (node.log.base_index, node.log.last_index) == (2, 3)
    assert node.log.entry(3) == Entry(2, b"c")


def test_follower_ignores_a_snapshot_it_already_has() -> None:
    node = follower()  # commit index 1

    node.step(Message(1, 2, 2, InstallSnapshot(last_index=1, last_term=1, data=b"old")))

    assert node.take_snapshot() is None
    assert node.log.base_index == 0
    [reply] = node.take_messages()
    assert reply.body == AppendReply(success=True, match_index=1)


def test_installed_snapshot_survives_a_restart() -> None:
    fs = FakeFileSystem()
    node = Node.open(fs, "/n2", 2, [1, 2, 3], CONFIG)
    node.step(Message(1, 2, 2, InstallSnapshot(last_index=10, last_term=2, data=b"state")))
    fs.crash()

    node = Node.open(fs, "/n2", 2, [1, 2, 3], CONFIG)

    assert node.snapshot is not None and node.snapshot.index == 10
    assert (node.log.base_index, node.commit_index) == (10, 10)


@pytest.mark.parametrize("seed", range(5))
def test_leader_sends_its_snapshot_to_a_follower_far_behind(seed: int) -> None:
    cluster = Cluster(3, seed, CONFIG)
    leader = cluster.wait_for_leader()
    behind = next(i for i in cluster.ids if i != leader)
    cluster.partition({behind})
    for round_ in range(30):
        cluster.propose(b"cmd%d" % round_)
        cluster.tick()
    cluster.tick(5)
    for i in cluster.ids:
        if i != behind:
            node = cluster.nodes[i]
            node.compact(node.last_applied, b"state at %d" % node.last_applied)

    # Without pre-vote, the cut-off node has been running elections and comes
    # back with a big term. That costs one election; whoever wins has the
    # compacted log.
    cluster.heal()
    leader = cluster.wait_for_leader()
    cluster.tick(20)

    caught_up = cluster.nodes[behind]
    assert caught_up.snapshot is not None
    assert caught_up.log.last_index == cluster.nodes[leader].log.last_index
    cluster.check_logs()
