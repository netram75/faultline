"""Safe reads with ReadIndex (Raft thesis, section 6.4)."""

import pytest

from faultline.fs.fake import FakeFileSystem
from faultline.raft.messages import AppendReply, AppendRequest, Message, VoteReply, VoteRequest
from faultline.raft.node import Config, Node, Role
from tests.raft.cluster import Cluster

CONFIG = Config(election_ticks=10, heartbeat_ticks=2, pre_vote=False, check_quorum=False)


def new_leader() -> Node:
    """Node 1 of 3, leader in term 1, with its no-op not committed yet."""
    node = Node.open(FakeFileSystem(), "/node1", 1, [1, 2, 3], CONFIG)
    while node.role is not Role.CANDIDATE:
        node.tick()
    node.step(Message(2, 1, node.term, VoteReply(granted=True)))
    node.take_messages()
    return node


def commit_no_op(node: Node) -> None:
    node.step(Message(2, 1, node.term, AppendReply(success=True, match_index=1)))
    node.take_messages()
    assert node.commit_index == 1


def test_follower_refuses() -> None:
    node = Node.open(FakeFileSystem(), "/node1", 1, [1, 2, 3], CONFIG)

    assert not node.read_index(1)


def test_new_leader_refuses_until_its_no_op_commits() -> None:
    node = new_leader()

    assert not node.read_index(1)
    commit_no_op(node)
    assert node.read_index(1)


def test_read_waits_for_a_majority_to_answer_its_round() -> None:
    node = new_leader()
    commit_no_op(node)

    assert node.read_index(7)

    # The read started round 1, and a heartbeat with it went out at once
    sent = node.take_messages()
    assert {m.receiver for m in sent} == {2, 3}
    assert all(isinstance(m.body, AppendRequest) and m.body.round == 1 for m in sent)
    assert node.take_reads() == []

    # An answer to an older round may have been sent before the read started
    node.step(Message(2, 1, 1, AppendReply(success=True, match_index=1, round=0)))
    assert node.take_reads() == []

    # Node 2 answers round 1. With the leader that is 2 of 3.
    node.step(Message(2, 1, 1, AppendReply(success=True, match_index=1, round=1)))
    assert node.take_reads() == [(7, 1)]
    assert node.take_reads() == []


def test_a_no_answer_still_confirms_the_leader() -> None:
    # A failed append in our term still means the follower sees us as leader
    node = new_leader()
    commit_no_op(node)
    node.read_index(7)

    node.step(Message(3, 1, 1, AppendReply(success=False, match_index=0, round=1)))

    assert node.take_reads() == [(7, 1)]


def test_read_index_is_the_commit_index_when_the_read_starts() -> None:
    node = new_leader()
    commit_no_op(node)
    node.propose(b"x")
    node.read_index(7)  # commit index is still 1, entry 2 is not committed

    node.step(Message(2, 1, 1, AppendReply(success=True, match_index=2, round=1)))

    assert node.commit_index == 2
    assert node.take_reads() == [(7, 1)]


def test_reads_come_out_in_order() -> None:
    node = new_leader()
    commit_no_op(node)
    node.read_index(1)
    node.read_index(2)

    node.step(Message(2, 1, 1, AppendReply(success=True, match_index=1, round=2)))

    assert node.take_reads() == [(1, 1), (2, 1)]


def test_cluster_of_one_reads_at_once() -> None:
    node = Node.open(FakeFileSystem(), "/solo", 1, [1], CONFIG)
    while node.role is not Role.LEADER:
        node.tick()

    assert node.read_index(5)
    assert node.take_reads() == [(5, 1)]


def test_waiting_reads_are_dropped_when_the_leader_steps_down() -> None:
    node = new_leader()
    commit_no_op(node)
    node.read_index(7)

    node.step(Message(3, 1, 2, VoteRequest(last_log_index=5, last_log_term=1)))
    node.step(Message(2, 1, 1, AppendReply(success=True, match_index=1, round=1)))

    assert node.role is Role.FOLLOWER
    assert node.take_reads() == []


@pytest.mark.parametrize("seed", range(10))
def test_cut_off_leader_can_never_confirm_a_read(seed: int) -> None:
    # Without check quorum, a cut-off leader goes on thinking it leads. Its
    # data is about to be stale, and ReadIndex is what stops it answering.
    cluster = Cluster(5, seed, CONFIG)
    old = cluster.wait_for_leader()
    cluster.tick(5)  # its no-op commits
    cluster.partition({old})
    for _ in range(500):
        cluster.tick()
        if cluster.leader() not in (None, old):
            break
    new = cluster.leader()
    assert new is not None and new != old
    cluster.propose(b"newer write")
    cluster.tick(5)

    assert cluster.nodes[old].role is Role.LEADER  # it doesn't know
    assert cluster.nodes[old].read_index(1)
    assert cluster.nodes[new].read_index(2)
    cluster.tick(50)

    assert cluster.nodes[old].take_reads() == []
    [(ctx, read_index)] = cluster.nodes[new].take_reads()
    assert ctx == 2
    assert read_index == cluster.nodes[new].log.last_index  # it covers the newer write
