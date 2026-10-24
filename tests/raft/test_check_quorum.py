"""Check quorum (Raft thesis, section 6.2): a leader that can't hear from a
majority steps down."""

import pytest

from faultline.fs.fake import FakeFileSystem
from faultline.raft.messages import AppendReply, Message, VoteReply
from faultline.raft.node import Config, Node, Role
from tests.raft.cluster import Cluster

QUORUM = Config(election_ticks=10, heartbeat_ticks=2, pre_vote=False, check_quorum=True)


def new_leader(config: Config) -> Node:
    node = Node.open(FakeFileSystem(), "/node1", 1, [1, 2, 3], config)
    while node.role is not Role.CANDIDATE:
        node.tick()
    node.step(Message(2, 1, node.term, VoteReply(granted=True)))
    return node


def test_new_leader_is_leader() -> None:
    assert new_leader(QUORUM).role is Role.LEADER


def test_leader_with_no_answers_steps_down() -> None:
    node = new_leader(QUORUM)

    for _ in range(QUORUM.election_ticks):
        node.tick()

    assert node.role is Role.FOLLOWER
    assert node.leader is None
    assert node.term == 1  # stepping down is not a new term


def test_leader_that_hears_from_a_majority_stays() -> None:
    node = new_leader(QUORUM)

    for _ in range(10 * QUORUM.election_ticks):
        node.tick()
        # Node 2 keeps answering. With the leader itself that is 2 of 3.
        node.step(Message(2, 1, node.term, AppendReply(success=True, match_index=0)))

    assert node.role is Role.LEADER


def test_answers_must_keep_coming() -> None:
    node = new_leader(QUORUM)
    node.step(Message(2, 1, node.term, AppendReply(success=True, match_index=0)))

    # One answer covers one check, not every check after it
    for _ in range(2 * QUORUM.election_ticks):
        node.tick()

    assert node.role is Role.FOLLOWER


def test_can_be_switched_off() -> None:
    node = new_leader(Config(election_ticks=10, pre_vote=False, check_quorum=False))

    for _ in range(100):
        node.tick()

    assert node.role is Role.LEADER


@pytest.mark.parametrize("seed", range(10))
def test_cut_off_leader_steps_down(seed: int) -> None:
    cluster = Cluster(5, seed)
    old = cluster.wait_for_leader()
    term = cluster.nodes[old].term

    cluster.partition({old})
    cluster.tick(3 * cluster.config.election_ticks)

    # It stepped down. Alone, it now tries pre-votes that can't win, so its
    # term stays put.
    assert cluster.nodes[old].role is not Role.LEADER
    assert cluster.nodes[old].term == term
    new = cluster.wait_for_leader()
    assert new != old
