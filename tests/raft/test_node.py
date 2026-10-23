"""One node at a time, with messages handed to it by hand.

Whole clusters are tested in test_election.py.
"""

from faultline.fs.fake import FakeFileSystem
from faultline.raft import hard_state
from faultline.raft.hard_state import HardState
from faultline.raft.messages import (
    AppendReply,
    AppendRequest,
    Message,
    VoteReply,
    VoteRequest,
)
from faultline.raft.node import Config, Node, Role

PEERS = [1, 2, 3]
CONFIG = Config(election_ticks=10, heartbeat_ticks=2)


def new_node(node_id: int = 1, fs: FakeFileSystem | None = None, seed: int = 0) -> Node:
    fs = fs or FakeFileSystem()
    return Node.open(fs, f"/node{node_id}", node_id, PEERS, CONFIG, seed)


def tick_until_election(node: Node) -> None:
    for _ in range(2 * CONFIG.election_ticks):
        if node.role is not Role.FOLLOWER:
            return
        node.tick()
    raise AssertionError("no election after the longest possible timeout")


def test_starts_as_follower_in_term_zero() -> None:
    node = new_node()

    assert node.role is Role.FOLLOWER
    assert node.term == 0
    assert node.voted_for is None
    assert node.leader is None
    assert node.take_messages() == []


def test_times_out_and_asks_for_votes() -> None:
    node = new_node()

    tick_until_election(node)

    assert node.role is Role.CANDIDATE
    assert node.term == 1
    assert node.voted_for == 1
    assert node.take_messages() == [
        Message(1, 2, 1, VoteRequest(0, 0)),
        Message(1, 3, 1, VoteRequest(0, 0)),
    ]


def test_election_timeout_is_random_within_range() -> None:
    timeouts = set()
    for seed in range(50):
        node = new_node(seed=seed)
        ticks = 0
        while node.role is Role.FOLLOWER:
            node.tick()
            ticks += 1
        timeouts.add(ticks)

    assert min(timeouts) >= CONFIG.election_ticks
    assert max(timeouts) < 2 * CONFIG.election_ticks
    assert len(timeouts) > 3


def test_new_term_and_own_vote_are_saved_before_requests_go_out() -> None:
    fs = FakeFileSystem()
    node = new_node(fs=fs)

    tick_until_election(node)
    fs.crash()

    assert hard_state.load(fs, "/node1") == HardState(1, 1)


def test_candidate_tries_again_with_a_new_term() -> None:
    node = new_node()
    tick_until_election(node)
    node.take_messages()

    for _ in range(2 * CONFIG.election_ticks):
        node.tick()

    assert node.role is Role.CANDIDATE
    assert node.term >= 2


def test_bigger_term_makes_a_follower_with_no_vote() -> None:
    fs = FakeFileSystem()
    node = new_node(fs=fs)
    tick_until_election(node)

    node.step(Message(2, 1, 5, AppendReply(success=True, match_index=0)))

    assert node.role is Role.FOLLOWER
    assert node.term == 5
    assert node.voted_for is None
    assert hard_state.load(fs, "/node1") == HardState(5, None)


def test_old_vote_request_gets_a_no_with_our_term() -> None:
    node = new_node()
    node.step(Message(2, 1, 4, AppendReply(success=True, match_index=0)))

    node.step(Message(3, 1, 2, VoteRequest(9, 2)))

    assert node.take_messages() == [Message(1, 3, 4, VoteReply(granted=False))]


def test_old_append_request_gets_a_no_with_our_term() -> None:
    node = new_node()
    node.step(Message(2, 1, 4, AppendReply(success=True, match_index=0)))

    node.step(Message(3, 1, 2, AppendRequest(0, 0, (), 0)))

    assert node.take_messages() == [Message(1, 3, 4, AppendReply(success=False, match_index=0))]


def test_old_replies_are_dropped() -> None:
    node = new_node()
    node.step(Message(2, 1, 4, AppendReply(success=True, match_index=0)))

    node.step(Message(3, 1, 2, VoteReply(granted=True)))

    assert node.take_messages() == []
    assert node.term == 4


def test_restart_keeps_term_and_vote() -> None:
    fs = FakeFileSystem()
    node = new_node(fs=fs)
    tick_until_election(node)

    fs.crash()
    restarted = new_node(fs=fs)

    assert restarted.term == 1
    assert restarted.voted_for == 1
    assert restarted.role is Role.FOLLOWER
