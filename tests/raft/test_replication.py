"""The leader's side and the follower's side of copying the log, one node at
a time. Whole clusters are tested in test_safety.py."""

from faultline.fs.fake import FakeFileSystem
from faultline.raft.messages import AppendReply, AppendRequest, Entry, Message, VoteReply
from faultline.raft.node import Config, Node, Role

PEERS = [1, 2, 3]
CONFIG = Config(election_ticks=10, heartbeat_ticks=2, pre_vote=False)


def new_node(node_id: int, fs: FakeFileSystem | None = None) -> Node:
    return Node.open(fs or FakeFileSystem(), f"/node{node_id}", node_id, PEERS, CONFIG)


def new_leader(fs: FakeFileSystem | None = None) -> Node:
    """Node 1, elected leader by handing it the votes."""
    node = new_node(1, fs)
    while node.role is Role.FOLLOWER:
        node.tick()
    node.step(Message(2, 1, node.term, VoteReply(granted=True)))
    assert node.role is Role.LEADER
    node.take_messages()
    return node


def appends_to(node: Node, peer: int) -> list[AppendRequest]:
    return [
        m.body
        for m in node.take_messages()
        if m.receiver == peer and isinstance(m.body, AppendRequest)
    ]


def test_only_the_leader_takes_proposals() -> None:
    assert new_node(1).propose(b"set x 1") is None


def test_leader_adds_to_its_log_and_sends_it_out() -> None:
    fs = FakeFileSystem()
    leader = new_leader(fs)
    start = leader.log.last_index

    index = leader.propose(b"set x 1")

    assert index == start + 1
    assert leader.log.entry(start + 1) == Entry(leader.term, b"set x 1")
    sent = leader.take_messages()
    assert [m.receiver for m in sent] == [2, 3]
    for message in sent:
        assert message.body == AppendRequest(
            prev_log_index=start,
            prev_log_term=leader.log.term_at(start),
            entries=(Entry(leader.term, b"set x 1"),),
            commit_index=leader.commit_index,
        )
    # Already on the leader's disk
    fs.crash()
    assert new_node(1, fs).log.entry(start + 1) == Entry(leader.term, b"set x 1")


def test_success_moves_the_follower_forward() -> None:
    leader = new_leader()
    first = leader.propose(b"a")
    assert first is not None
    leader.take_messages()

    leader.step(Message(2, 1, leader.term, AppendReply(success=True, match_index=first)))
    leader.propose(b"b")

    (request,) = appends_to(leader, 2)
    assert request.prev_log_index == first
    assert request.entries == (Entry(leader.term, b"b"),)


def test_heartbeat_resends_what_is_not_acknowledged() -> None:
    leader = new_leader()
    index = leader.propose(b"a")
    leader.take_messages()

    for _ in range(CONFIG.heartbeat_ticks):
        leader.tick()

    (request,) = appends_to(leader, 3)
    assert request.entries[-1] == Entry(leader.term, b"a")
    assert request.prev_log_index + len(request.entries) == index


def test_an_old_success_reply_does_not_move_back() -> None:
    leader = new_leader()
    a = leader.propose(b"a")
    b = leader.propose(b"b")
    assert a is not None and b is not None
    leader.take_messages()

    leader.step(Message(2, 1, leader.term, AppendReply(success=True, match_index=b)))
    leader.step(Message(2, 1, leader.term, AppendReply(success=True, match_index=a)))
    for _ in range(CONFIG.heartbeat_ticks):
        leader.tick()

    (request,) = appends_to(leader, 2)
    assert request.prev_log_index == b
    assert request.entries == ()
