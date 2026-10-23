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


def follower_with_log(*terms: int, fs: FakeFileSystem | None = None) -> Node:
    node = new_node(2, fs)
    node.log.append([Entry(term, f"old{i}".encode()) for i, term in enumerate(terms, 1)])
    return node


def append(node: Node, term: int, prev: tuple[int, int], *entries: Entry) -> list[Message]:
    node.step(Message(1, 2, term, AppendRequest(prev[0], prev[1], entries, commit_index=0)))
    return node.take_messages()


def test_follower_takes_entries_and_says_how_far_it_matches() -> None:
    fs = FakeFileSystem()
    node = new_node(2, fs)

    sent = append(node, 1, (0, 0), Entry(1, b"a"), Entry(1, b"b"))

    assert sent == [Message(2, 1, 1, AppendReply(success=True, match_index=2))]
    assert node.leader == 1
    # On disk before the reply went out
    fs.crash()
    assert new_node(2, fs).log.entries_from(1) == (Entry(1, b"a"), Entry(1, b"b"))


def test_heartbeat_gets_a_yes_when_the_logs_match() -> None:
    node = follower_with_log(1, 1)

    sent = append(node, 1, (2, 1))

    assert sent == [Message(2, 1, 1, AppendReply(success=True, match_index=2))]


def test_no_when_the_previous_entry_is_missing() -> None:
    node = follower_with_log(1)

    sent = append(node, 2, (3, 2), Entry(2, b"x"))

    assert sent == [Message(2, 1, 2, AppendReply(success=False, match_index=0))]
    assert node.log.last_index == 1


def test_no_when_the_previous_entry_has_another_term() -> None:
    node = follower_with_log(1, 1)

    sent = append(node, 3, (2, 2), Entry(3, b"x"))

    assert [m.body for m in sent] == [AppendReply(success=False, match_index=0)]
    assert node.log.last_index == 2


def test_conflicting_entries_are_replaced() -> None:
    # Entries 2 and 3 came from a leader of term 1 that never got them
    # committed. The leader of term 2 has something else at index 2.
    node = follower_with_log(1, 1, 1)

    append(node, 2, (1, 1), Entry(2, b"new"))

    assert node.log.entries_from(1) == (Entry(1, b"old1"), Entry(2, b"new"))


def test_a_late_shorter_message_cuts_nothing() -> None:
    node = new_node(2)
    append(node, 1, (0, 0), Entry(1, b"a"), Entry(1, b"b"), Entry(1, b"c"))

    # An older message from the same leader, overtaken on the way
    sent = append(node, 1, (0, 0), Entry(1, b"a"))

    assert node.log.last_index == 3
    assert sent == [Message(2, 1, 1, AppendReply(success=True, match_index=1))]


def test_a_repeated_message_changes_nothing() -> None:
    fs = FakeFileSystem()
    node = new_node(2, fs)
    append(node, 1, (0, 0), Entry(1, b"a"))
    steps = fs.steps

    append(node, 1, (0, 0), Entry(1, b"a"))

    assert node.log.entries_from(1) == (Entry(1, b"a"),)
    assert fs.steps == steps  # not even a write to disk
