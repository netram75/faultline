"""One node at a time, with messages handed to it by hand.

Whole clusters are tested in test_election.py.
"""

from faultline.fs.fake import FakeFileSystem
from faultline.raft import hard_state
from faultline.raft.hard_state import HardState
from faultline.raft.messages import (
    AppendReply,
    AppendRequest,
    Entry,
    Message,
    VoteReply,
    VoteRequest,
)
from faultline.raft.node import Config, Node, Role

PEERS = [1, 2, 3]
# Most tests here are about plain elections. Pre-vote has its own tests at the end.
CONFIG = Config(election_ticks=10, heartbeat_ticks=2, pre_vote=False)
PRE_VOTE = Config(election_ticks=10, heartbeat_ticks=2, pre_vote=True)


def new_node(
    node_id: int = 1, fs: FakeFileSystem | None = None, seed: int = 0, config: Config = CONFIG
) -> Node:
    fs = fs or FakeFileSystem()
    return Node.open(fs, f"/node{node_id}", node_id, PEERS, config, seed)


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


def give_log(node: Node, *terms: int) -> None:
    node.log.append([Entry(term, b"x") for term in terms])


def test_votes_for_the_first_candidate_that_asks() -> None:
    fs = FakeFileSystem()
    node = new_node(fs=fs)

    node.step(Message(2, 1, 1, VoteRequest(0, 0)))

    assert node.take_messages() == [Message(1, 2, 1, VoteReply(granted=True))]
    assert node.voted_for == 2
    # The vote is on disk before the reply leaves
    fs.crash()
    assert hard_state.load(fs, "/node1") == HardState(1, 2)


def test_only_one_vote_per_term() -> None:
    node = new_node()
    node.step(Message(2, 1, 1, VoteRequest(0, 0)))
    node.take_messages()

    node.step(Message(3, 1, 1, VoteRequest(0, 0)))

    assert node.take_messages() == [Message(1, 3, 1, VoteReply(granted=False))]
    assert node.voted_for == 2


def test_same_candidate_asking_again_gets_yes_again() -> None:
    # The first reply may have been lost
    node = new_node()
    node.step(Message(2, 1, 1, VoteRequest(0, 0)))
    node.take_messages()

    node.step(Message(2, 1, 1, VoteRequest(0, 0)))

    assert node.take_messages() == [Message(1, 2, 1, VoteReply(granted=True))]


def test_a_new_term_frees_the_vote() -> None:
    node = new_node()
    node.step(Message(2, 1, 1, VoteRequest(0, 0)))
    node.take_messages()

    node.step(Message(3, 1, 2, VoteRequest(0, 0)))

    assert node.take_messages() == [Message(1, 3, 2, VoteReply(granted=True))]
    assert (node.term, node.voted_for) == (2, 3)


def test_candidate_does_not_vote_for_a_rival() -> None:
    node = new_node()
    tick_until_election(node)
    node.take_messages()

    node.step(Message(2, 1, node.term, VoteRequest(0, 0)))

    assert node.take_messages() == [Message(1, 2, node.term, VoteReply(granted=False))]


def test_no_vote_for_a_candidate_with_an_older_last_term() -> None:
    node = new_node()
    give_log(node, 1, 2)

    node.step(Message(2, 1, 3, VoteRequest(last_log_index=5, last_log_term=1)))

    assert node.take_messages() == [Message(1, 2, 3, VoteReply(granted=False))]
    assert node.voted_for is None


def test_no_vote_for_a_candidate_with_a_shorter_log() -> None:
    node = new_node()
    give_log(node, 1, 2)

    node.step(Message(2, 1, 3, VoteRequest(last_log_index=1, last_log_term=2)))

    assert node.take_messages() == [Message(1, 2, 3, VoteReply(granted=False))]


def test_vote_for_a_candidate_with_a_newer_last_term() -> None:
    node = new_node()
    give_log(node, 1, 1, 1)

    node.step(Message(2, 1, 3, VoteRequest(last_log_index=1, last_log_term=2)))

    assert node.take_messages() == [Message(1, 2, 3, VoteReply(granted=True))]


def test_voting_resets_the_election_timer() -> None:
    node = new_node()
    for _ in range(CONFIG.election_ticks - 1):
        node.tick()

    node.step(Message(2, 1, 1, VoteRequest(0, 0)))
    for _ in range(CONFIG.election_ticks - 1):
        node.tick()

    assert node.role is Role.FOLLOWER


def test_vote_survives_a_restart() -> None:
    fs = FakeFileSystem()
    node = new_node(fs=fs)
    node.step(Message(2, 1, 1, VoteRequest(0, 0)))

    fs.crash()
    restarted = new_node(fs=fs)
    restarted.step(Message(3, 1, 1, VoteRequest(0, 0)))

    assert restarted.take_messages() == [Message(1, 3, 1, VoteReply(granted=False))]


def elect(node: Node) -> None:
    """Make `node` win an election by handing it the votes."""
    tick_until_election(node)
    node.take_messages()
    for voter in PEERS:
        if voter != node.id:
            node.step(Message(voter, node.id, node.term, VoteReply(granted=True)))


def heartbeat(node: Node) -> AppendRequest:
    return AppendRequest(node.log.last_index, node.log.last_term, (), commit_index=0)


def test_majority_of_votes_wins() -> None:
    node = new_node()
    tick_until_election(node)
    node.take_messages()

    node.step(Message(2, 1, 1, VoteReply(granted=True)))

    assert node.role is Role.LEADER
    assert node.leader == 1
    # Heartbeats go out straight away
    assert node.take_messages() == [
        Message(1, 2, 1, heartbeat(node)),
        Message(1, 3, 1, heartbeat(node)),
    ]


def test_no_votes_no_win() -> None:
    node = new_node()
    tick_until_election(node)

    node.step(Message(2, 1, 1, VoteReply(granted=False)))
    node.step(Message(3, 1, 1, VoteReply(granted=False)))

    assert node.role is Role.CANDIDATE


def test_a_vote_counts_once() -> None:
    fs = FakeFileSystem()
    node = Node.open(fs, "/node1", 1, [1, 2, 3, 4, 5], CONFIG)
    tick_until_election(node)

    node.step(Message(2, 1, 1, VoteReply(granted=True)))
    node.step(Message(2, 1, 1, VoteReply(granted=True)))

    assert node.role is Role.CANDIDATE


def test_cluster_of_one_elects_itself() -> None:
    node = Node.open(FakeFileSystem(), "/solo", 1, [1], CONFIG)

    tick_until_election(node)

    assert node.role is Role.LEADER
    assert node.term == 1
    assert node.take_messages() == []


def test_leader_sends_heartbeats_and_never_times_out() -> None:
    node = new_node()
    elect(node)
    node.take_messages()

    for _ in range(10 * CONFIG.election_ticks):
        node.tick()

    assert node.role is Role.LEADER
    assert node.term == 1
    sent = node.take_messages()
    expected = 10 * CONFIG.election_ticks // CONFIG.heartbeat_ticks * 2
    assert len(sent) == expected
    assert all(isinstance(m.body, AppendRequest) for m in sent)


def test_heartbeats_keep_a_follower_from_running() -> None:
    node = new_node()

    for _ in range(10 * CONFIG.election_ticks):
        node.tick()
        node.step(Message(2, 1, 1, AppendRequest(0, 0, (), 0)))

    assert node.role is Role.FOLLOWER
    assert node.leader == 2
    assert node.term == 1


def test_candidate_gives_up_when_someone_else_wins() -> None:
    node = new_node()
    tick_until_election(node)

    node.step(Message(2, 1, node.term, AppendRequest(0, 0, (), 0)))

    assert node.role is Role.FOLLOWER
    assert node.leader == 2


def test_leader_steps_down_for_a_bigger_term() -> None:
    node = new_node()
    elect(node)

    node.step(Message(3, 1, 7, AppendRequest(0, 0, (), 0)))

    assert node.role is Role.FOLLOWER
    assert node.term == 7
    assert node.leader == 3


def test_pre_vote_first_and_the_term_stays() -> None:
    fs = FakeFileSystem()
    node = new_node(fs=fs, config=PRE_VOTE)

    tick_until_election(node)

    assert node.role is Role.PRE_CANDIDATE
    assert node.term == 0
    assert node.voted_for is None
    assert hard_state.load(fs, "/node1") == HardState(0, None)
    request = VoteRequest(0, 0, pre_vote=True)
    assert node.take_messages() == [Message(1, 2, 1, request), Message(1, 3, 1, request)]


def test_majority_of_pre_votes_starts_the_real_election() -> None:
    node = new_node(config=PRE_VOTE)
    tick_until_election(node)
    node.take_messages()

    node.step(Message(2, 1, 1, VoteReply(granted=True, pre_vote=True)))

    assert node.role is Role.CANDIDATE
    assert node.term == 1
    assert node.voted_for == 1
    assert node.take_messages() == [
        Message(1, 2, 1, VoteRequest(0, 0)),
        Message(1, 3, 1, VoteRequest(0, 0)),
    ]


def test_cut_off_node_keeps_its_term() -> None:
    node = new_node(config=PRE_VOTE)

    for _ in range(50 * PRE_VOTE.election_ticks):
        node.tick()

    assert node.role is Role.PRE_CANDIDATE
    assert node.term == 0


def test_pre_vote_request_changes_nothing_on_the_voter() -> None:
    fs = FakeFileSystem()
    node = new_node(fs=fs, config=PRE_VOTE)

    node.step(Message(2, 1, 1, VoteRequest(0, 0, pre_vote=True)))

    assert node.take_messages() == [Message(1, 2, 1, VoteReply(granted=True, pre_vote=True))]
    assert node.term == 0
    assert node.voted_for is None
    assert not fs.exists("/node1/RAFT_STATE")


def test_no_pre_vote_while_the_leader_is_alive() -> None:
    node = new_node(config=PRE_VOTE)
    node.step(Message(2, 1, 1, AppendRequest(0, 0, (), 0)))

    node.step(Message(3, 1, 2, VoteRequest(0, 0, pre_vote=True)))

    assert node.take_messages() == [Message(1, 3, 1, VoteReply(granted=False, pre_vote=True))]


def test_pre_vote_once_the_leader_has_gone_quiet() -> None:
    node = new_node(config=PRE_VOTE)
    node.step(Message(2, 1, 1, AppendRequest(0, 0, (), 0)))
    for _ in range(PRE_VOTE.election_ticks):
        node.tick()
    node.take_messages()

    node.step(Message(3, 1, 2, VoteRequest(0, 0, pre_vote=True)))

    assert Message(1, 3, 2, VoteReply(granted=True, pre_vote=True)) in node.take_messages()


def test_no_pre_vote_for_a_log_that_is_behind() -> None:
    node = new_node(config=PRE_VOTE)
    give_log(node, 1, 1)

    node.step(Message(2, 1, 1, VoteRequest(1, 1, pre_vote=True)))

    assert node.take_messages() == [Message(1, 2, 0, VoteReply(granted=False, pre_vote=True))]


def test_refused_pre_vote_with_a_bigger_term_catches_us_up() -> None:
    node = new_node(config=PRE_VOTE)
    tick_until_election(node)

    node.step(Message(2, 1, 6, VoteReply(granted=False, pre_vote=True)))

    assert node.role is Role.FOLLOWER
    assert node.term == 6


def test_leader_refuses_pre_votes() -> None:
    node = new_node(config=PRE_VOTE)
    tick_until_election(node)
    node.step(Message(2, 1, 1, VoteReply(granted=True, pre_vote=True)))
    node.step(Message(2, 1, 1, VoteReply(granted=True)))
    assert node.role is Role.LEADER
    node.take_messages()

    node.step(Message(3, 1, 2, VoteRequest(0, 0, pre_vote=True)))

    assert node.take_messages() == [Message(1, 3, 1, VoteReply(granted=False, pre_vote=True))]
    assert node.role is Role.LEADER


def test_cluster_of_one_with_pre_vote() -> None:
    node = Node.open(FakeFileSystem(), "/solo", 1, [1], PRE_VOTE)

    tick_until_election(node)

    assert node.role is Role.LEADER
    assert node.term == 1


def test_log_survives_a_restart() -> None:
    fs = FakeFileSystem()
    node = new_node(fs=fs)
    node.log.append([Entry(1, b"set x 1")])

    fs.crash()

    assert new_node(fs=fs).log.entries_from(1) == (Entry(1, b"set x 1"),)
