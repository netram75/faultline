"""A Raft node, written as a state machine.

The node never reads a clock, opens a socket or starts a thread. Whoever runs
it drives it with three calls:

    node.tick()             one unit of time passed
    node.step(message)      a message arrived from another node
    node.take_messages()    the messages the node wants sent now

That makes a whole cluster repeatable from one seed: a test (and later the
fault simulator) decides when time passes and which messages arrive, in
which order, or not at all. A real server just calls the same three things
from a loop with a real clock and real sockets.

Every node starts as a follower. If it hears nothing from a leader for its
election timeout, it asks the others whether they would vote for it (a
pre-vote), and if a majority would, it becomes a candidate and asks for real
votes. Terms number the elections: each election gets a new, bigger term,
and every message carries the sender's term (Raft paper, section 5.1).

What must survive a restart (the term and the vote) is saved to disk before
any message that depends on it is put in the outbox. Everything else, like
the role or who the leader is, is rebuilt after a restart.
"""

import os
import random
from dataclasses import dataclass
from enum import Enum
from typing import Self

from faultline.fs import FileSystem
from faultline.raft import hard_state
from faultline.raft.hard_state import HardState
from faultline.raft.log import RaftLog
from faultline.raft.messages import (
    AppendReply,
    AppendRequest,
    Body,
    Message,
    NodeId,
    VoteReply,
    VoteRequest,
)


class Role(Enum):
    FOLLOWER = "follower"
    PRE_CANDIDATE = "pre-candidate"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True)
class Config:
    # A follower that hears nothing from a leader for this many ticks (plus
    # a random extra, up to the same again) starts an election
    election_ticks: int = 10
    # A leader sends a heartbeat this often. Must be well below election_ticks.
    heartbeat_ticks: int = 2
    # Ask for pre-votes before a real election (Raft thesis, section 9.6)
    pre_vote: bool = True


class Node:
    def __init__(
        self,
        fs: FileSystem,
        directory: str,
        node_id: NodeId,
        peers: list[NodeId],
        config: Config,
        seed: int,
        state: HardState,
        log: RaftLog,
    ) -> None:
        # Use Node.open(), which loads the saved state and then calls this
        self.id = node_id
        self.log = log
        self.role = Role.FOLLOWER
        self.leader: NodeId | None = None
        self._fs = fs
        self._directory = directory
        self._others = sorted(set(peers) - {node_id})
        self._config = config
        self._rng = random.Random(seed)
        self._state = state
        self._outbox: list[Message] = []
        self._votes: set[NodeId] = set()  # who voted for us, while a candidate
        self._election_elapsed = 0
        self._election_timeout = 0
        self._heartbeat_elapsed = 0
        self._reset_election_timer()

    @classmethod
    def open(
        cls,
        fs: FileSystem,
        directory: str,
        node_id: NodeId,
        peers: list[NodeId],
        config: Config | None = None,
        seed: int = 0,
    ) -> Self:
        """Start a node, or restart one, from what is saved in `directory`.

        `peers` lists every node in the cluster, this one included.
        """
        if not fs.exists(directory):
            fs.mkdir(directory)
            fs.sync_dir(os.path.dirname(os.path.normpath(directory)) or ".")
        state = hard_state.load(fs, directory)
        return cls(fs, directory, node_id, peers, config or Config(), seed, state, RaftLog())

    @property
    def term(self) -> int:
        return self._state.term

    @property
    def voted_for(self) -> NodeId | None:
        return self._state.voted_for

    def tick(self) -> None:
        if self.role is Role.LEADER:
            # A leader never times out. It keeps reminding everyone it is there.
            self._heartbeat_elapsed += 1
            if self._heartbeat_elapsed >= self._config.heartbeat_ticks:
                self._send_heartbeats()
            return
        self._election_elapsed += 1
        if self._election_elapsed >= self._election_timeout:
            if self._config.pre_vote:
                self._start_pre_vote()
            else:
                self._start_election()

    def step(self, message: Message) -> None:
        if message.receiver != self.id:
            raise ValueError(f"message for node {message.receiver} given to node {self.id}")

        if message.term > self.term and not _leaves_term_alone(message.body):
            # Raft paper, figure 2: anyone who sees a bigger term than its own
            # is behind. It moves to that term, as a follower with no vote.
            self._become_follower(message.term)
        elif message.term < self.term:
            self._answer_old_message(message)
            return

        match message.body:
            case VoteRequest(pre_vote=True) as request:
                self._on_pre_vote_request(message.sender, message.term, request)
            case VoteRequest() as request:
                self._on_vote_request(message.sender, request)
            case VoteReply() as reply:
                self._on_vote_reply(message.sender, reply)
            case AppendRequest() as request:
                self._on_append_request(message.sender, request)

    def take_messages(self) -> list[Message]:
        """Everything the node wants sent since the last call, in order."""
        messages, self._outbox = self._outbox, []
        return messages

    def _start_pre_vote(self) -> None:
        """Raft thesis, section 9.6. Before a real election, ask "would you
        vote for me?" without changing anyone's term.

        Without this, a node cut off from the others times out again and
        again, and its term climbs with every try. When it comes back, that
        big term forces the working leader to step down for nothing. With
        it, the cut-off node never gets a majority of pre-votes, so its term
        stays where it was.
        """
        self.role = Role.PRE_CANDIDATE
        self.leader = None
        self._reset_election_timer()
        self._votes = {self.id}
        if self._is_majority(self._votes):
            self._start_election()  # a cluster of one
            return
        request = VoteRequest(self.log.last_index, self.log.last_term, pre_vote=True)
        for peer in self._others:
            # The term we would run in. Nobody moves to it because of this.
            self._send(peer, request, term=self.term + 1)

    def _start_election(self) -> None:
        # A new term, and our own vote, saved before anyone hears about them
        self._save(HardState(self.term + 1, self.id))
        self.role = Role.CANDIDATE
        self.leader = None
        self._reset_election_timer()
        self._votes = {self.id}
        if self._is_majority(self._votes):
            self._become_leader()  # a cluster of one
            return
        for peer in self._others:
            self._send(peer, VoteRequest(self.log.last_index, self.log.last_term))

    def _on_vote_request(self, candidate: NodeId, request: VoteRequest) -> None:
        """Raft paper, figure 2 (RequestVote): vote for at most one candidate
        per term, and only for one whose log is at least as up to date as
        ours (section 5.4.1). The second rule is what stops a node that
        missed committed entries from ever becoming leader."""
        free_to_vote = self.voted_for in (None, candidate)
        up_to_date = self.log.is_up_to_date(request.last_log_index, request.last_log_term)
        granted = free_to_vote and up_to_date
        if granted:
            # Saved before the reply goes out. A node that forgot its vote in
            # a restart could vote twice in one term and elect two leaders.
            self._save(HardState(self.term, candidate))
            # The candidate we voted for may win. Give it time to tell us.
            self._reset_election_timer()
        self._send(candidate, VoteReply(granted))

    def _on_pre_vote_request(self, candidate: NodeId, term: int, request: VoteRequest) -> None:
        """Would we vote for this candidate in an election for `term`? Nothing
        changes on our side either way: no new term, no vote saved."""
        granted = (
            term > self.term
            # If we heard from a leader lately, it is working and there is no
            # reason for an election
            and not self._leader_seems_alive()
            and self.log.is_up_to_date(request.last_log_index, request.last_log_term)
        )
        # A yes carries the term asked about. A no carries our own term, so a
        # node that is behind on terms catches up.
        self._send(
            candidate, VoteReply(granted, pre_vote=True), term=term if granted else self.term
        )

    def _on_vote_reply(self, voter: NodeId, reply: VoteReply) -> None:
        # Replies from older terms never get here, and a bigger term has
        # already made us a follower. So a reply we see while still waiting
        # for votes is for the round we are in.
        waiting_for = Role.PRE_CANDIDATE if reply.pre_vote else Role.CANDIDATE
        if self.role is not waiting_for or not reply.granted:
            return
        # A set, so a reply that arrives twice still counts once
        self._votes.add(voter)
        if not self._is_majority(self._votes):
            return
        if reply.pre_vote:
            self._start_election()
        else:
            self._become_leader()

    def _on_append_request(self, leader: NodeId, request: AppendRequest) -> None:
        # Same term as ours: bigger terms were taken on above. Only one node
        # can win a term (each node votes once, and a win needs a majority),
        # so the sender is that term's leader, and we lost or never ran.
        assert self.role is not Role.LEADER, f"two leaders in term {self.term}"
        self.role = Role.FOLLOWER
        self.leader = leader
        self._reset_election_timer()

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader = self.id
        # Tell everyone at once, before another node times out and runs
        self._send_heartbeats()

    def _send_heartbeats(self) -> None:
        """An append with no entries. It says "I am the leader for this term",
        and every follower that gets it resets its election timer."""
        self._heartbeat_elapsed = 0
        for peer in self._others:
            self._send(
                peer, AppendRequest(self.log.last_index, self.log.last_term, (), commit_index=0)
            )

    def _become_follower(self, term: int) -> None:
        if term != self.term:
            self._save(HardState(term, None))
        self.role = Role.FOLLOWER
        self.leader = None
        self._reset_election_timer()

    def _answer_old_message(self, message: Message) -> None:
        """A message from an earlier term. A request gets a "no" that carries
        our term, so a stale candidate or leader learns it is behind and steps
        down. Old replies are dropped: whatever they answer is over."""
        match message.body:
            case VoteRequest() as request:
                self._send(message.sender, VoteReply(granted=False, pre_vote=request.pre_vote))
            case AppendRequest():
                self._send(message.sender, AppendReply(success=False, match_index=0))

    def _leader_seems_alive(self) -> bool:
        if self.role is Role.LEADER:
            return True
        return self.leader is not None and self._election_elapsed < self._config.election_ticks

    def _is_majority(self, nodes: set[NodeId]) -> bool:
        return 2 * len(nodes) > len(self._others) + 1

    def _reset_election_timer(self) -> None:
        # Random, so that nodes rarely time out together and split the vote
        # (Raft paper, section 5.2)
        self._election_elapsed = 0
        ticks = self._config.election_ticks
        self._election_timeout = self._rng.randrange(ticks, 2 * ticks)

    def _save(self, state: HardState) -> None:
        hard_state.save(self._fs, self._directory, state)
        self._state = state

    def _send(self, receiver: NodeId, body: Body, term: int | None = None) -> None:
        self._outbox.append(Message(self.id, receiver, self.term if term is None else term, body))


def _leaves_term_alone(body: Body) -> bool:
    """Pre-vote messages carry the term a candidate would run in, not a term
    anyone is in yet, so they never move anyone's term. A refused pre-vote is
    the one exception: it carries the voter's real term."""
    match body:
        case VoteRequest(pre_vote=True) | VoteReply(pre_vote=True, granted=True):
            return True
    return False
