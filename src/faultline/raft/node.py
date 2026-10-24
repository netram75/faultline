"""A Raft node, written as a state machine.

The node never reads a clock, opens a socket or starts a thread. Whoever runs
it drives it with a few calls:

    node.tick()             one unit of time passed
    node.step(message)      a message arrived from another node
    node.take_messages()    the messages the node wants sent now
    node.propose(command)   on the leader: add a command to the log
    node.take_committed()   committed commands, in order, to apply
    node.read_index(ctx)    on the leader: start a safe read (see read_index.py)
    node.take_reads()       reads that are safe to answer, with their read index
    node.compact(i, data)   the state machine saved a snapshot up to index i
    node.take_snapshot()    a snapshot from the leader, to load into the state machine

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
from faultline.raft import hard_state, snapshot
from faultline.raft.hard_state import HardState
from faultline.raft.log import RaftLog
from faultline.raft.messages import (
    AppendReply,
    AppendRequest,
    Body,
    Entry,
    InstallSnapshot,
    Message,
    NodeId,
    VoteReply,
    VoteRequest,
)
from faultline.raft.progress import Progress
from faultline.raft.read_index import ReadTracker
from faultline.raft.snapshot import Snapshot

# The command of the entry every new leader adds first (see _become_leader).
# Callers skip it when they apply entries.
NO_OP = b""


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
    # Most entries in one append. A follower far behind gets several.
    max_entries_per_message: int = 64
    # A leader that has not heard from a majority for election_ticks steps
    # down (Raft thesis, section 6.2)
    check_quorum: bool = True


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
        saved: Snapshot | None = None,
    ) -> None:
        # Use Node.open(), which loads the saved state and then calls this
        self.id = node_id
        self.log = log
        # The newest snapshot, if any. Everything up to its index is applied.
        self.snapshot = saved
        # A snapshot the leader sent, until the state machine takes it
        self._installed: Snapshot | None = None
        self.role = Role.FOLLOWER
        self.leader: NodeId | None = None
        # Highest log index known to be committed. Not saved: after a restart
        # the leader tells us again. Entries up to the log's base are in a
        # snapshot, so they are committed and applied.
        self.commit_index = log.base_index
        # Last index handed out by take_committed()
        self.last_applied = log.base_index
        self._fs = fs
        self._directory = directory
        self._others = sorted(set(peers) - {node_id})
        self._config = config
        self._rng = random.Random(seed)
        self._state = state
        self._outbox: list[Message] = []
        self._votes: set[NodeId] = set()  # who voted for us, while a candidate
        self._progress: dict[NodeId, Progress] = {}  # each follower, while leader
        self._election_elapsed = 0
        self._election_timeout = 0
        self._heartbeat_elapsed = 0
        # While leader: who answered us since the last quorum check
        self._heard_from: set[NodeId] = set()
        self._reads = ReadTracker(node_id)
        self._quorum_elapsed = 0
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
        log = RaftLog.open(fs, os.path.join(directory, "log"))
        saved = snapshot.load(fs, directory)
        return cls(fs, directory, node_id, peers, config or Config(), seed, state, log, saved)

    @property
    def term(self) -> int:
        return self._state.term

    @property
    def voted_for(self) -> NodeId | None:
        return self._state.voted_for

    def tick(self) -> None:
        if self.role is Role.LEADER:
            if self._lost_quorum():
                return
            # A leader never times out. It keeps reminding everyone it is there.
            self._heartbeat_elapsed += 1
            if self._heartbeat_elapsed >= self._config.heartbeat_ticks:
                self._broadcast_append()
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
            case AppendReply() as reply:
                self._on_append_reply(message.sender, reply)
            case InstallSnapshot() as request:
                self._on_install_snapshot(message.sender, request)

    def propose(self, command: bytes) -> int | None:
        """Add a command to the log. Returns its index, or None if this node is
        not the leader (the caller should try the leader instead).

        The command is not committed yet when this returns. It is committed
        once a majority has it.
        """
        if command == NO_OP:
            raise ValueError("an empty command is the leader's no-op, and can't be proposed")
        if self.role is not Role.LEADER:
            return None
        # On our own disk before anyone else hears of it
        self.log.append([Entry(self.term, command)])
        self._advance_commit()  # a cluster of one needs nobody else
        for peer, progress in self._progress.items():
            # A follower we are still probing gets it with the next probe
            if not progress.probing:
                self._send_append(peer)
        return self.log.last_index

    def take_committed(self) -> list[tuple[int, Entry]]:
        """Committed entries not handed out yet, as (index, entry), in log
        order. The caller applies them to its state machine.

        Once committed, an entry is final on every node that will ever be
        leader, so applying it is safe. Every node hands out the same entries
        in the same order, which is the whole point of Raft.
        """
        if self.commit_index <= self.last_applied:
            return []
        start = self.last_applied + 1
        entries = self.log.entries_from(start, self.commit_index - self.last_applied)
        self.last_applied = self.commit_index
        return list(enumerate(entries, start=start))

    def read_index(self, ctx: int) -> bool:
        """Start a safe read (Raft thesis, section 6.4). Returns False if this
        node can't serve reads right now: it is not the leader, or it has not
        committed an entry in its own term yet. The caller tries again later
        or elsewhere.

        Once a majority confirms this node is still the leader, take_reads()
        returns (ctx, read_index). The caller answers the read when its state
        machine has applied up to read_index.
        """
        if self.role is not Role.LEADER:
            return False
        # Step 1: until our no-op is committed, our commit index may be behind
        # the real one
        if self.log.term_at(self.commit_index) != self.term:
            return False
        self._reads.start(ctx, self.commit_index)
        self._reads.confirm(self._is_majority)  # a cluster of one needs nobody else
        # A heartbeat round now, not at the next tick, so the read is quick
        self._broadcast_append()
        return True

    def take_reads(self) -> list[tuple[int, int]]:
        """Reads confirmed since the last call, as (ctx, read_index)."""
        return self._reads.take_ready()

    def compact(self, index: int, data: bytes) -> None:
        """The state machine has everything up to `index` in `data`. Keep that
        as our snapshot and drop the log up to there (Raft paper, section 7)."""
        if index <= self.log.base_index:
            return
        if index > self.last_applied:
            raise ValueError(f"snapshot at {index}, but only {self.last_applied} was applied")
        saved = Snapshot(index, self.log.term_at(index), data)
        # Snapshot first, then drop the entries. See snapshot.py.
        snapshot.save(self._fs, self._directory, saved)
        self.snapshot = saved
        self.log.compact(index, saved.term)

    def take_snapshot(self) -> Snapshot | None:
        """A snapshot the leader sent us, once. The caller must replace its
        state machine's data with it before applying anything after it."""
        installed, self._installed = self._installed, None
        return installed

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
        self._accept_leader(leader)
        request = self._skip_compacted(request)

        # Raft paper, figure 2 (AppendEntries): our log must have the entry
        # just before the new ones, with the same term. If it does, the log
        # matching property (section 5.3) says everything before it matches
        # too, and the new entries can go right after it.
        if not self.log.matches(request.prev_log_index, request.prev_log_term):
            hint = self._mismatch_hint(request.prev_log_index)
            self._send(leader, AppendReply(success=False, match_index=hint, round=request.round))
            return
        self._append_new_entries(request.prev_log_index, request.entries)
        last_new = request.prev_log_index + len(request.entries)
        # Raft paper, figure 2 (AppendEntries step 5). Only up to what this
        # message vouched for: entries after it in our log may be left over
        # from an old leader and not match. And never down: a late message
        # can carry an older commit index.
        if request.commit_index > self.commit_index:
            self.commit_index = max(self.commit_index, min(request.commit_index, last_new))
        # log.append() synced the entries, so this promise holds after a crash
        self._send(leader, AppendReply(success=True, match_index=last_new, round=request.round))

    def _accept_leader(self, leader: NodeId) -> None:
        # Same term as ours: bigger terms were taken on above. Only one node
        # can win a term (each node votes once, and a win needs a majority),
        # so the sender is that term's leader, and we lost or never ran.
        assert self.role is not Role.LEADER, f"two leaders in term {self.term}"
        self.role = Role.FOLLOWER
        self.leader = leader
        self._reset_election_timer()

    def _on_install_snapshot(self, leader: NodeId, request: InstallSnapshot) -> None:
        """Raft paper, figure 13. The leader no longer has the entries we are
        missing, so it sends its snapshot instead."""
        self._accept_leader(leader)
        if request.last_index > self.commit_index:
            saved = Snapshot(request.last_index, request.last_term, request.data)
            # Saved before anything else changes, and before the reply. If we
            # crash before the state machine loads it, it is loaded at the
            # next start (see kv/server.py).
            snapshot.save(self._fs, self._directory, saved)
            self.snapshot = saved
            # Keeps the entries after it if our log agrees at last_index,
            # else drops the whole log (see RaftLog.compact)
            self.log.compact(request.last_index, request.last_term)
            # A snapshot only ever holds committed entries
            self.commit_index = self.last_applied = request.last_index
            self._installed = saved
        # Else we already have all of it, committed. Either way our log now
        # matches the leader's up to last_index.
        self._send(leader, AppendReply(success=True, match_index=request.last_index))

    def _skip_compacted(self, request: AppendRequest) -> AppendRequest:
        """Drop the entries of `request` that are already in our snapshot.

        We can't check them against our log any more, but we don't need to:
        they are committed, and a committed entry is the same on every node.
        So the request becomes one that starts right after our base.
        """
        base = self.log.base_index
        if request.prev_log_index >= base:
            return request
        skip = base - request.prev_log_index
        return AppendRequest(
            prev_log_index=base,
            prev_log_term=self.log.base_term,
            entries=request.entries[skip:],
            commit_index=request.commit_index,
            round=request.round,
        )

    def _mismatch_hint(self, prev_index: int) -> int:
        """Where the leader should try next, after our log failed its check at
        `prev_index`. Always before prev_index, so every failed try moves the
        leader back, and usually by a lot (Raft paper, end of section 5.3):

        * our log is shorter: start from where it ends
        * our entry at prev_index has another term: every entry of that term
          is suspect, so skip back to just before the first of them
        """
        if prev_index > self.log.last_index:
            return self.log.last_index
        suspect_term = self.log.term_at(prev_index)
        index = prev_index
        while index > self.log.base_index + 1 and self.log.term_at(index - 1) == suspect_term:
            index -= 1
        return index - 1

    def _append_new_entries(self, prev_index: int, entries: tuple[Entry, ...]) -> None:
        """Raft paper, figure 2 (AppendEntries steps 3 and 4). Skip entries we
        already have. At the first one that conflicts (same index, different
        term), cut our log there and take the leader's entries from that point.

        Never cut when nothing conflicts. A late or repeated message can carry
        fewer entries than a newer one already gave us, and cutting would throw
        away entries we already told the leader we have.
        """
        for offset, entry in enumerate(entries):
            index = prev_index + 1 + offset
            if index <= self.log.last_index and self.log.term_at(index) == entry.term:
                continue
            if index <= self.log.last_index:
                self.log.truncate_from(index)
            self.log.append(entries[offset:])
            return

    def _on_append_reply(self, follower: NodeId, reply: AppendReply) -> None:
        if self.role is not Role.LEADER:
            return
        # Any answer in our term, yes or no, means this follower still sees
        # us as its leader
        self._heard_from.add(follower)
        self._reads.answered(follower, reply.round)
        self._reads.confirm(self._is_majority)
        progress = self._progress[follower]
        if reply.success:
            progress.on_success(reply.match_index)
            self._advance_commit()
            # A follower that is behind gets the next batch straight away
            if progress.next <= self.log.last_index:
                self._send_append(follower)
        else:
            progress.on_failure(reply.match_index)
            # Try again from the new spot straight away, not at the next heartbeat
            self._send_append(follower)

    def _advance_commit(self) -> None:
        """Raft paper, figure 2 (last rule for leaders) and section 5.4.2.

        An entry is committed once a majority of nodes has it. But the leader
        only counts copies of entries from its own term. Entries from older
        terms get committed along with the first current-term entry above
        them. Counting copies of an old entry is not enough: figure 8 of the
        paper shows an old entry on a majority that a later leader still
        overwrites.
        """
        for index in range(self.log.last_index, self.commit_index, -1):
            if self.log.term_at(index) != self.term:
                return  # terms only go down from here, so no current-term entry is left
            copies = 1 + sum(1 for p in self._progress.values() if p.match >= index)
            if 2 * copies > len(self._others) + 1:
                self.commit_index = index
                return

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader = self.id
        self._heard_from = set()
        self._quorum_elapsed = 0
        self._reads = ReadTracker(self.id)
        self._progress = {
            peer: Progress(match=0, next=self.log.last_index + 1) for peer in self._others
        }
        # Raft paper, section 8. A new leader can't tell which of the older
        # entries in its log are committed, and can't commit them by counting
        # copies (see _advance_commit). An entry from its own term fixes both:
        # once that is committed, so is everything before it. So it starts
        # with an empty one, a no-op, instead of waiting for a client.
        self.log.append([Entry(self.term, NO_OP)])
        self._advance_commit()
        # Tell everyone at once, before another node times out and runs
        self._broadcast_append()

    def _broadcast_append(self) -> None:
        """Send every follower what it is missing. For a follower that is up
        to date that is an append with no entries, a heartbeat: it says "I am
        the leader for this term", and resets the follower's election timer."""
        self._heartbeat_elapsed = 0
        for peer in self._others:
            self._send_append(peer)

    def _send_append(self, peer: NodeId) -> None:
        progress = self._progress[peer]
        if progress.next <= self.log.base_index:
            # The entries it needs were compacted away. Only the snapshot can
            # bring it up to date.
            self._send_snapshot(peer)
            return
        prev_index = progress.next - 1
        entries = self.log.entries_from(progress.next, self._config.max_entries_per_message)
        request = AppendRequest(
            prev_log_index=prev_index,
            prev_log_term=self.log.term_at(prev_index),
            entries=entries,
            commit_index=self.commit_index,
            round=self._reads.round,
        )
        self._send(peer, request)
        progress.sent(len(entries))

    def _lost_quorum(self) -> bool:
        """Raft thesis, section 6.2 (check quorum). Once every election_ticks,
        a leader checks that a majority answered it since the last check. If
        not, it steps down to follower in the same term.

        A leader cut off from the rest can't commit anything, but without
        this it would go on thinking it is the leader, and keep clients
        talking to it, for as long as the network is split. The other side
        has elected a new leader by now anyway: it got no heartbeats for
        longer than this. Safe reads (ReadIndex) don't depend on it, but it
        sends clients to the real leader much sooner.
        """
        if not self._config.check_quorum:
            return False
        self._quorum_elapsed += 1
        if self._quorum_elapsed < self._config.election_ticks:
            return False
        self._quorum_elapsed = 0
        if self._is_majority(self._heard_from | {self.id}):
            self._heard_from = set()
            return False
        self._become_follower(self.term)
        return True

    def _send_snapshot(self, peer: NodeId) -> None:
        saved = self.snapshot
        # The log is only ever compacted after a snapshot is saved
        assert saved is not None and saved.index >= self.log.base_index
        self._send(peer, InstallSnapshot(saved.index, saved.term, saved.data))
        self._progress[peer].sent_snapshot(saved.index)

    def _become_follower(self, term: int) -> None:
        if term != self.term:
            self._save(HardState(term, None))
        self.role = Role.FOLLOWER
        self.leader = None
        # Reads still waiting are dropped. Their callers time out and retry.
        self._reads = ReadTracker(self.id)
        self._reset_election_timer()

    def _answer_old_message(self, message: Message) -> None:
        """A message from an earlier term. A request gets a "no" that carries
        our term, so a stale candidate or leader learns it is behind and steps
        down. Old replies are dropped: whatever they answer is over."""
        match message.body:
            case VoteRequest() as request:
                self._send(message.sender, VoteReply(granted=False, pre_vote=request.pre_vote))
            case AppendRequest() | InstallSnapshot():
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
