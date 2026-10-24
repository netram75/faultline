# Raft

The storage engine ([lsm.md](lsm.md)) keeps data safe on one machine. Raft
is how several machines keep **the same** data, so the database keeps
working when some of them die.

The idea: all nodes keep the same log of commands, in the same order. One
node, the **leader**, decides that order. The others, **followers**, copy
it. A command counts once a majority of nodes has it, so any majority that
is still alive has every command that counted.

This page has two parts: how a leader gets chosen, and how the log gets
copied and committed.

## A node is a state machine

The node never reads a clock, opens a socket or starts a thread. Whoever
runs it drives it:

```python
node = Node.open(fs, "/data/node1", node_id=1, peers=[1, 2, 3])
node.tick()  # one unit of time passed
node.step(message)  # a message arrived
node.take_messages()  # what the node wants sent now
node.propose(b"set x 1")  # on the leader: add a command to the log
node.take_committed()  # committed commands, in order, to apply
```

So a whole cluster runs from one seed, and a test decides when time passes
and which messages arrive, late, twice, or never. A real server calls the
same three things from a loop with a real clock and real sockets.

# Part 1: electing a leader

## Terms

Time is split into **terms**, numbered 1, 2, 3... Each term starts with an
election and has at most one leader. Every message carries the sender's
term, and two rules use it (Raft paper, figure 2):

- See a **bigger** term than yours? You are behind. Move to that term as a
  follower, with no vote yet.
- Get a request with a **smaller** term? It is from the past. Say no, and
  put your term in the answer so the sender learns it is behind.

## Roles

```
              times out                majority of pre-votes
 follower  -------------> pre-candidate ------------------> candidate
    ^                                                          |
    |  sees a bigger term, or a leader for its term            | majority of votes
    +----------------------------------------------------------+------> leader
```

**Follower.** Waits. Every message from the leader resets its election
timer. If the timer runs out, the leader is probably gone.

**Candidate.** Moves to a new term, votes for itself, asks everyone else
for a vote. With votes from a majority (itself included), it is the leader.

**Leader.** Sends a heartbeat (an append with no entries) every
`heartbeat_ticks`, so followers know it is alive.

The election timeout is random, between `election_ticks` and twice that, and
picked again every time the timer resets. If all followers timed out
together they would all run, all vote for themselves, and nobody would win.
With random timeouts one usually goes first and wins before the others wake
up (section 5.2).

## Voting

A node gives **at most one vote per term**, and only to a candidate whose
log is **at least as up to date** as its own: a later last term, or the same
last term and at least as long (section 5.4.1). The second rule is what
stops a node that missed committed entries from becoming leader and
throwing them away.

One vote per term is why there is never more than one leader in a term: a
win needs a majority, two majorities always share a node, and that node
voted only once.

**The vote is saved before the reply goes out.** The term and the vote live
in `RAFT_STATE`, replaced as a whole with a temp file and a rename (see
`fs/atomic.py`), so a crash leaves the old copy or the new one. A node that
voted, crashed, and forgot would vote again in the same term, and the
cluster could end up with two leaders.

## Pre-vote

A problem with plain Raft: cut one follower off from the others. It hears
no heartbeats, so it times out, moves to a new term and runs. Nobody answers.
It times out again, and again, and its term keeps climbing. When the network
comes back, its big term reaches the leader, and the leader has to step down
(rule one above) though nothing was wrong with it.

The fix is from Diego Ongaro's thesis, section 9.6. Before a real election,
a node asks **"would you vote for me?"** with the term it would run in, but
nobody changes their term over it, the asker included. A node says yes only
if:

- that term is newer than its own,
- the asker's log is up to date, and
- it has not heard from a leader within `election_ticks`. If it has, the
  leader is fine and there is no reason for an election.

Only with a majority of yeses does the node run a real election. A node
that is cut off never gets them, so its term stays put, and coming back
changes nothing.

The tests show the difference: with pre-vote, a node cut off for 300 ticks
comes back with the same term and the same leader carries on. Without it,
its term has gone up by many, and the whole cluster has to hold an election.

# Part 2: copying the log

## On disk

```
node1/
  RAFT_STATE        term and vote
  log/00000001.wal  the log
```

The log is a WAL (see [wal.md](wal.md)) of two kinds of records: APPEND
(index, term, command) and TRUNCATE (cut everything from this index on).
Opening the node replays them. `append()` and `truncate_from()` return only
once the record is synced.

Only the term, the vote and the log are saved. Role, leader, timers, vote
counts, what each follower has, and the commit index all start fresh after a
restart and are learned again.

## The leader sends, the follower checks

`propose(command)` on the leader adds an entry `(term, command)` to its own
log, synced, and sends it to the followers in an append:

```
AppendRequest(prev_log_index=7, prev_log_term=3, entries=[...], commit_index=6)
```

"Here are entries that go right after your entry 7, which should have term 3."

The follower checks that one entry. If it has entry 7 with term 3, the
**log matching property** (section 5.3) says everything before it matches
the leader's log too, because a leader only ever adds entries after one the
follower already agreed on. So the new entries can go right after it.

Then, for each new entry (figure 2, AppendEntries steps 3 and 4):

- already there with the same term: skip it
- there with a **different** term: that entry and everything after it came
  from an old leader and were never committed. Cut them off, take the
  leader's from here.
- not there: append it

And it **never cuts when nothing conflicts**. A late, repeated message can
carry fewer entries than a newer one already gave us. Cutting would throw
away entries we already told the leader we have.

The entries are synced, then the follower answers `success` with
`match_index`: "my log matches yours up to here".

## When the check fails

The follower answers `success=False`, and puts a hint in `match_index`:
where the leader should try next.

- its log is shorter than `prev_log_index`: try from where it ends
- its entry there has another term: every entry of that term is suspect, so
  skip back to just before the first of them (end of section 5.3)

The hint is always before `prev_log_index`, so every failure moves the
leader back. A follower with an empty log catches up in two round trips,
not one per missing entry.

## How much the leader sends

For each follower the leader keeps `match` (known to match up to here) and
`next` (what to send next). A new leader knows nothing, so it guesses the
follower is up to date: `next` just past its own log.

- **Probing.** We don't know where the follower's log matches ours yet, so
  every append may fail. One at a time: when a reply comes back, or once per
  heartbeat if none does.
- **Streaming.** The last append worked. Each new entry goes out as soon as
  it is proposed, and `next` moves past it without waiting for the reply.
  If a message is lost, the next append fails its check and we are back to
  probing.

No append carries more than `max_entries_per_message` entries. A follower
far behind gets one batch per reply.

## Committing

An entry is **committed** once a majority of nodes has it. From then on it
will be in the log of every future leader (the voting rule makes sure), so
it is safe to apply.

The leader moves `commit_index` forward when a majority's `match` has
reached an entry. But it only counts copies of entries **from its own
term**. Older entries get committed along with the first entry of the
current term above them.

Why not count copies of an old entry? Figure 8 of the Raft paper:

1. Node 1 leads in term 2 and gets entry X to node 2 only. It crashes.
2. Node 5 wins term 3 with votes from 3 and 4, and puts its own entry at
   X's index. It crashes before anyone sees it.
3. Node 1 comes back, wins term 4, and copies X to nodes 3 and 4. Now a
   majority has X.
4. If node 1 crashes now, node 5 can still win term 5: its last term (3) is
   newer than X's (2), so nodes 2, 3 and 4 vote for it. It overwrites X.

So "a majority has it" was not enough for X. Had node 1 counted it, a
committed entry would be gone. `test_figure_8` plays these four steps on
the test network.

Followers learn the commit index from the leader's appends, but only up to
the last entry that message vouched for: entries past it in their log may
be left over from an old leader. And the commit index never goes down.

## The no-op

A new leader can't tell which older entries in its log are committed, and
by the rule above it can't commit them by counting either. Waiting for the
next client command could take forever. So the moment it wins, the leader
adds an empty entry of its own term, a **no-op** (section 8). Once that is
committed, so is everything before it. Callers skip empty commands when they
apply, and `propose(b"")` is refused.

## Applying

`take_committed()` hands out committed entries that were not handed out
yet, in order, as `(index, entry)`. Every node hands out the same entries in
the same order. That is the whole point: feed them to the same state
machine and every node ends up in the same state.

## How it is tested

- **One node at a time.** Messages handed in by hand: who gets a vote, what
  is saved before what, what a follower does with each kind of append, how
  the leader backs up, what it counts as committed.
- **Whole clusters** on a small in-memory network (`tests/raft/cluster.py`)
  that can lose, delay, repeat and reorder messages, cut the network into
  parts, and crash and restart nodes with their disks. While it runs it
  checks, after every single message:
  - **election safety**: never two leaders in one term
  - **state machine safety**: every node applies the same entry at the same
    index
  - **leader completeness**: a new leader has every entry committed before
    it

  and at the end, **log matching**: two logs with the same entry (index and
  term) are the same up to there.
- **Elections**: a leader gets elected with 1, 3 and 5 nodes; it stays while
  the network is fine; a new one takes over when it crashes; a minority
  never elects one; a node cut off for 300 ticks comes back with the same
  term (with pre-vote) or forces an election (without).
- **Replication**: every node applies every command in order; a follower
  that was down catches up in batches; a leader cut off from the majority
  loses the entries it could not commit; figure 8, step by step.
- **Chaos.** 300 election runs and 200 replication runs (each seed with and
  without pre-vote), with all the faults at once, including nodes rebooting
  right after they handle a message, when their reply is already on its
  way. When the trouble stops, every node must end up with the same log.

To check that the tests catch real bugs, we broke the code on purpose, one
way at a time:

| Bug put in | Caught by |
|---|---|
| Vote not saved before the reply | chaos (two leaders in one term), and a node test |
| No log up-to-date check when voting | node test |
| A second vote in the same term | chaos (two leaders in one term) |
| Pre-vote messages change terms | a leader is elected, cut-off node keeps its term |
| Copies of old-term entries counted for commit | figure 8, and a node test |
| Conflicting entries not cut off | cut-off leader loses its entries, chaos |
| Entries not synced before the follower says yes | chaos (a new leader missing a committed entry), figure 8 |
| Follower takes the leader's commit index without the cap | follower catches up, and a node test |

Two of these first slipped through, and the tests got better for it:

- The vote bug needed the quick reboots. Crashing only between rounds never
  hit the moment after a vote was sent and before the next request came.
- The commit-rule bug needed figure 8 played by hand. In random runs the
  no-op almost always travels with the old entries, so the bad count never
  happened to matter.

## Known limits

- Snapshots (so the log stops growing), check quorum and safe reads came
  later, with the database on top of Raft. See [kv.md](kv.md).
- **Fixed membership.** The cluster's nodes are set at start. Adding and
  removing nodes is a topic of its own (chapter 4 of the thesis).
- **One sync per proposal.** Each `propose()` syncs on its own. Batching
  several commands into one sync is an easy win for later.
