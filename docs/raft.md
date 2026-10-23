# Raft

The storage engine ([lsm.md](lsm.md)) keeps data safe on one machine. Raft
is how several machines keep **the same** data, so the database keeps
working when some of them die.

The idea: all nodes keep the same log of commands, in the same order. One
node, the **leader**, decides that order. The others, **followers**, copy
it. A command counts once a majority of nodes has it, so any majority that
is still alive has every command that counted.

This page has two parts: how a leader gets chosen (this part), and how the
log gets copied (part 2, coming with log replication).

## A node is a state machine

The node never reads a clock, opens a socket or starts a thread. Whoever
runs it drives it:

```python
node = Node.open(fs, "/data/node1", node_id=1, peers=[1, 2, 3])
node.tick()  # one unit of time passed
node.step(message)  # a message arrived
node.take_messages()  # what the node wants sent now
```

So a whole cluster runs from one seed, and a test decides when time passes
and which messages arrive, late, twice, or never. A real server calls the
same three things from a loop with a real clock and real sockets.

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
last term and at least as long (section 5.4.1). The second rule matters on
day 2 of Raft: it stops a node that missed committed entries from becoming
leader and losing them.

One vote per term is why there is never more than one leader in a term: a
win needs a majority, two majorities always share a node, and that node
voted only once.

**The vote is saved before the reply goes out.** The term and the vote live
in `RAFT_STATE`, replaced as a whole with a temp file and a rename (see
`fs/atomic.py`), so a crash leaves the old copy or the new one. A node that
voted, crashed, and forgot would vote again in the same term, and the
cluster could end up with two leaders.

Nothing else is saved. Role, leader, timers and vote counts start fresh
after a restart.

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

## How it is tested

- **One node at a time.** Messages handed in by hand: who gets a vote, what
  is saved before what, what each role does with each message.
- **Whole clusters** on a small in-memory network (`tests/raft/cluster.py`).
  A leader gets elected with 1, 3 and 5 nodes; it stays while the network is
  fine; a new one takes over when it crashes; a minority never elects one.
- **Chaos.** 300 runs (150 seeds, with and without pre-vote): messages lost,
  late and repeated, partitions that come and go, nodes crashing, and nodes
  rebooting right after they handle a message, when their reply is already
  on its way. All along, the network checks that **no term ever has two
  leaders**. When the trouble stops, a leader must still get elected.

To check that the tests catch real bugs, we broke the code on purpose, one
way at a time:

| Bug put in | Caught by |
|---|---|
| Vote not saved before the reply | chaos (two leaders in one term), and a node test |
| No log up-to-date check when voting | node test (cluster logs are empty until replication) |
| A second vote in the same term | chaos (two leaders in one term) |
| Pre-vote messages change terms | a leader is elected, cut-off node keeps its term |

The reboots in the chaos test were added because without them the first bug
slipped through: a crash only between rounds never hit the moment after a
vote was sent and before the next request arrived.

## Known limits

- **No check quorum.** A leader cut off from the majority still thinks it is
  leader until it hears a bigger term. That is safe for writes (they can't
  reach a majority), but reads need care. That comes with safe reads.
- **Fixed membership.** The cluster's nodes are set at start. Adding and
  removing nodes is a whole topic of its own (section 4 of the thesis).
- **The log is in memory.** Fine while only elections run. It goes to disk
  with log replication.
