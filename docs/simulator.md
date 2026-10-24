# The fault simulator

The simulator runs a whole faultline cluster (five nodes, their disks, the
network between them, and clients) inside one Python process, from one seed.
While clients work, a nemesis crashes nodes, cuts the power in the middle of
disk writes, splits the network, loses messages and skews clocks. After
every single step, Raft's safety rules are checked.

```
$ python -m faultline.sim --seed 7 --seconds 30
seed 7, 30 s of heavy faults, then calm
operations: 1101 ok, 7 failed, 9 unknown
ok while calm: 466
highest term: 299, crashes: 71, power cuts: 7, snapshots taken: 5
messages: 26747 sent, 4704 dropped
safety checks passed: 41881
history digest: 390c30f7b6fd59ca
```

```
python -m faultline.sim --seed 1 --runs 1000           # bug hunt: one line per seed
python -m faultline.sim --seed 7 --log                 # what the nemesis did, and when
python -m faultline.sim --seed 7 --history out.jsonl   # every client operation, for the checker
```

The idea comes from FoundationDB, which tests its database this way, and from
Jepsen, which finds bugs in real databases from the outside.

## One seed, one run

Nothing in a run reads the real clock, opens a socket or starts a thread.
The Raft node and the database were written that way from the start: they
only move when someone calls `tick()` or `step()`. That is what makes this
possible.

**Time** is a number in the event loop (`sim/loop.py`). Everything that
happens is an event with a time: a node's timer ticks, a message arrives, a
client sends a request, the nemesis strikes. The loop runs them in time
order and jumps its clock straight to the next one. Nothing waits, so 20
simulated seconds take about a fifth of a real second: 100 runs of 20
seconds take about 23 seconds on a laptop.

**Randomness** comes from one generator per part (each disk, the network,
each client, the nemesis), each seeded from the run's seed and the part's
name. Same seed, same run, event for event. The history digest at the end is
how you see it. A bug found by seed 160 is found by seed 160 every time, and
can be replayed, printed and stepped through until it is understood.

One trap: Python's `hash()` of a string changes every time Python starts, so
nothing may iterate over a set of strings or bytes where the order matters,
and seeds are derived with blake2b, not `hash()`.

## The parts

```
sim/loop.py      the clock and the event queue
sim/network.py   latency, loss, duplicates, reordering, partitions, bridges
sim/node.py      a KVNode on its own fake disk, ticking on its own clock
sim/client.py    clients that find the leader, time out and retry
sim/history.py   every client operation, for the checker
sim/nemesis.py   the faults, on a random schedule
sim/checks.py    Raft's safety rules, after every step
sim/run.py       one run, start to end, and its summary
```

**Network.** Each message takes 1 to 10 ms, picked at random, so messages
overtake each other. 1% are lost and 1% arrive twice. Every message is
encoded to bytes and decoded again, like on a real socket. A partition is a
list of groups, and a node can be in two groups: a bridge, which talks to
both sides while the sides can't talk to each other.

**Nodes.** Each has its own `FakeFileSystem` (see [wal.md](wal.md)), so a
crash loses exactly what that node had not synced. Each ticks every 10 ms of
its own clock, and a clock can run up to 30% fast or slow.

**Clients.** Each does one operation at a time on a few shared keys: get,
put or compare-and-set. A node that is not the leader says no, and the
client asks another. A node that says yes may never answer, so every try
times out, and the client asks again with the same seq (sessions make that
safe, see [kv.md](kv.md)). Each operation ends ok, failed (surely did not
happen) or unknown (a write that may or may not have happened).

**The nemesis.** Every half second on average (`heavy`), one of:

| Fault | What happens |
|---|---|
| partition | one node cut off, or a minority and a majority, or two sides with a bridge; heals later |
| crash | a node loses power between two steps; comes back later |
| reboot | a node dies and comes straight back |
| power cut | a node loses power in the middle of one of its next 50 disk calls |
| loss burst | half of all messages lost, for a while |
| clock skew | one node's clock runs up to 30% fast or slow |

On top of that, any node may die right after any step it takes (0.2% of
steps in `heavy`, 3% in `reboots`) and come straight back. Its replies are
already on their way. If it promised something it had not saved yet, it has
now forgotten a promise someone else counts on.

The seed also picks Raft's settings: election timeouts of 3, 5 or 10 ticks,
and pre-vote and check quorum each on or off. Short timeouts mean many
elections, some with two candidates in one term. The default settings
almost never produce those.

**Safety checks**, after every step of every node (Raft paper, figure 3):

- at most one leader per term
- every node that commits an index commits the same entry there
- a new leader already has every committed entry

A broken rule stops the run at once with the seed and the time.

**Liveness.** After the trouble, the nemesis heals everything and stops. If
no client operation succeeds in the next 5 seconds, the run is stuck.

## What it found

While the simulator was being built, a run (seed 24) got stuck: no leader
for five seconds, with every node up and the network healed. One node was
calling an election every 60 ms, and nobody could win.

The cause was in the simulator: a node that died and came back during a
tick ended up with two rounds of ticks, so its clock ran twice as fast.
With pre-vote off, a node whose clock is that fast and whose log is behind
times out first every time, starts a new term, and the others drop back to
follower with fresh timers before they can time out themselves. It can never
win (its log is behind) and never stop. A livelock.

The fix was one line, plus a test. It is also a good reminder of why pre-vote
exists: with pre-vote on, that node would have asked first, been told no,
and never bumped the term.

## How we know it catches bugs

We broke Raft on purpose, one way at a time, and ran the simulator's tests
(`tests/sim`):

| Bug put in | Caught? |
|---|---|
| Followers don't cut entries that conflict with the leader's | yes, 62 tests |
| Entries not synced before the follower says it has them | yes, 168 tests |
| A follower commits past what the leader's message vouched for | yes, 1 test (3-node runs) |
| A vote is kept in memory and not saved | yes, 7 reboot-storm runs |
| A leader counts copies of old-term entries to commit (figure 8) | **no** |

Two lessons from this table:

- **The vote bug needs a crash in a gap of a few milliseconds:** after a node
  votes and before a second candidate asks. Ordinary crashes almost never
  land there: in 300 runs of 20 seconds of heavy faults it showed up twice.
  Dying right after a step lands there on purpose. The reboot storm catches
  it in 7 of its 100 short runs, and since a run is fixed by its seed, the
  test suite fails on it every time.
- **Figure 8 needs a long, exact sequence of leaders,** and in thousands of
  runs the simulator never produced it. The scripted test in
  `tests/raft/test_safety.py` plays that sequence step by step and catches
  the bug. Random testing finds a lot, but not everything. Known traps still
  deserve their own test.

## Known limits

- **Disks are instant.** A sync takes no simulated time, so slow disks are
  not simulated, and neither is a disk that lies about syncing.
- **Linearizability is checked once, at the end.** The safety checks look
  inside Raft after every step. The linearizability checker
  (`src/faultline/checker/`) reads the whole history when the run is over,
  so a run that goes wrong early still runs to the end before it is caught.
- **Clients know every node.** They don't need to discover the cluster.
- **One process.** Real concurrency (threads, real sockets) is not exercised
  here. That is what the real cluster and its chaos tests are for (Day 9).
