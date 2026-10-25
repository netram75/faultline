# Bugs

The bugs found while building faultline, and the bugs put in on purpose to
check that the tests would find them. Every entry here really happened.

## Real bugs found

### Raft log compaction could lose committed entries on a power cut

Compaction (raft/log.py) writes a new log segment that starts with a BASE
record ("the log now starts after this index") followed by the entries
after it, then deletes the old segments. The first version wrote that
segment in place. A power cut in the middle could keep the BASE record and
tear off the entries after it, and those entries may already be committed.

**Found by** the power-cut tests, which cut the power at every step of a
compaction, in both crash modes. **Fixed** by writing the whole new segment
under a temporary name and renaming it into place, so replay sees all of it
or none of it, then deleting old segments newest first.

### A node rebooted mid-tick ran its clock twice as fast (livelock)

A simulator run (seed 24) got stuck: every node up, the network healed, and
no leader for five seconds. One node called an election every 60 ms and
nobody could win.

**The cause** was in the simulator: a node that died and came back during a
tick ended up with two rounds of ticks. With pre-vote off, a node with a
fast clock and a log that is behind times out first every time, starts a new
term, and knocks the others back to follower before they can time out
themselves. It can never win and never stops. **Found by** the simulator's
"stuck" check. **Fixed** with one line and a test. Details in
[docs/simulator.md](docs/simulator.md).

### The test network's partition cut off every node

A helper in the Raft tests, `partition()`, was meant to cut the listed
nodes off from the rest. But every node left off the list ended up alone,
so `partition({3})` cut every node off from every other. Two election tests
called it that way to cut off one node, and both still passed, which is the
worrying part: they checked the cut-off node and never looked at whether
the rest could still talk. **Fixed** in the replication tests: the nodes left off
the list now form one group together.

### Checker: milliseconds were not enough to order operations

The first draft of the checker took each operation's window from the
simulator's clock. But the simulator does many things in one millisecond,
and a client often starts its next operation in the same millisecond its
last one ended. The checker would have treated the two as overlapping and
let them swap, so a client could "read before its own write" and pass.
**Fixed** before it landed: windows are positions in the history, which
have a strict order.

### Checker: the obvious way to shrink a failure is wrong

The plan for explaining a failure was: drop operations one at a time while
the history stays bad. But dropping a write can make a good history look
bad, so the "explanation" could point at a problem that isn't there. The
explanation cuts the history at a point in time instead, which can only
remove problems. See [docs/checker.md](docs/checker.md).

### Checker: too slow on harsh runs ("unknown")

On harsh simulator settings (up to 8 clients on one key, client timeouts of
15 to 40 ms, 5 to 10% of messages lost or doubled) the first checker ran out
of steps on 19 of 400 runs. The search kept trying old writes that got no
answer in front of every compare-and-set that did not match. **Fixed** with
five changes to the search, each one safe (docs/checker.md says why). All 19
are now checked, and all are ok, in under 0.2 seconds each.

## The bug hunt

The bug hunt ran the linearizability checker over the real code:

| Runs | Settings | Not linearizable |
|---|---|---|
| 200 | 30 s of heavy faults, 5 nodes, 3 clients, 3 keys | 0 |
| 100 | 10 s of heavy faults, 3 nodes, 4 clients, 2 keys | 0 |
| 150 | 5 s reboot storms, 1 key | 0 |
| 12 | 120 s of heavy faults, 5 clients, 2 keys | 0 |
| 40 | 20 s of light faults, 8 clients, 1 key | 0 |
| 400 | harsh: short client timeouts, 2 to 10% message loss and duplication, snapshots every 50 entries, up to 8 clients on 1 key | 0 |

902 runs, no linearizability bug found. That is not proof there are none.
It is what the tests below say: if one of these known bugs were in the code,
these runs would very likely have shown it.

## Bugs put in on purpose

The same idea every day: break the code one way at a time and check that
some test fails. A test suite that has never caught a planted bug proves
little.

### Storage engine ([docs/lsm.md](docs/lsm.md))

| Bug put in | Caught by |
|---|---|
| Manifest save skips the folder sync | random power cuts |
| Manifest save skips the file sync | random power cuts |
| Flush deletes old WAL segments before saving the manifest | random power cuts |
| Memtable sees a write before the WAL sync | failed write is never visible |
| Flush leaves tombstones out of the table | random power cuts |
| Compaction deletes its inputs before saving the manifest | random power cuts |
| Open keeps tables the manifest doesn't list | random power cuts |
| Open replays segments below the replay point | flushed segments are not replayed |
| WAL numbering restarts at 1 when the folder is empty | WAL numbers never go back |
| Table writer skips the folder sync | finished table survives a crash |

### Raft ([docs/raft.md](docs/raft.md))

| Bug put in | Caught by |
|---|---|
| Vote not saved before the reply | chaos (two leaders in one term), and a node test |
| No log up-to-date check when voting | node test |
| A second vote in the same term | chaos (two leaders in one term) |
| Pre-vote messages change terms | a leader is elected, cut-off node keeps its term |
| Copies of old-term entries counted for commit | figure 8, and a node test |
| Conflicting entries not cut off | cut-off leader loses its entries, chaos |
| Entries not synced before the follower says yes | chaos, figure 8 |
| Follower takes the leader's commit index without the cap | follower catches up, and a node test |

### The database on Raft ([docs/kv.md](docs/kv.md))

| Bug put in | Caught by |
|---|---|
| Sessions not checked (a retry runs again) | end to end, retry tests |
| Applied index written in its own batch | power cut at every step of an apply |
| ReadIndex answers without the heartbeat round | end to end with stale leaders, ReadIndex tests |
| A follower installs a snapshot but its data is not replaced | end to end, catch-up test |

### The simulator's safety checks ([docs/simulator.md](docs/simulator.md))

| Bug put in | Caught? |
|---|---|
| Followers don't cut entries that conflict with the leader's | yes, 62 tests |
| Entries not synced before the follower says it has them | yes, 168 tests |
| A follower commits past what the leader's message vouched for | yes, 1 test (3-node runs) |
| A vote is kept in memory and not saved | yes, 7 reboot-storm runs |
| A leader counts copies of old-term entries to commit (figure 8) | **no** |

**The honest miss.** Figure 8 of the Raft paper needs a long, exact sequence
of leaders, and in thousands of runs the simulator never produced it. The
scripted test in `tests/raft/test_safety.py` plays that sequence step by
step and catches the bug. Random testing finds a lot, but not everything.

### The linearizability checker, on the whole system

These bugs live outside Raft, in what a node tells its clients. Raft's own
safety checks pass on every one of them. Only the checker sees them. Each
can be switched on from the command line (`--bug NAME`, see
`src/faultline/sim/bugs.py`):

| Bug | What it does | Caught in 100 seeds: 10 s heavy / 10 s light / 30 s heavy |
|---|---|---|
| `no-sessions` | the state machine forgets sessions, so a retried write runs twice | 99 / 97 / 99 |
| `early-ack` | a leader says ok to a put before it is committed | 44 / 47 / 73 |
| `leader-read` | a leader serves reads from its own data, without ReadIndex | 10 / 2 / 30 |
| none | the real code | 0 / 0 / 0 |

`leader-read` is the stale read Jepsen found in etcd and Consul in 2014. It
needs a leader that has been cut off and replaced but doesn't know it yet,
and a client that still asks it. Short runs rarely set that up. Longer ones
do.

### The checker itself ([docs/checker.md](docs/checker.md))

29 bugs put into the checker one at a time, all caught, plus one that
turned out not to be a bug. The table is in the checker doc.

### The real server ([docs/net.md](docs/net.md))

| Bug put in | Caught by |
|---|---|
| Every connection shares one client id | each connection is its own session |
| The write number (seq) never moves | the commands test (the second write gets the first one's answer) |
| NOTLEADER names this node instead of the leader | a follower points to the leader |
| A write that timed out is answered OK | a write with no majority times out |
| Any node answers reads from its own data | a follower points to the leader |
| A frame says it is one byte longer than it is | 32 tests, everything over TCP |
| A frame keeps its length bytes in the message | frame tests |
| A leader answers reads from its own data, without ReadIndex | **no** |

**The miss.** The test that freezes the leader with SIGSTOP should catch the
last one, and doesn't: when the frozen leader wakes up, the new leader's
messages are already waiting in its socket. It reads those first, sees the
newer term, and steps down before it gets to the read. Real processes can't
force the dangerous moment. The simulator can, which is why its
`leader-read` bug exists (above).

### The chaos client ([src/faultline/chaos/clients.py](src/faultline/chaos/clients.py))

How a chaos client writes down an outcome decides what the checker may
conclude, so it got its own bugs:

| Bug put in | Caught? |
|---|---|
| A write that timed out written down as failed | yes |
| A write sent again after its connection broke | yes |
| NOTLEADER ends the operation instead of asking the leader | yes |
| A compare-and-set that did not match written down as matched | yes |
| A write that timed out sent again | yes |
