# The database on top of Raft

Raft ([raft.md](raft.md)) makes a group of nodes agree on one list of
commands. The storage engine ([lsm.md](lsm.md)) keeps data on one disk. This
part puts them together into a replicated key-value database: every node
applies the same commands in the same order to its own engine, so every node
ends up with the same data.

```python
node = KVNode.open(fs, "/data/node1", node_id=1, peers=[1, 2, 3])
node.submit(Command(client=7, seq=1, op=Put(b"name", b"faultline")))
node.read(b"name", ctx=42)
# ... tick() and step() until the answers come out of
node.take_results()  # [(command, Result(ok=True))]
node.take_read_results()  # [(42, b"faultline")]
```

Like `raft.Node`, a `KVNode` never reads a clock or opens a socket. Whoever
runs it calls `tick()`, `step()` and `take_messages()`.

## Folders and keys

```
node1/raft/   term and vote, the Raft log, the latest snapshot
node1/data/   the storage engine
```

The engine holds more than the user's data, so every key gets a one-byte
prefix and a user key can never clash with ours:

| Key | What |
|---|---|
| `d` + key | the user's value |
| `s` + client id | that client's session: last seq and its result |
| `m/applied` | index of the last log entry applied |

## A write, start to end

1. The client sends `Command(client, seq, op)` to the leader. `op` is a put,
   a delete or a compare-and-set.
2. The leader adds it to its log, and Raft copies it to the others.
3. Once a majority has it, it is committed. Every node applies it.
4. The leader answers the client with the result.

Applying writes three things **in one WriteBatch**: the change itself, the
client's session, and the new applied index. A batch is atomic, so after a
crash all three are there or none are. As two separate writes, a crash in
between would either lose the command or apply it a second time after the
restart.

After a restart Raft hands out committed entries from the start of its log
again. The node skips every index at or below its saved applied index.

## Sessions: a retry runs once

A client that gets no answer can't tell whether its command was lost or only
the answer was. So it sends the same command again, with the same seq. If
the first copy did run, running the second one too would be wrong: a
compare-and-set would now fail, an increment would count twice.

So each node remembers, per client, the last seq it ran and that result
(Raft thesis, section 6.3). A command with that seq is a repeat: it gets the
saved answer and changes nothing. A command with an older seq is ignored,
since the client has moved on.

## Reads: ReadIndex

A read changes nothing, so it does not go through the log. But the leader
can't just answer from its own data either: it may have been cut off while
the others elected a new leader and committed new writes. Its answer would
be stale.

ReadIndex (Raft thesis, section 6.4) fixes that without a disk write:

1. The leader must have committed an entry in its own term (its no-op), or
   it doesn't know how far the log is really committed.
2. It notes its commit index. That is the read index.
3. It sends a round of heartbeats. A majority answering proves nobody had
   replaced it when the read started.
4. Once it has applied up to the read index, it answers from its data.

Each heartbeat carries a round number and every reply echoes it, so a reply
sent before the read started can't confirm it.

**Check quorum** (thesis, section 6.2) helps from the other side: a leader
that hasn't heard from a majority for an election timeout steps down. Reads
are safe without it, but it sends clients to the real leader much sooner.

## Snapshots

Every write adds a log entry, so the log would grow forever. Once
`snapshot_every` entries (1000 by default) were applied since the last
snapshot:

1. The state machine dumps every key and value of its engine, sessions and
   applied index included.
2. Raft saves that as its snapshot (`raft/SNAPSHOT`, replaced atomically).
3. Raft drops its log up to there: a new segment starting with a BASE record
   and the entries after it is written whole and renamed into place, then
   the old segments are deleted.

Snapshot first, then drop the log. A crash in between leaves both.

A follower so far behind that the leader no longer has the entries it is
missing gets the snapshot instead (`InstallSnapshot`). It saves it, drops or
trims its log, and its state machine replaces all its data with the
snapshot's, in one batch.

| Crash after | On restart |
|---|---|
| the follower saved the snapshot, before the machine loaded it | The machine's applied index is below the snapshot's, so it loads it at start. |
| the machine loaded it | Nothing to do. |

## How it is tested

- **The state machine on its own.** Every command type, sessions, retries,
  older commands, restarts. The power is cut at every step of one apply,
  and afterwards the command is fully there or not at all.
- **Raft's new parts.** Check quorum, ReadIndex, log compaction (power cut
  at every step of it, in both crash modes) and installing a snapshot.
- **End to end.** Five nodes, three clients, 800 rounds per seed. 5% of
  messages are lost and 5% repeated, the network splits and heals, nodes
  crash and come back, and some lose power in the middle of a disk write.
  Snapshots are taken every 20 entries, so lagging nodes catch up from
  them.

  Each client owns one key and is the only one writing it, so it always
  knows the right value. Every write answer must be "done", every read must
  return exactly that value, and at the end every node must hold it. It runs
  30 seeds like that, 15 more with check quorum off so that cut-off leaders
  hang around and get asked, and 5 with a quiet network.

To check that the tests catch real bugs, we broke the code on purpose, one
way at a time:

| Bug put in | Caught by |
|---|---|
| Sessions not checked (a retry runs again) | end to end, retry tests |
| Applied index written in its own batch | power cut at every step of an apply |
| ReadIndex answers without the heartbeat round | end to end with stale leaders, ReadIndex tests |
| A follower installs a snapshot but its data is not replaced | end to end, catch-up test |

All four were caught.

## Known limits

- **Sessions are never thrown away.** One small entry per client, forever.
  Real systems expire them (and then must refuse a retry from an expired
  session).
- **One request at a time per client.** That is what makes one saved result
  per client enough.
- **Reads only on the leader.** Followers could serve reads too by asking
  the leader for a read index. Leader leases would skip the heartbeat round,
  but they trust clocks.
- **Snapshots are whole.** The data is built in memory and sent in one
  message. Big databases stream it in chunks.
- **Clients find the leader by asking around.** A node that is not leader
  says no, but doesn't say who is.
- **Fixed membership,** as in [raft.md](raft.md).
