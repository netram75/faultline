# How the pieces fit together

faultline is a stack of layers. Each one only talks to the one below it, and
each has its own page in this folder. This page is the map: what sits where,
and what happens to one write and one read on their way through all of it.

## The layers

```
                     Redis clients
                          |
   net/       client port (RESP)   peer port (frames over TCP)    one process
              ClientServer         NodeRunner: clock, sockets      per node
   ------------------------------------------------------------------------
   kv/        KVNode: Raft + state machine, sessions, safe reads
   raft/      Node: elections, the log, commit, snapshots, ReadIndex
   ------------------------------------------------------------------------
   lsm/       Engine: memtable, flush, compaction, manifest
   sstable/   sorted files with an index and a bloom filter
   memtable/  a skip list in memory
   wal/       write-ahead log: records with checksums, recovery
   fs/        the disk: a real one, or a fake one that can crash
```

Two rules hold the whole thing together:

1. **Nothing below `net/` does any I/O on its own.** Raft and the KVNode have
   no clock, no sockets and no threads. Someone calls `tick()`, `step()` and
   `take_messages()`. In a real node that someone is `NodeRunner`
   ([net.md](net.md)). In the simulator it is a fake world with a fake clock
   ([simulator.md](simulator.md)). The same code runs in both, which is why
   the simulator tests the real thing.
2. **Every file goes through `fs/`.** On the real disk that is plain `os`
   calls. In tests it is `FakeFileSystem`, which can lose unsynced data,
   tear a write in half, or cut the power at any chosen step
   ([wal.md](wal.md)).

## A write, start to end

`SET greeting hello` from a Redis client, on a three-node cluster:

1. **Client port** ([net/server.py](../src/faultline/net/server.py)). The
   bytes are parsed as RESP and turned into `Put(b"greeting", b"hello")`,
   wrapped in a `Command` with this connection's client id and its next seq
   number.
2. **Raft, on the leader** ([raft.md](raft.md)). `propose()` appends the
   command to the leader's log, and the log saves it (a WAL record, then
   fsync) before anything else happens. The leader sends it to both
   followers in an `AppendRequest`, inside a frame on their TCP connection.
3. **Raft, on each follower.** It checks that its log matches the leader's
   up to the entry before, saves the new entry (fsync again), and answers
   yes with how far its log now matches.
4. **Commit.** One yes is enough: the leader plus one follower is a majority
   of three. The entry is committed, but only because it is from the
   leader's own term (the figure 8 rule, see [raft.md](raft.md)).
5. **Apply** ([kv.md](kv.md)). The state machine checks the session (has this
   client already sent this seq?), then writes the new value, the session
   and the applied index in **one** `WriteBatch` to the storage engine.
6. **Storage engine** ([lsm.md](lsm.md)). The batch goes to the engine's
   WAL (fsync), then into the memtable. When the memtable is full it is
   flushed to a sorted file ([sstable.md](sstable.md)). When there are
   enough of those, a later write merges them first (compaction). Nothing
   runs in the background.
7. **Answer.** The result goes back up to the client port, which sends
   `+OK`. The followers apply the same entry when they hear it is committed,
   with the next heartbeat.

Every 1000 applied entries, the state machine's data becomes a snapshot and
Raft drops its log up to there, so the log doesn't grow forever.

## A read, start to end

`GET greeting`:

1. The client port asks the node for a safe read.
2. **ReadIndex** ([raft.md](raft.md)). The leader notes its commit index and
   sends a round of heartbeats. When a majority answers that round, it knows
   it was still the leader when the read started, so nothing newer than its
   commit index can exist anywhere.
3. Once the state machine has applied up to that index, it reads the key:
   memtable first, then level 0 files newest first, then the one level 1
   file whose key range holds it. Each file's bloom filter can say "not here"
   without reading the file.
4. The value goes back to the client.

No disk write, so reads are much faster than writes
([benchmarks.md](benchmarks.md)).

## After a crash

A node that starts again rebuilds everything from its disk:

* **Raft:** the saved term and vote, the log (replayed from its WAL, a torn
  last record ignored), and the latest snapshot.
* **Storage engine:** the manifest says which sorted files are live, and the
  WAL is replayed into a fresh memtable.
* **State machine:** its applied index was saved with the data, so entries
  at or below it are skipped when Raft hands out the log again.

Then it rejoins. If it missed too much, the leader sends it a snapshot
instead of the entries.

## How each layer is tested

| Layer | How |
|---|---|
| wal, sstable, lsm | power cut at every step of a write, flush, compaction; every bit flipped in a file |
| raft | scripted scenarios from the paper (figure 8 among them), random chaos on a test network |
| kv | end to end on the test network, power cuts during apply, snapshots |
| the whole thing, simulated | the fault simulator: thousands of seeded runs with crashes, partitions, clock skew, power cuts, then the linearizability checker ([checker.md](checker.md)) |
| the whole thing, for real | real processes over TCP, kill -9 and SIGSTOP, chaos runs with the checker, Docker in CI ([net.md](net.md)) |
| the tests themselves | bugs put in on purpose, one at a time, to see that some test fails ([BUGS.md](../BUGS.md)) |
