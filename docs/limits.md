# Design choices and known limits

faultline was built to learn how a replicated database stays correct, and
to test that hard. Some things were left out on purpose, and some are
simply not done yet. This page lists them, so nobody has to find
out the hard way.

## Things it does not do

* **No changing the cluster while it runs.** The cluster file is fixed.
  Adding or removing a node means stopping everything, changing the file
  everywhere and starting again. Raft's membership changes (the paper,
  section 6) are not built.
* **One key at a time.** Every command reads or writes a single key. There
  are no transactions and nothing that changes two keys together.
* **Reads only on the leader.** Followers send every request to the leader.
  The leader serves reads with ReadIndex, one heartbeat round each (shared
  by reads that arrive together). No leases, no follower reads.
* **No security.** No passwords, no TLS. Anyone who can reach a port can
  read and write everything. Only run it where you trust the network.
* **A small part of Redis.** PING, GET, SET, DEL, SETNX, INFO, and CAS,
  which Redis doesn't have. No expiry, no other data types, and `DEL`
  always answers `:1`, because faultline does not track whether the key was
  there.

## Things that are simple on purpose

* **Two levels in the storage engine.** Level 0 holds flushed files, level 1
  holds everything else in non-overlapping files. A compaction merges all of
  level 0 with the level 1 files it overlaps. With much more data than
  memory, that rewrites a lot of data again and again. Real LSM engines have
  more levels ([lsm.md](lsm.md)).
* **Compaction runs inside a write.** No background threads, so the write
  that fills level 0 waits for the whole compaction.
* **One write, one fsync.** Concurrent writes are not grouped into one save
  (group commit), so writes top out around 55 a second on the Mac it was
  measured on, however many clients there are ([benchmarks.md](benchmarks.md)).
* **One thread per node.** Python, one asyncio loop, and every fsync blocks
  it while it runs. Simple to reason about, not fast.
* **Snapshots are one piece.** A snapshot is all the data in memory at
  once, and goes to a follower in one message. Messages between nodes are
  capped at 64 MiB ([net/frames.py](../src/faultline/net/frames.py)), so with
  more data than that, a follower that fell far behind can't catch up.
  Real databases send snapshots in chunks.

## Sessions grow forever

Every client connection is a session of its own, with a random client id
([net.md](net.md#sessions)), so that a retry inside it runs only once. Each
session that wrote something leaves a small record in the state machine:
its last sequence number and that result. **Those records are never
removed.** So the table grows with the number of connections that ever
wrote, for as long as the cluster lives, and it is part of every snapshot.

The fix is known: expire sessions that have been quiet for a while, decided
through the log so every node expires the same ones at the same point (the
Raft thesis, section 6.3). It isn't built.

Also: a client that reconnects is a new session, so a write sent again on a
new connection after a TIMEOUT can happen twice. Real Redis has the same
problem.

## What the testing can and can't show

* **The checker proves a negative.** When it says a history is not
  linearizable, that is certain, and it shows why. When it says "ok", that
  history was fine, nothing more. Some histories are too big to decide in
  its step budget, and those get "unknown" ([checker.md](checker.md)).
* **The simulator misses the figure 8 bug.** Breaking the Raft rule from the
  paper's figure 8 needs a long, exact sequence of leaders, and thousands of
  random runs never produced it. A scripted test catches it
  ([BUGS.md](../BUGS.md)).
* **Some bugs show up rarely.** The `leader-read` bug (a leader serving
  reads without ReadIndex) is caught in 2 to 30 of 100 simulator runs,
  depending on the run length. Short runs rarely set up the stale leader it
  needs.
* **Real processes can't force every moment.** The test that freezes the
  leader with SIGSTOP checks it never answers with old data, but in practice
  the woken node reads the new leader's messages first and steps down. So
  that test would pass even with the `leader-read` bug. The simulator is
  what catches it.
* **Chaos runs break one node at a time** with kill -9 and SIGSTOP. They
  can't cut the network between processes (that needs root and iptables),
  and they are not repeatable. The simulator covers network partitions.
* **Some parts only run in CI.** The Docker image and compose file were
  written on a machine without Docker. CI builds and runs them on every
  push. `redis-cli` was not used in any test either: the tests use their own
  small client ([net/client.py](../src/faultline/net/client.py)).
* **Durability depends on the disk telling the truth.** On macOS faultline
  uses `F_FULLFSYNC`. On Linux it uses `fsync()`, which is only as good as
  the file system and drive underneath.
