# Changelog

Notable changes to faultline, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-10-25

The first release: a key-value store replicated with Raft, on its own
storage engine, with a fault simulator and a linearizability checker to
test it, and a real server that speaks the Redis protocol.

### Added

- **Storage engine.** A write-ahead log with checksums that recovers from a
  torn last write. A fake disk that loses unsynced data and can cut the
  power at any chosen step. A skip list memtable, sorted files with an index
  and a bloom filter, and an LSM tree (two levels) with write batches, a
  manifest, flush and compaction.
- **Raft.** Elections with pre-vote, log replication with fast backup, the
  figure 8 commit rule, check quorum, safe reads with ReadIndex, snapshots
  sent with InstallSnapshot, and log compaction.
- **The database.** Put, delete and compare-and-set, client sessions so a
  retried write runs once, and an apply that writes the change, the session
  and the applied index in one atomic batch.
- **Fault simulator** (`python -m faultline.sim`). A whole cluster in one
  process, repeatable from one seed: lost, late and repeated messages,
  partitions, clock skew, crashes, power cuts in the middle of a write,
  reboot storms, and safety checks after every step.
- **Linearizability checker.** The Wing and Gong search with Lowe's memo,
  made fast enough for long histories, checked key by key, with a step
  budget, an explanation of the first answer that can't be explained, and
  tests that it agrees with brute force. Known bugs can be switched on in
  the simulator (`--bug`) to watch it catch them.
- **Server** (`faultline serve`). One process per node, TCP between nodes,
  the Redis protocol for clients (PING, GET, SET, DEL, SETNX, CAS, INFO),
  clear errors (NOTLEADER, TRYAGAIN, TIMEOUT), a cluster file, metrics for
  Prometheus, and a Docker image with a three-node compose file.
- **Chaos runs** (`python -m faultline.chaos`): real processes, kill -9 and
  SIGSTOP, clients that write down every answer, and the checker at the end.
- **Benchmark** (`python -m faultline.bench`).
- **CI:** lint, strict type checks and about 2000 tests on Linux and macOS,
  Python 3.12 and 3.13. A Docker smoke test on every push. 500 simulator runs
  and a chaos run every night. Releases built from version tags.

### How it was built

| Date | Part |
|---|---|
| 22 Oct 2026 | write-ahead log, fake disk; memtable, sorted files, bloom filters |
| 23 Oct 2026 | the storage engine |
| 24 Oct 2026 | Raft elections and replication; the database on Raft; the fault simulator |
| 25 Oct 2026 | the linearizability checker and a bug hunt; the server, Docker, chaos, metrics; docs and this release |

### Known limits

No adding or removing nodes while running, one key per command, writes
saved one at a time (about 55 a second on the Mac it was measured on),
client sessions that are never cleaned up, snapshots sent in one message of
at most 64 MiB, and no authentication. The full list, with what the tests
can't show, is in
[docs/limits.md](https://github.com/netram75/faultline/blob/main/docs/limits.md).
