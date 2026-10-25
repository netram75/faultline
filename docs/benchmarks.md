# Benchmarks

How fast faultline is, measured, with the reasons. Short version: reads are
fast, writes are slow, and the slow part is waiting for the disk, on
purpose.

## How it was measured

```
uv run python -m faultline.bench --seconds 10 --clients 1,8,32
```

[bench.py](../src/faultline/bench.py) starts three real `faultline serve`
processes on one machine, finds the leader, and runs 1, 8 or 32 connections
against it. Each connection sends one command, waits for the answer, sends
the next. 10 seconds per line. Values are 100 bytes, on 100 keys.

The machine (25 Oct 2026):

| | |
|---|---|
| CPU | Apple M5 Pro, 15 cores |
| Memory | 24 GB |
| Disk | built-in SSD, APFS |
| OS | macOS 26.5 |
| Python | 3.12.13 |
| Power | on battery |

All three nodes run on this one machine and share its CPU and its disk. A
real cluster has a disk per node, and a network with far more delay than
127.0.0.1. So these numbers say how faultline behaves, not what it would do
in a data center.

## Results

| Mode | Clients | Answers per second | p50 | p99 |
|---|---|---|---|---|
| writes (SET) | 1 | 52 | 19.8 ms | 27.3 ms |
| writes (SET) | 8 | 54 | 149.0 ms | 212.5 ms |
| writes (SET) | 32 | 57 | 563.7 ms | 823.6 ms |
| reads (GET) | 1 | 4,197 | 0.2 ms | 0.3 ms |
| reads (GET) | 8 | 20,940 | 0.4 ms | 0.6 ms |
| reads (GET) | 32 | 33,994 | 0.9 ms | 1.3 ms |
| half and half | 1 | 103 | 10.9 ms | 23.8 ms |
| half and half | 8 | 105 | 72.9 ms | 149.3 ms |
| half and half | 32 | 93 | 361.8 ms | 607.5 ms |

No errors in any run.

## Why writes are slow

An answer to a write means it is on disk on a majority of nodes. On macOS,
"on disk" means `F_FULLFSYNC` (see [wal.md](wal.md)): wait until the drive
has really written the data, not just put it in its own cache. On this
machine that takes about **3 ms** each time. A plain `fsync()`, which macOS
lets the drive answer from its cache, takes 0.02 ms, more than a hundred
times less, and is not safe against a power cut.

One write waits for several of those, one after another: the leader saves
the entry to its Raft log, a follower saves it to its log, and once that
makes a majority, the leader applies it to its storage engine, which saves
again. Add the trips between processes, and a write takes about 20 ms.

**More clients don't help**, because faultline saves each write on its own.
32 clients send 32 writes at once, and they still go to disk one by one, so
the total stays at about 55 a second while each client waits longer. The
usual fix is group commit: put every write that is waiting into one save, so
32 writes cost one 3 ms wait instead of 32. faultline doesn't do that yet
([limits.md](limits.md)).

## Why reads are fast

A GET never touches the disk. It is a safe read (ReadIndex, see
[raft.md](raft.md)): the leader checks with one round of heartbeats that a
majority still follows it, then reads its own memory. On one machine that
round is a fraction of a millisecond, and many reads can share it, so more
clients means more reads per second.

## Try it yourself

```
uv run python -m faultline.bench --modes writes --clients 1 --seconds 5
```

Expect different numbers on Linux: there `fsync()` does write through the
drive's cache, and how long that takes depends a lot on the drive.
