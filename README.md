# faultline

[![CI](https://github.com/netram75/faultline/actions/workflows/ci.yml/badge.svg)](https://github.com/netram75/faultline/actions/workflows/ci.yml)

A key-value database that keeps a copy of your data on several machines and
does not lose it when some of them crash. Built from scratch in Python, with
no dependencies outside the standard library.

Most of the work here is not the database itself. It is the part that tries
to break it: a simulator that crashes machines, cuts the network and damages
disk writes on purpose, real processes killed with `kill -9`, and a checker
that reads every answer clients got and proves whether a single copy of the
data could have given them.

## What is in it

- **Storage engine:** a write-ahead log, a skip list memtable, sorted files
  with bloom filters, an LSM tree with flush and compaction, crash recovery
  that is tested by cutting the power at every step ([lsm.md](docs/lsm.md))
- **Raft:** elections with pre-vote, log replication, the figure 8 commit
  rule, check quorum, snapshots, safe reads with ReadIndex
  ([raft.md](docs/raft.md))
- **The database on Raft:** client sessions so a retried write runs once,
  linearizable reads ([kv.md](docs/kv.md))
- **A server:** one process per node, TCP between nodes, the Redis protocol
  for clients, Prometheus metrics, Docker ([net.md](docs/net.md))
- **A fault simulator:** a whole cluster in one process on a fake clock,
  network and disks, fully repeatable from one seed
  ([simulator.md](docs/simulator.md))
- **A linearizability checker** with a search fast enough for long histories,
  that shows the few operations where a history goes wrong
  ([checker.md](docs/checker.md))
- **Chaos runs** on real processes: kill -9, SIGSTOP, then the checker

## Quick start

Needs Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```
uv sync
make check      # lint, type check, about 2000 tests

# three nodes, each in its own terminal
uv run faultline serve --id 1 --cluster examples/local-cluster.toml
uv run faultline serve --id 2 --cluster examples/local-cluster.toml
uv run faultline serve --id 3 --cluster examples/local-cluster.toml

redis-cli -p 6401 SET greeting hello
```

Then break it: `uv run python -m faultline.chaos --seconds 30`, or run the
simulator: `uv run python -m faultline.sim --seed 7`. The whole walk-through
is in [getting-started.md](docs/getting-started.md).

## How it is tested

| What | How |
|---|---|
| Crashes on one disk | a fake disk that loses unsynced data, tears writes, and cuts the power at any chosen step. Every step of every write, flush, compaction and apply. |
| Raft | the paper's scenarios played step by step (figure 8 among them), and random chaos on a test network |
| The whole cluster, simulated | crashes, partitions, clock skew, power cuts, lost and repeated messages, in one repeatable process. Nearly 200 runs on every push, 500 more every night. |
| The whole cluster, for real | three real processes: kill -9, SIGSTOP, full restarts, chaos runs. Docker in CI. |
| Every answer clients got | the linearizability checker, at the end of every simulated and chaos run |
| The tests themselves | bugs put in on purpose, one at a time, to see that a test fails |

The bug hunt on Day 8: 902 simulator runs, many of them harsh, and the
checker found no answer that one copy of the data couldn't have given. The
tests did catch three bugs switched on for the purpose (a write applied
twice, a write answered before it was committed, a read served by a leader
that was no longer the leader), and close to 70 more put in by hand across
the code. A few were missed, and [BUGS.md](BUGS.md) lists those too, along
with the real bugs found while building it.

## How fast

Reads: up to about 34,000 a second. Writes: about 55 a second, because each
one waits for the disk on two nodes, and faultline doesn't yet group writes
into one save. Measured on one Mac, all three nodes on one machine
([benchmarks.md](docs/benchmarks.md)).

## Docs

| | |
|---|---|
| [architecture.md](docs/architecture.md) | the map: the layers, and one write and one read through all of them |
| [getting-started.md](docs/getting-started.md) | run a cluster, talk to it, break it |
| [wal.md](docs/wal.md), [sstable.md](docs/sstable.md), [lsm.md](docs/lsm.md) | the storage engine |
| [raft.md](docs/raft.md), [kv.md](docs/kv.md) | replication and the database on top |
| [simulator.md](docs/simulator.md), [checker.md](docs/checker.md) | the fault simulator and the linearizability checker |
| [net.md](docs/net.md) | the server, the Redis protocol, metrics |
| [benchmarks.md](docs/benchmarks.md) | numbers, and why they are what they are |
| [limits.md](docs/limits.md) | what it does not do, and what the tests can't show |
| [BUGS.md](BUGS.md) | bugs found, and bugs put in on purpose |

## Layout

```
src/faultline/
  fs/        file access: the real disk, and a fake disk that can crash
             or lose power at any chosen step
  wal/       write-ahead log: record format, writer, recovery
  memtable/  new writes in memory: skip list and memtable
  sstable/   sorted files on disk: blocks, bloom filter, writer, reader
  lsm/       the storage engine: batches, manifest, flush, compaction
  raft/      Raft: messages, saved term and vote, the log on disk, the node,
             what the leader knows about each follower, safe reads, snapshots
  kv/        the database: commands, the state machine, one node of it
  sim/       the fault simulator: clock, network, nodes, clients, nemesis,
             safety checks, known bugs to switch on (python -m faultline.sim)
  checker/   the linearizability checker
  net/       the real server: Redis protocol, TCP between nodes, metrics,
             the cluster file, real processes on one machine
  chaos/     break a real cluster with kill -9 and SIGSTOP, then check it
             (python -m faultline.chaos)
  cli.py     the faultline command (faultline serve)
  bench.py   the benchmark (python -m faultline.bench)
  entry.py   the tombstone that marks a deleted key
tests/       one folder per package, same names
docs/        how each part works and why
examples/    a cluster file for three nodes on one machine
docker/      the cluster file for Docker, and a smoke test
```

## License

MIT
