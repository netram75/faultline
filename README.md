# faultline

A key-value database that keeps a copy of your data on several machines and
does not lose it when some of them crash. Built from scratch in Python.

Most of the work here is not the database itself. It is the part that tries to
break it: a simulator that crashes machines, cuts the network and damages disk
writes on purpose, then checks that every answer the database gave was correct.

> Work in progress. Started 22 Oct 2026.

## Plan

- [x] Write-ahead log that survives crashes ([how it works](docs/wal.md))
- [x] Memtable, sorted disk files (SSTables) and bloom filters ([how they work](docs/sstable.md))
- [ ] Storage engine (LSM tree): flush, compaction, crash recovery
- [ ] Raft: leader election and log replication
- [ ] Database on top of Raft: sessions, safe reads, snapshots
- [ ] Fault simulator: fake network, fake disk, fake clock, one seed per run
- [ ] Correctness checker (linearizability) and a bug hunt
- [ ] Real cluster: TCP, Redis protocol, Docker, chaos tests, metrics
- [ ] Docs and release

## Layout

```
src/faultline/
  fs/        file access: the real disk, and a fake disk that can crash
  wal/       write-ahead log: record format, writer, recovery
  memtable/  new writes in memory: skip list and memtable
  sstable/   sorted files on disk: blocks, bloom filter, writer, reader
  lsm/       putting it together: merging memtable and tables
  entry.py   the tombstone that marks a deleted key
tests/    one folder per package, same names
docs/     how each part works and why
```

## Running the checks

Needs Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```
uv sync
make check     # lint, type check, tests
```

## License

MIT
