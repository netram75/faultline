# The storage engine

`Engine` is a key-value store on one machine's disk. It is an LSM tree
(log-structured merge tree), built from the parts in [wal.md](wal.md) and
[sstable.md](sstable.md). Later, every Raft node keeps its copy of the data in
one of these.

```python
engine = Engine.open(fs, "/data/node1")
engine.put(b"name", b"faultline")
engine.write(WriteBatch().put(b"a", b"1").delete(b"b"))
engine.get(b"name")  # b"faultline"
engine.scan(b"a", b"m")  # [(key, value), ...] for keys a <= key < m
engine.close()
```

## Files in the folder

```
MANIFEST          which tables are live, plus two counters
wal/00000004.wal  the write-ahead log
00000007.sst      tables
00000012.sst
```

The folder can hold files that are not part of the database: a table that
was half written when the power went, or an old one compaction replaced but
did not get to delete. So the engine never trusts the folder. **A table is
live only if the manifest lists it.**

The manifest is small, so it is rewritten whole on every change: written to
`MANIFEST.tmp`, synced, renamed over `MANIFEST`, then the folder is synced.
A rename is atomic, so after a crash we see the old manifest or the new one,
never a mix.

## Writing

A write is a **batch**: one or more puts and deletes. The batch becomes one
WAL record, and a record comes back whole or not at all, so a batch is
atomic. `put` and `delete` are batches of one.

1. Append the batch to the WAL.
2. Sync the WAL.
3. Apply it to the memtable.

Step 3 comes after step 2 on purpose. If the memtable saw the write first, a
reader could see it, and then a crash could take it back.

## Flush

When the memtable is full it becomes a table:

1. Start a new WAL segment. The old segments now hold exactly this memtable.
2. Write the memtable to a new table, tombstones included (an older table may
   still have the key, and the tombstone has to keep hiding it).
3. Save a manifest that lists the new table and says "replay the WAL from the
   new segment".
4. Delete the old segments.

What a crash does at each point:

| Crash after | On restart |
|---|---|
| 1 or 2 | The table is not in the manifest, so it is deleted. The old segments are still there and get replayed. Nothing lost. |
| 3 | The manifest has the table. The old segments are below the replay point, so they are deleted and not replayed. |
| 4 | Done. |

The one rule behind all of this, for flush and compaction alike:
**nothing is deleted until the manifest that makes it unneeded is on disk.**

## Two levels

```
memtable   newest
level 0    tables straight from the memtable, may overlap, newest first
level 1    the bottom: tables never overlap, sorted by key
```

**get** checks the memtable, then level 0 newest first, then the one level 1
table whose key range holds the key (a binary search on the tables' largest
keys). The first place that knows the key wins, even if what it knows is a
tombstone.

**scan** merges all of them (see the merge in [sstable.md](sstable.md)) and
skips tombstones. It returns a list, not an iterator, so a flush in the
middle of using the result can't pull the memtable out from under it.

## Compaction

Every flush adds a level 0 table, and every level 0 table is one more place
a read has to look. When level 0 has `l0_limit` tables (4 by default), we
merge all of level 0, plus the level 1 tables whose range overlaps it, into
new level 1 tables of about `table_size` bytes each.

Tombstones are dropped here. That is only safe because level 1 is the bottom
level: the merge already used each tombstone to hide older copies of its key,
and every older copy that could exist was part of the merge. So a delete
finally frees disk space at compaction time, not before.

Same rule as flush: write the new tables, save the manifest, and only then
delete the old ones. A crash before the save leaves the new tables as
orphans, which the next open deletes.

## Opening

1. Load the manifest (none means a new, empty engine).
2. Delete tables the manifest doesn't list, and WAL segments below its
   replay point. These are leftovers from a crash.
3. Open the live tables.
4. Replay the WAL into a fresh memtable.

Opening can be cut short by a crash too. Every step can simply run again.

## How it is tested

- **Against a dict.** Random puts, deletes, closes and crashes with tiny sizes,
  so it flushes and compacts all the time. `get` and `scan` must match the
  dict.
- **Power cut at every step.** The fake disk can cut the power at any chosen
  step (each write, sync, rename, delete...). One workload of 40 batches
  takes 373 steps, with several flushes and compactions. We run it 746 times:
  a cut at each step, in both crash modes below. After each one we reopen and
  check that every batch that returned is there, the one that was running is
  there fully or not at all, and nothing else changed.
- **Random power cuts.** 40 seeds, 6 cuts each, some of them during recovery
  itself.
- **Both kinds of crash.** A change to a folder that was never synced may be
  lost, or the disk may have written it out by itself just before the power
  went. Each hides different bugs, so the crash tests try both.

To check that the tests catch real bugs, we broke the engine on purpose, one
way at a time:

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

All ten were caught.

## Known limits

- **Only two levels.** Each compaction rewrites every level 1 table it
  overlaps, and with random keys that is most of them. So the cost of a
  compaction grows with the size of the database. LevelDB uses 7 levels, each
  10 times bigger than the last, to keep that in check.
- **Flush and compaction run inside the write that triggers them.** That
  write is slow. A real engine does them on a background thread.
- **One thread.** No locks, so one caller at a time.
- **Every batch pays for its own sync.** Group commit across callers needs
  the background thread too.
- **No snapshots.** A read sees the latest value. There are no sequence
  numbers yet to read the database as it was at some earlier point.
- **No block cache.** Every lookup that passes the bloom filter reads a block
  from disk.
