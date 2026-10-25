# Memtable and SSTables

These are the two halves of an LSM tree (log-structured merge tree), the
storage design behind LevelDB, RocksDB, Cassandra and many others.

- **Memtable:** new writes, kept sorted in memory. Fast to write and read.
- **SSTable:** a sorted, read-only file on disk. When the memtable is full it
  is written out as one SSTable and a fresh memtable starts.

A write is: append to the WAL ([wal.md](wal.md)), then put in the memtable.
That's it. No random disk writes, which is why LSM trees are so good at
taking a lot of writes.

## Memtable

The memtable is a **skip list**: a sorted linked list with extra "express"
levels on top, so search and insert are O(log n) on average.

```
level 2:  head ---------------------------> 40 ------------------> None
level 1:  head ----------> 17 ------------> 40 ------> 63 -------> None
level 0:  head --> 5 ----> 17 --> 23 -----> 40 --> 51 -> 63 -> 70 > None
```

Each node's height is random (1 in 4 chance per extra level, like LevelDB),
so the list takes a seed. Same seed, same shape, repeatable tests.

**Deletes don't remove anything.** They store a **tombstone**, a marker that
says "this key is gone". Older copies of the key may sit in SSTables on disk.
If the delete just removed the key from memory, those old copies would come
back. The tombstone is newer, so it hides them.

## SSTable layout

```
+----------------+
| data block 0   |  sorted entries, about 4 KB per block
| data block 1   |
| ...            |
+----------------+
| bloom filter   |  covers every key in the file
+----------------+
| index block    |  last key of each data block -> (offset, size)
+----------------+
| footer         |  where the filter and index are, fixed 44 bytes, always last
+----------------+
```

Every part ends with a CRC32 checksum, and the footer ends with a magic
number (`FLTSST01`).

**Opening a table** reads the footer, then loads the index and the bloom
filter into memory.

**Looking up a key:**

1. Ask the bloom filter. If it says no, we're done. No disk read at all.
2. Binary search the index for the first block whose last key is >= the key.
   That's the only block the key can be in.
3. Read that one block, check its checksum, binary search inside it.

So a lookup costs at most one block read.

**Scanning** reads one block at a time, so a big table never sits in memory
all at once.

SSTables are written once, synced (file and folder), and never changed
again. No locks, no partial updates. Old ones get replaced by compaction
later (see [lsm.md](lsm.md)).

## Bloom filter

A row of bits. Each key is hashed k times and those k bits are set. To check
a key, hash it the same way: if any of its bits is 0, the key is definitely
not in the file. If all are 1, it **maybe** is.

- Two hashes are turned into k positions (Kirsch and Mitzenmacher), so we
  hash each key only once.
- We use blake2b, not Python's `hash()`. `hash()` on bytes changes every time
  Python starts, so a filter saved to disk would be useless after a restart.

Measured false positive rate, 10,000 keys in and 10,000 other keys checked:

| Bits per key | False positives |
|---|---|
| 4 | 14.8% |
| 10 (default) | 0.98% |
| 16 | 0.01% |

10 bits per key matches the textbook number (about 1%).

## Reading across sources: the merge

A key can be in the memtable and in several SSTables. Each is sorted, so we
merge them with a heap, like the merge step of merge sort. Sources are passed
**newest first**, and when a key shows up more than once the newest copy
wins.

Tombstones are kept in the merge unless the caller asks to drop them. That's
only safe when nothing older exists outside the merge, or when the result is
going to a user.

## How it is tested

- Skip list and merge are checked against a plain dict or a simple "apply
  oldest to newest" model, over many seeds.
- A random memtable is flushed and read back, for 25 seeds: every entry,
  every lookup and the full scan must match.
- **Every single bit** of a small table is flipped, one at a time, and every
  cut length is tried. Each damaged file must raise `CorruptTable`. None slip
  through, because every part of the file has a checksum.

## Known limits

- No key prefix compression inside blocks (LevelDB has it). Simpler to read,
  bigger files.
- One bloom filter per table, built from every key at the end, so writing a
  table keeps all its keys in memory. Fine for memtable-sized tables.
- No block cache yet. Every lookup that passes the bloom filter reads a block.
