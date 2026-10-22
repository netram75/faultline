# The write-ahead log

## Why it exists

A database keeps recent changes in memory because memory is fast. But memory
is gone the moment the process dies. So before a change goes anywhere else,
we append it to a file on disk: the write-ahead log, or WAL.

On restart we read the log back and replay it. As long as a change made it
into the log and was synced, it is not lost.

Appending to the end of one file is also the cheapest kind of disk write,
which is why almost every database (Postgres, SQLite, RocksDB, etcd) has one.

## What a record looks like

```
+------------+------------+--------------------+
|  checksum  |   length   |      payload       |
|  4 bytes   |  4 bytes   |   `length` bytes   |
+------------+------------+--------------------+
```

The checksum is CRC32 over the length and the payload. When we read a record
back there are three possible results:

| What we see | What it means |
|---|---|
| Checksum matches | Good record |
| File ends before the record does | Torn record. A crash hit mid-write |
| Checksum does not match | Corrupt record. The bytes are wrong |

A length over 16 MB counts as corrupt. We never write records that big, so
the length field itself must be broken.

## Segments

The log is split into numbered files: `00000001.wal`, `00000002.wal` and so
on. New records go to the newest one. When it passes the size limit we start
the next. Later on this lets us delete old parts of the log in one go, once
their data is stored somewhere else.

## Making data safe: sync

Writing to a file does not put the data on disk. The OS keeps it in memory
for a while. Only `sync()` (fsync) waits until it is really stored.

Two things are easy to miss:

1. **A new file's name needs its own sync.** Syncing the file saves its
   bytes, but the name lives in the folder. Until the folder is synced, a
   crash can make the whole file disappear. So every new segment is followed
   by `sync_dir()`.
2. **On macOS, fsync is not enough.** It hands the data to the drive, which
   can still lose it from its cache. We use `F_FULLFSYNC` there.

`append()` does not sync. The caller decides when. Writing a batch of
records and then syncing once is called group commit, and it is much faster
than one sync per record.

## Recovery

When the WAL is opened, we read every segment from oldest to newest.

The rule that makes this simple: **before starting a new segment, we sync the
old one.** So only the newest segment can end in a half-written record.

- Newest segment ends in a bad record: that is a normal crash. We cut the
  file back to the last good record and continue writing from there.
- Any other segment has a bad record: that cannot come from a crash. It is
  real damage, so we raise `CorruptWal` and stop. Guessing here could
  silently drop data.

Why cut the tail instead of just skipping it? If we kept the garbage and
appended after it, the next recovery would stop at the garbage and lose
everything written after it.

## How it is tested

`tests/wal/test_crash.py` runs 300 seeded runs on the fake disk
(`faultline/fs/fake.py`). Each run appends random records, syncs at random
points and crashes several times. A crash keeps only what was synced, and
unsynced data at the end of a file can be lost, kept, or torn partway.

After every crash it checks two things:

1. Every synced record came back, in order.
2. Nothing came back that was never written.

To make sure the test can actually catch bugs, we broke the code on purpose
three ways and ran it:

| What we removed | Seeds that failed (of 300) |
|---|---|
| Cutting off the torn tail during recovery | 179 |
| Syncing the old segment before starting a new one | 217 |
| Syncing the folder after creating a segment | 233 |

Any failing seed can be rerun alone and fails the same way every time.

## Known limits

- If a record in the middle of the newest segment gets damaged (bit rot, not
  a crash), recovery treats it like a torn tail and cuts everything after it.
  Telling the two apart would mean checking whether good records follow the
  bad one. Not done yet.
- The fake disk only models torn writes that keep a prefix of the data. Real
  disks can also leave zeros or write sectors out of order.
