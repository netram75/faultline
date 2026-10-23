import random
from pathlib import Path

import pytest

from faultline.fs.fake import FakeFileSystem
from faultline.fs.real import OSFileSystem
from faultline.lsm.batch import WriteBatch
from faultline.lsm.engine import Engine, Options
from faultline.wal import record

ROOT = "/db"
# Flush after every write
TINY = Options(memtable_size=1)


def table_files(fs: FakeFileSystem) -> list[str]:
    return [name for name in fs.list_dir(ROOT) if name.endswith(".sst")]


def test_put_get_delete() -> None:
    engine = Engine.open(FakeFileSystem(), ROOT)

    assert engine.get(b"k") is None
    engine.put(b"k", b"v1")
    assert engine.get(b"k") == b"v1"
    engine.put(b"k", b"v2")
    assert engine.get(b"k") == b"v2"
    engine.delete(b"k")
    assert engine.get(b"k") is None


def test_write_a_batch() -> None:
    engine = Engine.open(FakeFileSystem(), ROOT)
    engine.put(b"old", b"x")

    engine.write(WriteBatch().put(b"a", b"1").put(b"b", b"2").delete(b"old"))

    assert engine.get(b"a") == b"1"
    assert engine.get(b"b") == b"2"
    assert engine.get(b"old") is None


def test_empty_batch_writes_nothing() -> None:
    fs = FakeFileSystem()
    engine = Engine.open(fs, ROOT)

    engine.write(WriteBatch())

    assert fs.read_file(f"{ROOT}/wal/00000001.wal") == b""


def test_reopen_after_close() -> None:
    fs = FakeFileSystem()
    engine = Engine.open(fs, ROOT)
    engine.put(b"a", b"1")
    engine.delete(b"a")
    engine.put(b"b", b"2")
    engine.close()

    engine = Engine.open(fs, ROOT)

    assert engine.get(b"a") is None
    assert engine.get(b"b") == b"2"


def test_every_write_survives_a_crash() -> None:
    fs = FakeFileSystem()
    engine = Engine.open(fs, ROOT)
    for i in range(100):
        engine.put(f"key{i}".encode(), f"value{i}".encode())

    fs.crash()
    engine = Engine.open(fs, ROOT)

    for i in range(100):
        assert engine.get(f"key{i}".encode()) == f"value{i}".encode()


def test_full_memtable_is_flushed_to_a_table() -> None:
    fs = FakeFileSystem()
    engine = Engine.open(fs, ROOT, Options(memtable_size=1024))

    for i in range(100):
        engine.put(f"key{i:03d}".encode(), b"v" * 20)

    assert len(table_files(fs)) >= 5
    # The segments whose writes are now in tables are gone
    assert len(fs.list_dir(f"{ROOT}/wal")) == 1
    for i in range(100):
        assert engine.get(f"key{i:03d}".encode()) == b"v" * 20


def test_tombstone_in_a_newer_table_hides_an_older_one() -> None:
    fs = FakeFileSystem()
    engine = Engine.open(fs, ROOT, TINY)
    engine.put(b"k", b"old")
    engine.delete(b"k")

    assert len(table_files(fs)) == 2
    assert engine.get(b"k") is None
    engine.close()
    assert Engine.open(fs, ROOT).get(b"k") is None


def test_reopen_reads_tables_and_the_wal() -> None:
    fs = FakeFileSystem()
    engine = Engine.open(fs, ROOT, TINY)
    engine.put(b"in a table", b"1")
    engine.close()
    engine = Engine.open(fs, ROOT)  # big memtable, so this one stays in the WAL
    engine.put(b"in the wal", b"2")
    engine.close()

    engine = Engine.open(fs, ROOT)

    assert engine.get(b"in a table") == b"1"
    assert engine.get(b"in the wal") == b"2"


def test_table_not_in_the_manifest_is_removed_at_open() -> None:
    # What a crash in the middle of a flush leaves behind
    fs = FakeFileSystem()
    fs.mkdir(ROOT)
    f = fs.open_append(f"{ROOT}/00000001.sst")
    f.write(b"half a table")
    f.close()

    engine = Engine.open(fs, ROOT, TINY)
    engine.put(b"k", b"v")  # its table reuses number 1

    assert table_files(fs) == ["00000001.sst"]
    assert engine.get(b"k") == b"v"


def test_flushed_segments_are_not_replayed() -> None:
    fs = FakeFileSystem()
    engine = Engine.open(fs, ROOT, TINY)
    engine.put(b"k", b"new")  # flushed, so segment 1 is deleted
    engine.close()
    # Bring segment 1 back with an old write in it, as if a crash undid the
    # delete. Replaying it would put the old value on top of the new one.
    f = fs.open_append(f"{ROOT}/wal/00000001.wal")
    f.write(record.encode(WriteBatch().put(b"k", b"old").encode()))
    f.close()

    engine = Engine.open(fs, ROOT)

    assert engine.get(b"k") == b"new"
    assert "00000001.wal" not in fs.list_dir(f"{ROOT}/wal")


def test_works_on_the_real_disk(tmp_path: Path) -> None:
    root = str(tmp_path / "db")
    options = Options(memtable_size=1024)
    engine = Engine.open(OSFileSystem(), root, options)
    for i in range(100):
        engine.put(f"key{i:03d}".encode(), f"value{i}".encode())
    engine.close()

    engine = Engine.open(OSFileSystem(), root, options)

    for i in range(100):
        assert engine.get(f"key{i:03d}".encode()) == f"value{i}".encode()
    engine.close()


@pytest.mark.parametrize("seed", range(10))
def test_matches_a_dict(seed: int) -> None:
    rng = random.Random(seed)
    fs = FakeFileSystem(seed)
    # Small sizes, so the log rolls over and the memtable flushes many times
    options = Options(memtable_size=500, wal_segment_size=200)
    engine = Engine.open(fs, ROOT, options)
    model: dict[bytes, bytes] = {}

    for step in range(500):
        key = bytes([rng.randrange(40)])
        if rng.random() < 0.3:
            engine.delete(key)
            model.pop(key, None)
        else:
            value = rng.randbytes(rng.randint(0, 20))
            engine.put(key, value)
            model[key] = value
        if step % 100 == 99:
            # Every write was synced, so a crash loses nothing
            if rng.random() < 0.5:
                engine.close()
            else:
                fs.crash()
            engine = Engine.open(fs, ROOT, options)

    for k in range(40):
        assert engine.get(bytes([k])) == model.get(bytes([k]))
