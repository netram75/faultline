import random
from pathlib import Path

import pytest

from faultline.fs.fake import FakeFileSystem
from faultline.fs.real import OSFileSystem
from faultline.lsm.batch import WriteBatch
from faultline.lsm.engine import Engine, Options

ROOT = "/db"


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


def test_works_on_the_real_disk(tmp_path: Path) -> None:
    root = str(tmp_path / "db")
    engine = Engine.open(OSFileSystem(), root)
    engine.put(b"k", b"v")
    engine.close()

    engine = Engine.open(OSFileSystem(), root)

    assert engine.get(b"k") == b"v"
    engine.close()


@pytest.mark.parametrize("seed", range(10))
def test_matches_a_dict(seed: int) -> None:
    rng = random.Random(seed)
    fs = FakeFileSystem(seed)
    # Small segments, so the log rolls over many times
    options = Options(wal_segment_size=200)
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
            engine.close()
            engine = Engine.open(fs, ROOT, options)

    for k in range(40):
        assert engine.get(bytes([k])) == model.get(bytes([k]))
