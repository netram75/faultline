"""Crash tests for the storage engine.

The sizes are tiny, so the memtable flushes and level 0 compacts all the
time. We cut the power at some step, open the engine again on the same fake
disk, and check what it holds against a plain dict:

* every batch that returned must be there
* the batch that was running when the power went is there fully or not at all
* nothing else changed
"""

import random

import pytest

from faultline.entry import Tombstone
from faultline.fs.fake import FakeFileSystem, PowerCut
from faultline.lsm import manifest
from faultline.lsm.batch import WriteBatch
from faultline.lsm.engine import Engine, Options

ROOT = "/db"
OPTIONS = Options(memtable_size=300, l0_limit=3, table_size=200, wal_segment_size=150)
KEYS = [bytes([i]) for i in range(30)]

type Model = dict[bytes, bytes]


def random_batch(rng: random.Random) -> WriteBatch:
    batch = WriteBatch()
    for _ in range(rng.randint(1, 4)):
        key = rng.choice(KEYS)
        if rng.random() < 0.3:
            batch.delete(key)
        else:
            batch.put(key, rng.randbytes(rng.randint(0, 12)))
    return batch


def applied(model: Model, batch: WriteBatch) -> Model:
    after = dict(model)
    for key, value in batch:
        if isinstance(value, Tombstone):
            after.pop(key, None)
        else:
            after[key] = value
    return after


def recover(fs: FakeFileSystem) -> Model:
    """Open the engine with no cut planned and read everything in it."""
    engine = Engine.open(fs, ROOT, OPTIONS)
    found = dict(engine.scan())
    # get() takes a different path through the tables than scan(), so
    # check that it agrees for every key
    for key in KEYS:
        assert engine.get(key) == found.get(key)
    engine.close()
    return found


@pytest.mark.parametrize("seed", range(40))
def test_power_cut_at_a_random_step(seed: int) -> None:
    rng = random.Random(seed)
    fs = FakeFileSystem(seed)
    model: Model = {}

    for _ in range(6):
        # Opening takes only a few steps, so a small number now and then
        # makes the cut land in the middle of recovery itself
        steps = rng.randint(0, 5) if rng.random() < 0.2 else rng.randint(0, 300)
        fs.cut_power_after(steps, keep_names=rng.random() < 0.5)
        pending = model
        try:
            engine = Engine.open(fs, ROOT, OPTIONS)
            while True:
                batch = random_batch(rng)
                pending = applied(model, batch)
                engine.write(batch)
                model = pending
        except PowerCut:
            pass

        found = recover(fs)
        assert found in (model, pending)
        model = found


def test_power_cut_at_every_step_of_flushes_and_compactions() -> None:
    rng = random.Random(1)
    batches = [random_batch(rng) for _ in range(40)]

    # One run with no cut, to count the steps and check that the workload
    # really does flush and compact
    fs = FakeFileSystem()
    engine = Engine.open(fs, ROOT, OPTIONS)
    for batch in batches:
        engine.write(batch)
    total_steps = fs.steps
    state = manifest.load(fs, ROOT)
    assert state is not None
    assert any(info.level == 1 for info in state.tables)

    # Both ways a crash can treat names that were never synced, at every step
    for n in range(total_steps * 2):
        fs = FakeFileSystem(n)
        fs.cut_power_after(n // 2, keep_names=n % 2 == 1)
        done = 0
        with pytest.raises(PowerCut):
            engine = Engine.open(fs, ROOT, OPTIONS)
            for batch in batches:
                engine.write(batch)
                done += 1

        before: Model = {}
        for batch in batches[:done]:
            before = applied(before, batch)
        after = applied(before, batches[done]) if done < len(batches) else before
        assert recover(fs) in (before, after)


def test_failed_write_is_never_visible() -> None:
    fs = FakeFileSystem()
    engine = Engine.open(fs, ROOT)
    engine.put(b"k", b"old")

    fs.cut_power_after(1)  # the WAL write goes through, the sync is cut
    with pytest.raises(PowerCut):
        engine.put(b"k", b"new")

    # The caller was told the write failed, so nobody may ever read it
    assert engine.get(b"k") == b"old"


def test_wal_numbers_never_go_back_below_wal_start() -> None:
    # If the log folder ever ends up empty, new writes must not go to
    # segment 1. The manifest says segments below wal_start are already in
    # tables, so the next open would delete those writes.
    fs = FakeFileSystem()
    engine = Engine.open(fs, ROOT, Options(memtable_size=1))
    engine.put(b"a", b"1")  # flushed, so wal_start is now 2
    engine.close()
    for name in fs.list_dir(f"{ROOT}/wal"):
        fs.remove(f"{ROOT}/wal/{name}")

    engine = Engine.open(fs, ROOT)
    engine.put(b"b", b"2")
    engine.close()

    assert recover(fs) == {b"a": b"1", b"b": b"2"}
