import random

import pytest

from faultline.entry import TOMBSTONE, Tombstone, Value
from faultline.fs.fake import FakeFileSystem
from faultline.lsm.merge import merge
from faultline.memtable import Memtable
from faultline.sstable.reader import TableReader
from faultline.sstable.writer import TableWriter

type Entries = list[tuple[bytes, Value]]


def test_interleaves_sources_in_key_order() -> None:
    a: Entries = [(b"a", b"1"), (b"c", b"3")]
    b: Entries = [(b"b", b"2"), (b"d", b"4")]

    assert list(merge([a, b])) == [(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")]


def test_newest_copy_of_a_key_wins() -> None:
    newest: Entries = [(b"k", b"new")]
    middle: Entries = [(b"k", b"mid")]
    oldest: Entries = [(b"k", b"old")]

    assert list(merge([newest, middle, oldest])) == [(b"k", b"new")]
    assert list(merge([oldest, middle, newest])) == [(b"k", b"old")]


def test_tombstone_hides_older_value() -> None:
    newer: Entries = [(b"k", TOMBSTONE)]
    older: Entries = [(b"j", b"keep"), (b"k", b"gone")]

    assert list(merge([newer, older])) == [(b"j", b"keep"), (b"k", TOMBSTONE)]
    assert list(merge([newer, older], drop_tombstones=True)) == [(b"j", b"keep")]


def test_no_sources_or_empty_sources() -> None:
    assert list(merge([])) == []
    assert list(merge([[], []])) == []


@pytest.mark.parametrize("seed", range(30))
def test_matches_applying_sources_oldest_to_newest(seed: int) -> None:
    rng = random.Random(seed)
    sources: list[dict[bytes, Value]] = []
    for _ in range(rng.randint(1, 6)):
        source: dict[bytes, Value] = {}
        for _ in range(rng.randint(0, 40)):
            key = bytes([rng.randrange(30)])
            source[key] = TOMBSTONE if rng.random() < 0.3 else rng.randbytes(3)
        sources.append(source)

    # The slow, obviously right answer: start from the oldest source and let
    # each newer one overwrite it
    expected: dict[bytes, Value] = {}
    for source in reversed(sources):
        expected.update(source)

    merged = list(merge([sorted(s.items()) for s in sources]))
    assert merged == sorted(expected.items())

    live = list(merge([sorted(s.items()) for s in sources], drop_tombstones=True))
    assert live == [(k, v) for k, v in sorted(expected.items()) if not isinstance(v, Tombstone)]


def test_memtable_over_an_sstable() -> None:
    # The real use: fresh writes in memory sitting on top of a flushed table
    fs = FakeFileSystem()
    fs.mkdir("/data")
    writer = TableWriter(fs, "/data/00000001.sst")
    for key, value in [(b"a", b"old"), (b"b", b"old"), (b"c", b"old")]:
        writer.add(key, value)
    writer.finish()

    mt = Memtable()
    mt.put(b"a", b"new")
    mt.delete(b"b")
    mt.put(b"d", b"new")

    table = TableReader(fs, "/data/00000001.sst")
    result = list(merge([mt, table], drop_tombstones=True))

    assert result == [(b"a", b"new"), (b"c", b"old"), (b"d", b"new")]
