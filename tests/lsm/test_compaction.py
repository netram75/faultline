import random
from itertools import pairwise

import pytest

from faultline.entry import TOMBSTONE, Tombstone, Value
from faultline.fs.fake import FakeFileSystem
from faultline.lsm.compaction import compact
from faultline.lsm.files import table_path
from faultline.lsm.manifest import TableInfo
from faultline.sstable.reader import TableReader
from faultline.sstable.writer import TableWriter

ROOT = "/db"

type Entries = list[tuple[bytes, Value]]


def new_fs() -> FakeFileSystem:
    fs = FakeFileSystem()
    fs.mkdir(ROOT)
    return fs


def make_table(fs: FakeFileSystem, number: int, entries: Entries) -> TableReader:
    writer = TableWriter(fs, table_path(ROOT, number))
    for key, value in entries:
        writer.add(key, value)
    writer.finish()
    return TableReader(fs, table_path(ROOT, number))


def read_outputs(fs: FakeFileSystem, outputs: list[TableInfo]) -> Entries:
    entries: Entries = []
    for info in outputs:
        entries += list(TableReader(fs, table_path(ROOT, info.number)))
    return entries


def test_newest_value_wins_and_tombstones_go() -> None:
    fs = new_fs()
    newer = make_table(fs, 2, [(b"a", b"new"), (b"b", TOMBSTONE)])
    older = make_table(fs, 1, [(b"a", b"old"), (b"b", b"old"), (b"c", b"old")])

    outputs = compact(fs, ROOT, [newer, older], first_number=3)

    assert read_outputs(fs, outputs) == [(b"a", b"new"), (b"c", b"old")]
    assert outputs == [TableInfo(3, 1, b"a", b"c", len(fs.read_file(table_path(ROOT, 3))))]


def test_big_output_is_split_into_tables_that_do_not_overlap() -> None:
    fs = new_fs()
    table = make_table(fs, 1, [(f"key{i:04d}".encode(), b"v" * 100) for i in range(1000)])

    outputs = compact(fs, ROOT, [table], first_number=2, table_size=10_000)

    assert len(outputs) > 5
    assert [info.number for info in outputs] == list(range(2, 2 + len(outputs)))
    for before, after in pairwise(outputs):
        assert before.largest < after.smallest
    assert len(read_outputs(fs, outputs)) == 1000


def test_nothing_left_means_no_outputs() -> None:
    fs = new_fs()
    newer = make_table(fs, 2, [(b"a", TOMBSTONE)])
    older = make_table(fs, 1, [(b"a", b"old")])

    assert compact(fs, ROOT, [newer, older], first_number=3) == []
    assert not fs.exists(table_path(ROOT, 3))


@pytest.mark.parametrize("seed", range(20))
def test_matches_applying_tables_oldest_to_newest(seed: int) -> None:
    rng = random.Random(seed)
    fs = new_fs()
    tables: list[dict[bytes, Value]] = []
    for _ in range(rng.randint(1, 5)):
        table: dict[bytes, Value] = {}
        for _ in range(rng.randint(1, 60)):
            key = bytes([rng.randrange(80)])
            table[key] = TOMBSTONE if rng.random() < 0.3 else rng.randbytes(5)
        tables.append(table)

    # Newest first, so the newest table gets the biggest number
    readers = [
        make_table(fs, len(tables) - i, sorted(table.items())) for i, table in enumerate(tables)
    ]
    outputs = compact(fs, ROOT, readers, first_number=100, table_size=200)

    expected: dict[bytes, Value] = {}
    for table in reversed(tables):
        expected.update(table)
    live = [(k, v) for k, v in sorted(expected.items()) if not isinstance(v, Tombstone)]
    assert read_outputs(fs, outputs) == live
