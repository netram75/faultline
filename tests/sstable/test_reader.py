import random

import pytest

from faultline.entry import TOMBSTONE, Value
from faultline.fs.fake import FakeFileSystem
from faultline.sstable.errors import CorruptTable
from faultline.sstable.reader import TableReader
from faultline.sstable.writer import TableWriter

PATH = "/data/00000001.sst"


def write_table(entries: list[tuple[bytes, Value]], block_size: int = 4096) -> FakeFileSystem:
    fs = FakeFileSystem()
    fs.mkdir("/data")
    writer = TableWriter(fs, PATH, block_size=block_size)
    for key, value in entries:
        writer.add(key, value)
    writer.finish()
    return fs


def test_finds_values_and_tombstones() -> None:
    fs = write_table([(b"a", b"1"), (b"b", TOMBSTONE), (b"c", b"3")])
    table = TableReader(fs, PATH)

    assert table.get(b"a") == b"1"
    assert table.get(b"b") is TOMBSTONE
    assert table.get(b"c") == b"3"
    assert table.entry_count == 3


def test_missing_keys_are_none() -> None:
    fs = write_table([(b"b", b"1"), (b"d", b"2")])
    table = TableReader(fs, PATH)

    for key in [b"a", b"c", b"e", b"", b"bb"]:
        assert table.get(key) is None


def test_empty_table() -> None:
    fs = write_table([])
    table = TableReader(fs, PATH)

    assert table.get(b"anything") is None
    assert table.entry_count == 0


def test_every_key_across_many_blocks() -> None:
    rng = random.Random(3)
    keys = sorted({rng.randbytes(rng.randint(1, 12)) for _ in range(2_000)})
    entries: list[tuple[bytes, Value]] = [(k, k[::-1]) for k in keys]
    fs = write_table(entries, block_size=200)
    table = TableReader(fs, PATH)

    for key, value in entries:
        assert table.get(key) == value


def test_garbage_file_is_refused() -> None:
    fs = FakeFileSystem()
    fs.mkdir("/data")
    f = fs.open_append(PATH)
    f.write(b"this is not an sstable" * 10)
    f.close()

    with pytest.raises(CorruptTable):
        TableReader(fs, PATH)


def test_tiny_file_is_refused() -> None:
    fs = FakeFileSystem()
    fs.mkdir("/data")
    fs.open_append(PATH).close()

    with pytest.raises(CorruptTable):
        TableReader(fs, PATH)


def test_scan_returns_everything_in_order() -> None:
    entries: list[tuple[bytes, Value]] = [
        (f"k{i:04d}".encode(), TOMBSTONE if i % 7 == 0 else str(i).encode()) for i in range(300)
    ]
    fs = write_table(entries, block_size=128)
    table = TableReader(fs, PATH)

    assert list(table) == entries


def test_seek_lands_on_the_first_key_not_smaller() -> None:
    entries: list[tuple[bytes, Value]] = [(f"k{i:04d}".encode(), b"") for i in range(0, 300, 2)]
    fs = write_table(entries, block_size=128)
    table = TableReader(fs, PATH)

    # An exact key, a key that sits between two others, and a key past the end
    assert next(table.seek(b"k0100"))[0] == b"k0100"
    assert next(table.seek(b"k0101"))[0] == b"k0102"
    assert list(table.seek(b"k9999")) == []
    tail = [k for k, _ in table.seek(b"k0290")]
    assert tail == [b"k0290", b"k0292", b"k0294", b"k0296", b"k0298"]


def test_seek_matches_a_sorted_list_for_random_starts() -> None:
    rng = random.Random(8)
    keys = sorted({rng.randbytes(rng.randint(1, 6)) for _ in range(800)})
    entries: list[tuple[bytes, Value]] = [(k, b"v") for k in keys]
    fs = write_table(entries, block_size=100)
    table = TableReader(fs, PATH)

    for _ in range(200):
        start = rng.randbytes(rng.randint(0, 6))
        expected = [e for e in entries if e[0] >= start]
        assert list(table.seek(start)) == expected
