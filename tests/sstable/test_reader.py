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
