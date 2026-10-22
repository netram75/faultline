"""Whole-table tests: what goes in comes out, and damage never goes unnoticed."""

import random
from pathlib import Path

import pytest

from faultline.entry import Value
from faultline.fs.fake import FakeFileSystem
from faultline.fs.real import OSFileSystem
from faultline.memtable import Memtable
from faultline.sstable.errors import CorruptTable
from faultline.sstable.reader import TableReader
from faultline.sstable.writer import TableWriter

PATH = "/data/00000001.sst"


def random_memtable(seed: int) -> Memtable:
    rng = random.Random(seed)
    mt = Memtable(seed)
    for _ in range(rng.randint(0, 400)):
        key = rng.randbytes(rng.randint(0, 8))
        if rng.random() < 0.25:
            mt.delete(key)
        else:
            mt.put(key, rng.randbytes(rng.randint(0, 50)))
    return mt


def flush(fs: FakeFileSystem | OSFileSystem, path: str, entries: list[tuple[bytes, Value]]) -> None:
    writer = TableWriter(fs, path, block_size=256)
    for key, value in entries:
        writer.add(key, value)
    writer.finish()


@pytest.mark.parametrize("seed", range(25))
def test_memtable_flush_round_trip(seed: int) -> None:
    mt = random_memtable(seed)
    fs = FakeFileSystem()
    fs.mkdir("/data")
    flush(fs, PATH, list(mt))

    table = TableReader(fs, PATH)

    assert list(table) == list(mt)
    assert table.entry_count == len(mt)
    for key, value in mt:
        assert table.get(key) == value


def test_round_trip_on_the_real_disk(tmp_path: Path) -> None:
    mt = random_memtable(99)
    fs = OSFileSystem()
    path = str(tmp_path / "00000001.sst")
    flush(fs, path, list(mt))

    table = TableReader(fs, path)

    assert list(table) == list(mt)
    table.close()


def read_everything(fs: FakeFileSystem) -> None:
    """Open the table and touch every byte of it the reader cares about."""
    table = TableReader(fs, PATH)
    for key, _ in list(table):
        table.get(key)


def small_table() -> tuple[FakeFileSystem, bytes]:
    fs = FakeFileSystem()
    fs.mkdir("/data")
    entries: list[tuple[bytes, Value]] = [(f"key{i:03d}".encode(), b"value") for i in range(20)]
    flush(fs, PATH, entries)
    return fs, fs.read_file(PATH)


def rewrite(fs: FakeFileSystem, data: bytes) -> None:
    fs.remove(PATH)
    f = fs.open_append(PATH)
    f.write(data)
    f.close()


def test_every_flipped_bit_is_caught() -> None:
    # Every part of the file is covered by a checksum: each data block, the
    # bloom filter, the index and the footer. So no single flipped bit, in
    # any of them, should ever get through as wrong data.
    fs, original = small_table()

    for i in range(len(original) * 8):
        damaged = bytearray(original)
        damaged[i // 8] ^= 1 << (i % 8)
        rewrite(fs, bytes(damaged))
        with pytest.raises(CorruptTable):
            read_everything(fs)


def test_every_cut_is_caught() -> None:
    fs, original = small_table()

    for cut in range(len(original)):
        rewrite(fs, original[:cut])
        with pytest.raises(CorruptTable):
            read_everything(fs)
