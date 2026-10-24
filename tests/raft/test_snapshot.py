import pytest

from faultline.fs.fake import FakeFileSystem
from faultline.raft import snapshot
from faultline.raft.snapshot import CorruptSnapshot, Snapshot


def test_round_trip() -> None:
    fs = FakeFileSystem()
    fs.mkdir("/n")
    saved = Snapshot(index=42, term=7, data=b"all the data")

    snapshot.save(fs, "/n", saved)
    fs.crash()

    assert snapshot.load(fs, "/n") == saved


def test_missing_is_none() -> None:
    fs = FakeFileSystem()
    fs.mkdir("/n")

    assert snapshot.load(fs, "/n") is None


def test_every_flipped_bit_is_caught() -> None:
    data = Snapshot(3, 1, b"abc").encode()
    for i in range(len(data) * 8):
        damaged = bytearray(data)
        damaged[i // 8] ^= 1 << (i % 8)
        with pytest.raises(CorruptSnapshot):
            Snapshot.decode(bytes(damaged))


def test_every_cut_is_caught() -> None:
    data = Snapshot(3, 1, b"abc").encode()
    for cut in range(len(data)):
        with pytest.raises(CorruptSnapshot):
            Snapshot.decode(data[:cut])
