import random

import pytest

from faultline.entry import TOMBSTONE
from faultline.lsm.batch import CorruptBatch, WriteBatch
from faultline.memtable import Memtable


def test_round_trip() -> None:
    batch = WriteBatch().put(b"a", b"1").delete(b"b").put(b"", b"").put(b"c", b"x" * 1000)

    decoded = WriteBatch.decode(batch.encode())

    assert list(decoded) == [(b"a", b"1"), (b"b", TOMBSTONE), (b"", b""), (b"c", b"x" * 1000)]


def test_empty_batch() -> None:
    batch = WriteBatch()

    assert len(batch) == 0
    assert list(WriteBatch.decode(batch.encode())) == []


@pytest.mark.parametrize("seed", range(20))
def test_random_round_trip(seed: int) -> None:
    rng = random.Random(seed)
    batch = WriteBatch()
    for _ in range(rng.randint(0, 50)):
        key = rng.randbytes(rng.randint(0, 10))
        if rng.random() < 0.3:
            batch.delete(key)
        else:
            batch.put(key, rng.randbytes(rng.randint(0, 30)))

    assert list(WriteBatch.decode(batch.encode())) == list(batch)


def test_apply_goes_in_order() -> None:
    mt = Memtable()
    mt.put(b"old", b"v")
    batch = WriteBatch().put(b"k", b"first").put(b"k", b"second").delete(b"old")

    batch.apply(mt)

    assert mt.get(b"k") == b"second"
    assert mt.get(b"old") is TOMBSTONE


def test_every_cut_is_caught() -> None:
    data = WriteBatch().put(b"key", b"value").delete(b"gone").encode()

    for cut in range(len(data)):
        with pytest.raises(CorruptBatch):
            WriteBatch.decode(data[:cut])


def test_extra_bytes_are_caught() -> None:
    data = WriteBatch().put(b"key", b"value").encode()

    with pytest.raises(CorruptBatch, match="extra bytes"):
        WriteBatch.decode(data + b"\x00")


def test_unknown_kind_is_caught() -> None:
    data = bytearray(WriteBatch().put(b"key", b"value").encode())
    data[4] = 7  # the kind byte of the first change, right after the count

    with pytest.raises(CorruptBatch, match="unknown change kind 7"):
        WriteBatch.decode(bytes(data))
