import random

import pytest

from faultline.memtable.skiplist import SkipList


def test_empty_list() -> None:
    sl: SkipList[bytes] = SkipList()

    assert len(sl) == 0
    assert sl.get(b"a") is None
    assert list(sl) == []


def test_keys_come_out_sorted() -> None:
    sl: SkipList[bytes] = SkipList()
    for key in [b"m", b"c", b"x", b"a", b"q"]:
        sl.put(key, key.upper())

    assert list(sl) == [(b"a", b"A"), (b"c", b"C"), (b"m", b"M"), (b"q", b"Q"), (b"x", b"X")]


def test_put_replaces_existing_value() -> None:
    sl: SkipList[bytes] = SkipList()
    sl.put(b"k", b"old")
    sl.put(b"k", b"new")

    assert sl.get(b"k") == b"new"
    assert len(sl) == 1


def test_get_finds_only_exact_keys() -> None:
    sl: SkipList[int] = SkipList()
    sl.put(b"apple", 1)
    sl.put(b"banana", 2)

    assert sl.get(b"apple") == 1
    assert sl.get(b"app") is None
    assert sl.get(b"apples") is None


def test_seek_starts_at_first_key_not_smaller() -> None:
    sl: SkipList[int] = SkipList()
    for i, key in enumerate([b"b", b"d", b"f"]):
        sl.put(key, i)

    assert [k for k, _ in sl.seek(b"d")] == [b"d", b"f"]
    assert [k for k, _ in sl.seek(b"c")] == [b"d", b"f"]
    assert [k for k, _ in sl.seek(b"g")] == []


@pytest.mark.parametrize("seed", range(20))
def test_matches_a_plain_dict(seed: int) -> None:
    rng = random.Random(seed)
    sl: SkipList[int] = SkipList(seed)
    model: dict[bytes, int] = {}

    for i in range(500):
        # Short keys so plenty of them repeat and get overwritten
        key = rng.randbytes(rng.randint(0, 2))
        sl.put(key, i)
        model[key] = i

    assert len(sl) == len(model)
    assert list(sl) == sorted(model.items())
    for key, value in model.items():
        assert sl.get(key) == value


def test_handles_many_keys() -> None:
    sl: SkipList[int] = SkipList()
    keys = [f"key{i:06d}".encode() for i in range(20_000)]
    random.Random(1).shuffle(keys)
    for i, key in enumerate(keys):
        sl.put(key, i)

    assert [k for k, _ in sl] == sorted(keys)
