from faultline.entry import TOMBSTONE
from faultline.memtable.memtable import Memtable


def test_put_and_get() -> None:
    mt = Memtable()
    mt.put(b"name", b"netram")

    assert mt.get(b"name") == b"netram"


def test_unknown_key_is_none_not_deleted() -> None:
    mt = Memtable()

    assert mt.get(b"nobody") is None


def test_delete_leaves_a_tombstone() -> None:
    mt = Memtable()
    mt.put(b"k", b"v")
    mt.delete(b"k")

    assert mt.get(b"k") is TOMBSTONE


def test_delete_of_unseen_key_still_leaves_a_tombstone() -> None:
    # The key may live in an older file on disk, so the delete must be recorded.
    mt = Memtable()
    mt.delete(b"only-on-disk")

    assert mt.get(b"only-on-disk") is TOMBSTONE
    assert len(mt) == 1


def test_put_after_delete_brings_the_key_back() -> None:
    mt = Memtable()
    mt.delete(b"k")
    mt.put(b"k", b"again")

    assert mt.get(b"k") == b"again"


def test_iteration_is_sorted_and_keeps_tombstones() -> None:
    mt = Memtable()
    mt.put(b"b", b"2")
    mt.delete(b"c")
    mt.put(b"a", b"1")

    assert list(mt) == [(b"a", b"1"), (b"b", b"2"), (b"c", TOMBSTONE)]
    assert list(mt.seek(b"b")) == [(b"b", b"2"), (b"c", TOMBSTONE)]


def test_size_tracks_keys_and_values() -> None:
    mt = Memtable()
    assert mt.size == 0

    mt.put(b"k", b"x" * 100)
    first = mt.size
    assert first > 100

    mt.put(b"k", b"x" * 10)  # same key, smaller value
    assert mt.size == first - 90

    mt.delete(b"k")  # a tombstone has no value bytes
    assert mt.size == first - 100

    mt.put(b"other", b"")
    assert mt.size > first - 100
