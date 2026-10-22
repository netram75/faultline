import pytest

from faultline.sstable.bloom import BloomFilter, build_filter
from faultline.sstable.errors import CorruptTable

KEYS = [f"user:{i}".encode() for i in range(10_000)]


def test_never_says_no_to_a_key_it_has() -> None:
    bloom = BloomFilter(build_filter(KEYS))

    assert all(bloom.may_contain(key) for key in KEYS)


def test_false_positives_are_rare() -> None:
    bloom = BloomFilter(build_filter(KEYS))
    absent = [f"other:{i}".encode() for i in range(10_000)]

    false_positives = sum(bloom.may_contain(key) for key in absent)

    # About 1% in theory with 10 bits per key. Leave some room.
    assert false_positives / len(absent) < 0.02


def test_more_bits_means_fewer_false_positives() -> None:
    absent = [f"other:{i}".encode() for i in range(5_000)]

    def rate(bits_per_key: int) -> float:
        bloom = BloomFilter(build_filter(KEYS, bits_per_key))
        return sum(bloom.may_contain(k) for k in absent) / len(absent)

    assert rate(16) < rate(10) < rate(4)


def test_empty_filter_says_no() -> None:
    bloom = BloomFilter(build_filter([]))

    assert not bloom.may_contain(b"anything")


def test_same_keys_give_the_same_bytes() -> None:
    # Filters are saved to disk, so they must not depend on the process
    assert build_filter(KEYS[:100]) == build_filter(KEYS[:100])


def test_broken_filter_data_is_refused() -> None:
    with pytest.raises(CorruptTable):
        BloomFilter(b"")
    with pytest.raises(CorruptTable):
        BloomFilter(bytes(9))  # last byte says zero hashes
