import pytest

from faultline.entry import TOMBSTONE, Value
from faultline.sstable.block import BlockBuilder, decode_block
from faultline.sstable.errors import CorruptTable


def build(entries: list[tuple[bytes, Value]]) -> bytes:
    builder = BlockBuilder()
    for key, value in entries:
        builder.add(key, value)
    return builder.finish()


def test_round_trip_with_values_and_tombstones() -> None:
    entries: list[tuple[bytes, Value]] = [
        (b"apple", b"red"),
        (b"banana", TOMBSTONE),
        (b"cherry", b""),
        (b"date", b"brown" * 50),
    ]

    assert decode_block(build(entries)) == entries


def test_empty_block() -> None:
    assert decode_block(BlockBuilder().finish()) == []


def test_size_matches_finished_bytes() -> None:
    builder = BlockBuilder()
    builder.add(b"a", b"1")
    builder.add(b"b", TOMBSTONE)

    assert builder.size == len(builder.finish())


def test_keys_out_of_order_are_refused() -> None:
    builder = BlockBuilder()
    builder.add(b"b", b"1")

    with pytest.raises(ValueError):
        builder.add(b"a", b"2")
    with pytest.raises(ValueError):
        builder.add(b"b", b"again")


def test_every_flipped_bit_is_caught() -> None:
    raw = build([(b"key", b"value"), (b"other", TOMBSTONE)])

    for i in range(len(raw) * 8):
        damaged = bytearray(raw)
        damaged[i // 8] ^= 1 << (i % 8)
        with pytest.raises(CorruptTable):
            decode_block(bytes(damaged))


def test_cut_off_block_is_caught() -> None:
    raw = build([(b"key", b"value")])

    for cut in range(len(raw)):
        with pytest.raises(CorruptTable):
            decode_block(raw[:cut])
