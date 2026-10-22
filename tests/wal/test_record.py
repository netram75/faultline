import pytest

from faultline.wal import record
from faultline.wal.record import CorruptRecord, RecordError, TornRecord


def test_round_trip() -> None:
    data = record.encode(b"set name netram")

    payload, end = record.decode(data)

    assert payload == b"set name netram"
    assert end == len(data)


def test_empty_payload_is_fine() -> None:
    payload, end = record.decode(record.encode(b""))

    assert payload == b""
    assert end == record.HEADER_SIZE


def test_records_back_to_back() -> None:
    payloads = [b"first", b"", b"third one is longer"]
    buf = b"".join(record.encode(p) for p in payloads)

    found = []
    offset = 0
    while offset < len(buf):
        payload, offset = record.decode(buf, offset)
        found.append(payload)

    assert found == payloads


def test_every_cut_is_a_torn_record() -> None:
    data = record.encode(b"some payload")

    for cut in range(len(data)):
        with pytest.raises(TornRecord):
            record.decode(data[:cut])


def test_every_flipped_bit_is_caught() -> None:
    data = record.encode(b"some payload")

    for i in range(len(data) * 8):
        damaged = bytearray(data)
        damaged[i // 8] ^= 1 << (i % 8)
        # A flip in the length can make the record look longer than the
        # buffer (torn) instead of wrong (corrupt). Both count as caught.
        with pytest.raises(RecordError):
            record.decode(bytes(damaged))


def test_zeros_are_not_a_valid_record() -> None:
    # Some file systems leave zeros where an unfinished write was.
    with pytest.raises(CorruptRecord):
        record.decode(bytes(64))


def test_huge_length_is_corrupt_not_torn() -> None:
    header = (0).to_bytes(4, "little") + (2**32 - 1).to_bytes(4, "little")

    with pytest.raises(CorruptRecord):
        record.decode(header)


def test_payload_over_the_limit_is_refused() -> None:
    with pytest.raises(ValueError):
        record.encode(bytes(record.MAX_PAYLOAD_SIZE + 1))
