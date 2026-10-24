import random

import pytest

from faultline.kv.commands import (
    Command,
    CompareAndSet,
    CorruptCommand,
    Delete,
    Operation,
    Put,
    decode,
    encode,
)

EXAMPLES = [
    Command(1, 1, Put(b"name", b"faultline")),
    Command(1, 2, Put(b"", b"")),
    Command(7, 3, Delete(b"name")),
    Command(2, 9, CompareAndSet(b"x", b"old", b"new")),
    Command(2, 10, CompareAndSet(b"x", None, b"first")),
    Command(2**64 - 1, 2**64 - 1, CompareAndSet(b"x", b"", b"")),
]


@pytest.mark.parametrize("command", EXAMPLES)
def test_round_trip(command: Command) -> None:
    assert decode(encode(command)) == command


@pytest.mark.parametrize("seed", range(20))
def test_random_round_trip(seed: int) -> None:
    rng = random.Random(seed)

    def blob() -> bytes:
        return rng.randbytes(rng.randint(0, 20))

    ops: list[Operation] = [
        Put(blob(), blob()),
        Delete(blob()),
        CompareAndSet(blob(), rng.choice([None, blob()]), blob()),
    ]
    op = rng.choice(ops)
    command = Command(rng.randrange(1000), rng.randrange(1, 1000), op)

    assert decode(encode(command)) == command


@pytest.mark.parametrize("command", EXAMPLES)
def test_every_cut_is_caught(command: Command) -> None:
    data = encode(command)
    for cut in range(len(data)):
        with pytest.raises(CorruptCommand):
            decode(data[:cut])


def test_extra_bytes_are_caught() -> None:
    with pytest.raises(CorruptCommand, match="extra bytes"):
        decode(encode(EXAMPLES[0]) + b"\x00")


def test_unknown_kind_is_caught() -> None:
    data = bytearray(encode(EXAMPLES[0]))
    data[16] = 9  # the kind byte, after client and seq

    with pytest.raises(CorruptCommand, match="unknown command kind 9"):
        decode(bytes(data))


def test_bad_flag_is_caught() -> None:
    data = bytearray(encode(Command(1, 1, CompareAndSet(b"k", None, b"v"))))
    data[17 + 4 + 1] = 5  # the has_expected byte, after the header and the key

    with pytest.raises(CorruptCommand, match="bad flag 5"):
        decode(bytes(data))
