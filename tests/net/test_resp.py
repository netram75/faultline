import random

import pytest

from faultline.net.resp import (
    MAX_LINE,
    Parser,
    ProtocolError,
    RespError,
    SimpleString,
    Value,
    encode,
)


def parse_all(data: bytes) -> list[Value]:
    parser = Parser()
    values = parser.feed(data)
    assert parser.waiting == 0
    return values


@pytest.mark.parametrize(
    ("value", "wire"),
    [
        (SimpleString("OK"), b"+OK\r\n"),
        (RespError("ERR no such key"), b"-ERR no such key\r\n"),
        (42, b":42\r\n"),
        (-7, b":-7\r\n"),
        (b"hello", b"$5\r\nhello\r\n"),
        (b"", b"$0\r\n\r\n"),
        (b"a\r\nb", b"$4\r\na\r\nb\r\n"),
        (None, b"$-1\r\n"),
        ([], b"*0\r\n"),
        ([b"GET", b"k"], b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
        ([1, [b"x", None]], b"*2\r\n:1\r\n*2\r\n$1\r\nx\r\n$-1\r\n"),
    ],
)
def test_encode_and_parse(value: Value, wire: bytes) -> None:
    assert encode(value) == wire
    assert parse_all(wire) == [value]


def test_null_array_reads_as_none() -> None:
    assert parse_all(b"*-1\r\n") == [None]


def test_several_values_in_one_read() -> None:
    assert parse_all(b"+OK\r\n:1\r\n$1\r\nx\r\n") == [SimpleString("OK"), 1, b"x"]


def test_one_byte_at_a_time() -> None:
    wire = encode([b"SET", b"key", b"some\r\nvalue"]) + encode(SimpleString("OK"))
    parser = Parser()
    values: list[Value] = []
    for i in range(len(wire)):
        values += parser.feed(wire[i : i + 1])

    assert values == [[b"SET", b"key", b"some\r\nvalue"], SimpleString("OK")]
    assert parser.waiting == 0


def test_half_a_value_waits() -> None:
    parser = Parser()

    assert parser.feed(b"$5\r\nhel") == []
    assert parser.waiting == 7
    assert parser.feed(b"lo\r\n") == [b"hello"]


def random_value(rng: random.Random, depth: int = 0) -> Value:
    kind = rng.randrange(6 if depth < 3 else 5)
    if kind == 0:
        return rng.randbytes(rng.randrange(20))
    if kind == 1:
        return rng.randrange(-(2**63), 2**63)
    if kind == 2:
        return SimpleString(rng.choice(["OK", "PONG", "", "héllo"]))
    if kind == 3:
        return RespError(rng.choice(["ERR bad", "WRONGTYPE x", ""]))
    if kind == 4:
        return None
    return [random_value(rng, depth + 1) for _ in range(rng.randrange(4))]


@pytest.mark.parametrize("seed", range(50))
def test_random_values_in_random_pieces(seed: int) -> None:
    rng = random.Random(seed)
    values = [random_value(rng) for _ in range(10)]
    wire = b"".join(encode(value) for value in values)

    parser = Parser()
    got: list[Value] = []
    pos = 0
    while pos < len(wire):
        step = rng.randrange(1, 12)
        got += parser.feed(wire[pos : pos + step])
        pos += step

    assert got == values


@pytest.mark.parametrize(
    "wire",
    [
        b"?what\r\n",
        b"\r\n",
        b":12x\r\n",
        b": 7\r\n",
        b":+7\r\n",
        b"$-2\r\n",
        b"$3\r\nhello\r\n",
        b"*-5\r\n",
        b"+\xff\r\n",
    ],
)
def test_bad_input(wire: bytes) -> None:
    with pytest.raises(ProtocolError):
        Parser().feed(wire)


def test_line_with_no_end_is_cut_off() -> None:
    parser = Parser()

    assert parser.feed(b"+" + b"a" * (MAX_LINE - 1)) == []
    with pytest.raises(ProtocolError, match="too long"):
        parser.feed(b"a")


def test_arrays_nested_too_deep() -> None:
    with pytest.raises(ProtocolError, match="too deep"):
        Parser().feed(b"*1\r\n" * 40 + b":1\r\n")


def test_huge_bulk_length_is_refused_before_waiting_for_it() -> None:
    with pytest.raises(ProtocolError, match="bulk string length"):
        Parser().feed(b"$999999999999\r\n")


@pytest.mark.parametrize("value", [SimpleString("a\nb"), RespError("a\rb")])
def test_simple_strings_are_one_line(value: Value) -> None:
    with pytest.raises(ValueError, match="can't hold"):
        encode(value)


def test_no_booleans() -> None:
    with pytest.raises(TypeError):
        encode(True)
