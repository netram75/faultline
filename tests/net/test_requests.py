import pytest

from faultline.kv.commands import CompareAndSet, Delete, Put
from faultline.net.requests import BadRequest, Get, Info, Ping, Request, parse_request
from faultline.net.resp import Parser, SimpleString, Value


@pytest.mark.parametrize(
    ("command", "request_"),
    [
        ([b"PING"], Ping()),
        ([b"PING", b"hi"], Ping(b"hi")),
        ([b"GET", b"k"], Get(b"k")),
        ([b"SET", b"k", b"v"], Put(b"k", b"v")),
        ([b"DEL", b"k"], Delete(b"k")),
        ([b"SETNX", b"k", b"v"], CompareAndSet(b"k", None, b"v")),
        ([b"CAS", b"k", b"old", b"new"], CompareAndSet(b"k", b"old", b"new")),
        ([b"INFO"], Info()),
        ([b"INFO", b"all"], Info()),
    ],
)
def test_commands(command: Value, request_: Request) -> None:
    assert parse_request(command) == request_


def test_names_are_not_case_sensitive() -> None:
    assert parse_request([b"get", b"k"]) == Get(b"k")
    assert parse_request([b"SeT", b"k", b"v"]) == Put(b"k", b"v")


def test_keys_and_values_are_any_bytes() -> None:
    assert parse_request([b"SET", b"\x00\r\n", b"\xff"]) == Put(b"\x00\r\n", b"\xff")


def test_straight_from_the_wire() -> None:
    # What redis-cli sends for: SET greeting hello
    [value] = Parser().feed(b"*3\r\n$3\r\nSET\r\n$8\r\ngreeting\r\n$5\r\nhello\r\n")

    assert parse_request(value) == Put(b"greeting", b"hello")


def test_unknown_command() -> None:
    with pytest.raises(BadRequest, match="ERR unknown command 'FLUSHALL'"):
        parse_request([b"FLUSHALL"])


@pytest.mark.parametrize(
    "command",
    [[b"GET"], [b"GET", b"a", b"b"], [b"SET", b"k"], [b"DEL"], [b"CAS", b"k", b"v"], [b"PING"] * 3],
)
def test_wrong_number_of_arguments(command: Value) -> None:
    with pytest.raises(BadRequest, match="ERR wrong number of arguments"):
        parse_request(command)


@pytest.mark.parametrize("command", [[], b"GET", None, 7, [b"GET", 7], [SimpleString("GET")]])
def test_not_an_array_of_bulk_strings(command: Value) -> None:
    with pytest.raises(BadRequest, match="non-empty array of bulk strings"):
        parse_request(command)


def test_error_stays_on_one_line() -> None:
    with pytest.raises(BadRequest) as caught:
        parse_request([b"BAD\r\nNAME"])

    assert "\n" not in str(caught.value)
    assert "\r" not in str(caught.value)
