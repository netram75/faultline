import random

import pytest

from faultline.raft.messages import (
    AppendReply,
    AppendRequest,
    CorruptMessage,
    Entry,
    InstallSnapshot,
    Message,
    VoteReply,
    VoteRequest,
    decode,
    encode,
)

EXAMPLES = [
    Message(1, 2, 7, VoteRequest(last_log_index=10, last_log_term=6)),
    Message(1, 2, 7, VoteRequest(0, 0, pre_vote=True)),
    Message(2, 1, 7, VoteReply(granted=True)),
    Message(2, 1, 8, VoteReply(granted=False, pre_vote=True)),
    Message(1, 3, 7, AppendRequest(10, 6, (), commit_index=9)),
    Message(1, 3, 7, AppendRequest(10, 6, (Entry(7, b"set x 1"), Entry(7, b"")), 9)),
    Message(3, 1, 7, AppendReply(success=True, match_index=12)),
    Message(3, 1, 7, AppendReply(success=False, match_index=0)),
    Message(1, 3, 7, AppendRequest(10, 6, (), commit_index=9, round=42)),
    Message(3, 1, 7, AppendReply(success=True, match_index=12, round=42)),
    Message(1, 3, 7, InstallSnapshot(last_index=500, last_term=6, data=b"all the data")),
    Message(1, 3, 7, InstallSnapshot(0, 0, b"")),
]


@pytest.mark.parametrize("message", EXAMPLES)
def test_round_trip(message: Message) -> None:
    assert decode(encode(message)) == message


@pytest.mark.parametrize("seed", range(20))
def test_random_append_round_trip(seed: int) -> None:
    rng = random.Random(seed)
    entries = tuple(
        Entry(rng.randrange(1, 100), rng.randbytes(rng.randint(0, 40)))
        for _ in range(rng.randint(0, 20))
    )
    message = Message(
        rng.randrange(5),
        rng.randrange(5),
        rng.randrange(100),
        AppendRequest(rng.randrange(1000), rng.randrange(100), entries, rng.randrange(1000)),
    )

    assert decode(encode(message)) == message


@pytest.mark.parametrize("message", EXAMPLES)
def test_every_cut_is_caught(message: Message) -> None:
    data = encode(message)
    for cut in range(len(data)):
        with pytest.raises(CorruptMessage):
            decode(data[:cut])


def test_extra_bytes_are_caught() -> None:
    with pytest.raises(CorruptMessage, match="extra bytes"):
        decode(encode(EXAMPLES[0]) + b"\x00")


def test_unknown_kind_is_caught() -> None:
    data = bytearray(encode(EXAMPLES[0]))
    data[0] = 9

    with pytest.raises(CorruptMessage, match="unknown message kind 9"):
        decode(bytes(data))


def test_bad_flag_is_caught() -> None:
    data = bytearray(encode(Message(2, 1, 7, VoteReply(granted=True))))
    data[-2] = 5  # the granted byte

    with pytest.raises(CorruptMessage, match="true/false byte is 5"):
        decode(bytes(data))
