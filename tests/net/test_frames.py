import random

import pytest

from faultline.net.frames import MAX_FRAME, FrameError, FrameReader, frame
from faultline.raft.messages import AppendRequest, Entry, Message, decode, encode


def test_one_frame() -> None:
    reader = FrameReader()

    assert frame(b"hello") == b"\x05\x00\x00\x00hello"
    assert reader.feed(frame(b"hello")) == [b"hello"]
    assert reader.waiting == 0


def test_empty_payload() -> None:
    assert FrameReader().feed(frame(b"")) == [b""]


def test_several_frames_in_one_read() -> None:
    data = frame(b"a") + frame(b"bb") + frame(b"ccc")

    assert FrameReader().feed(data) == [b"a", b"bb", b"ccc"]


def test_half_a_frame_waits() -> None:
    reader = FrameReader()
    data = frame(b"hello") + frame(b"world")

    assert reader.feed(data[:2]) == []
    assert reader.feed(data[2:7]) == []
    assert reader.feed(data[7:12]) == [b"hello"]
    assert reader.waiting == 3
    assert reader.feed(data[12:]) == [b"world"]


@pytest.mark.parametrize("seed", range(20))
def test_random_pieces(seed: int) -> None:
    rng = random.Random(seed)
    payloads = [rng.randbytes(rng.randrange(50)) for _ in range(30)]
    data = b"".join(frame(payload) for payload in payloads)

    reader = FrameReader()
    got: list[bytes] = []
    pos = 0
    while pos < len(data):
        step = rng.randrange(1, 40)
        got += reader.feed(data[pos : pos + step])
        pos += step

    assert got == payloads


def test_raft_messages_go_through() -> None:
    message = Message(1, 2, 7, AppendRequest(4, 3, (Entry(7, b"put k v"),), 4))

    [payload] = FrameReader().feed(frame(encode(message)))

    assert decode(payload) == message


def test_a_huge_length_is_refused_at_once() -> None:
    # Don't wait for 4 GB that will never come
    with pytest.raises(FrameError, match="too big"):
        FrameReader().feed(b"\xff\xff\xff\xff")


def test_too_big_to_send() -> None:
    with pytest.raises(FrameError, match="too big to send"):
        frame(bytes(MAX_FRAME + 1))
