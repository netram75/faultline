"""The messages Raft nodes send each other.

There are only two conversations in Raft, each a request and a reply:

    VoteRequest  -> VoteReply     "I want to be leader for this term, will you vote for me?"
    AppendRequest -> AppendReply  "Here are log entries (maybe none, a heartbeat), take them."

Every message carries the sender's term. That one number does a lot of work:
a node that sees a bigger term than its own knows it is behind and steps
down to follower, and a message with a smaller term is from the past and is
ignored.

Wire format, numbers little endian:

    kind u8, sender u32, receiver u32, term u64, then the body:
    VoteRequest:   last_log_index u64, last_log_term u64, pre_vote u8
    VoteReply:     granted u8, pre_vote u8
    AppendRequest: prev_log_index u64, prev_log_term u64, commit_index u64,
                   round u64, entry count u32,
                   then per entry: term u64, length u32, command
    AppendReply:   success u8, match_index u64, round u64
"""

import struct
from dataclasses import dataclass

type NodeId = int


@dataclass(frozen=True)
class Entry:
    """One command in the log, tagged with the term of the leader that added it."""

    term: int
    command: bytes


@dataclass(frozen=True)
class VoteRequest:
    last_log_index: int
    last_log_term: int
    # A pre-vote asks "would you vote for me?" without anyone changing their
    # term. See node.py for why that matters.
    pre_vote: bool = False


@dataclass(frozen=True)
class VoteReply:
    granted: bool
    pre_vote: bool = False


@dataclass(frozen=True)
class AppendRequest:
    prev_log_index: int  # the entry just before the new ones
    prev_log_term: int  # and its term, so the follower can check it has it
    entries: tuple[Entry, ...]
    commit_index: int  # how far the leader knows the log is committed
    # The leader's heartbeat round, echoed back in the reply. Safe reads use
    # it to know which replies came after they started (see read_index.py).
    round: int = 0


@dataclass(frozen=True)
class AppendReply:
    success: bool
    match_index: int  # on success: the follower's log matches up to here
    round: int = 0  # the round of the request this answers


type Body = VoteRequest | VoteReply | AppendRequest | AppendReply


@dataclass(frozen=True)
class Message:
    sender: NodeId
    receiver: NodeId
    term: int
    body: Body


class CorruptMessage(Exception):
    """The bytes are not a message we sent."""


_HEADER = struct.Struct("<BIIQ")  # kind, sender, receiver, term
_VOTE_REQUEST = struct.Struct("<QQB")
_VOTE_REPLY = struct.Struct("<BB")
_APPEND_REQUEST = struct.Struct("<QQQQI")
_ENTRY = struct.Struct("<QI")  # term, command length
_APPEND_REPLY = struct.Struct("<BQQ")

_KINDS: dict[type, int] = {VoteRequest: 1, VoteReply: 2, AppendRequest: 3, AppendReply: 4}


def encode(message: Message) -> bytes:
    body = message.body
    parts = [_HEADER.pack(_KINDS[type(body)], message.sender, message.receiver, message.term)]
    match body:
        case VoteRequest():
            parts.append(_VOTE_REQUEST.pack(body.last_log_index, body.last_log_term, body.pre_vote))
        case VoteReply():
            parts.append(_VOTE_REPLY.pack(body.granted, body.pre_vote))
        case AppendRequest():
            parts.append(
                _APPEND_REQUEST.pack(
                    body.prev_log_index,
                    body.prev_log_term,
                    body.commit_index,
                    body.round,
                    len(body.entries),
                )
            )
            for entry in body.entries:
                parts += [_ENTRY.pack(entry.term, len(entry.command)), entry.command]
        case AppendReply():
            parts.append(_APPEND_REPLY.pack(body.success, body.match_index, body.round))
    return b"".join(parts)


def decode(data: bytes) -> Message:
    try:
        message, end = _decode(data)
    except (struct.error, ValueError) as err:
        raise CorruptMessage(str(err)) from err
    if end != len(data):
        raise CorruptMessage("extra bytes after the message")
    return message


def _decode(data: bytes) -> tuple[Message, int]:
    kind, sender, receiver, term = _HEADER.unpack_from(data, 0)
    offset = _HEADER.size
    body: Body
    if kind == 1:
        last_index, last_term, pre_vote = _VOTE_REQUEST.unpack_from(data, offset)
        body = VoteRequest(last_index, last_term, _flag(pre_vote))
        offset += _VOTE_REQUEST.size
    elif kind == 2:
        granted, pre_vote = _VOTE_REPLY.unpack_from(data, offset)
        body = VoteReply(_flag(granted), _flag(pre_vote))
        offset += _VOTE_REPLY.size
    elif kind == 3:
        prev_index, prev_term, commit, round_, count = _APPEND_REQUEST.unpack_from(data, offset)
        offset += _APPEND_REQUEST.size
        entries = []
        for _ in range(count):
            entry_term, length = _ENTRY.unpack_from(data, offset)
            offset += _ENTRY.size
            if offset + length > len(data):
                raise ValueError("an entry runs past the end of the message")
            entries.append(Entry(entry_term, data[offset : offset + length]))
            offset += length
        body = AppendRequest(prev_index, prev_term, tuple(entries), commit, round_)
    elif kind == 4:
        success, match_index, round_ = _APPEND_REPLY.unpack_from(data, offset)
        body = AppendReply(_flag(success), match_index, round_)
        offset += _APPEND_REPLY.size
    else:
        raise ValueError(f"unknown message kind {kind}")
    return Message(sender, receiver, term, body), offset


def _flag(byte: int) -> bool:
    if byte not in (0, 1):
        raise ValueError(f"a true/false byte is {byte}")
    return byte == 1
