"""Turn a Redis command into a faultline request.

A Redis client sends each command as an array of bulk strings, like
[b"SET", b"k", b"v"]. These are the commands faultline understands:

    PING [message]              PONG, or the message back
    GET key                     the value, or null if the key is missing
    SET key value               OK
    DEL key                     1 if the key was there, 0 if not
    SETNX key value             1 if it was set, 0 if the key was already there
    CAS key expected value      1 if key held expected and is now value, 0 if not

SET, DEL and SETNX are real Redis commands, so redis-cli and the usual Redis
libraries can send them as they are. CAS is ours: Redis has no plain
compare-and-set (it does that with WATCH and MULTI, which faultline does not
have). Command names are not case sensitive, keys and values are any bytes.

Writes become the same operations the simulator's clients use (see
kv/commands.py), so they go through Raft and the client sessions the same
way.
"""

from dataclasses import dataclass

from faultline.kv.commands import CompareAndSet, Delete, Operation, Put
from faultline.net.resp import Value


class BadRequest(Exception):
    """The command is wrong. The message is what the client gets back,
    worded the way Redis words it."""


@dataclass(frozen=True)
class Ping:
    message: bytes | None = None


@dataclass(frozen=True)
class Get:
    key: bytes


type Request = Ping | Get | Operation

# How many arguments each command takes, after its name
_ARGUMENTS = {
    "PING": (0, 1),
    "GET": (1, 1),
    "SET": (2, 2),
    "DEL": (1, 1),
    "SETNX": (2, 2),
    "CAS": (3, 3),
}


def parse_request(value: Value) -> Request:
    if not isinstance(value, list):
        value = []
    parts = [part for part in value if isinstance(part, bytes)]
    if not parts or len(parts) != len(value):
        raise BadRequest("ERR a command must be a non-empty array of bulk strings")
    name = parts[0].decode(errors="replace").upper()
    args = parts[1:]
    if name not in _ARGUMENTS:
        raise BadRequest(f"ERR unknown command '{_printable(name)}'")
    low, high = _ARGUMENTS[name]
    if not low <= len(args) <= high:
        raise BadRequest(f"ERR wrong number of arguments for '{name.lower()}' command")

    match name:
        case "PING":
            return Ping(args[0] if args else None)
        case "GET":
            return Get(args[0])
        case "SET":
            return Put(args[0], args[1])
        case "DEL":
            return Delete(args[0])
        case "SETNX":
            return CompareAndSet(args[0], None, args[1])
        case _:
            return CompareAndSet(args[0], args[1], args[2])


def _printable(name: str) -> str:
    # The name goes back to the client in a one-line error, so keep it
    # short and on one line
    return name[:40].replace("\r", " ").replace("\n", " ")
