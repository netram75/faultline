"""Known bugs to switch on, to see the checker catch them.

A checker that never says "bad" proves nothing. Maybe the code is right,
maybe the checker is blind. So here are a few bugs that real databases have
shipped, each one a small change to faultline. Switch one on for a run:

    python -m faultline.sim --seed 3 --bug early-ack

and the checker should say the history is not linearizable, for some seeds
at least. The rest of the code never knows about this file. A bug is put in
by swapping one method for a broken copy, only while the run lasts.
"""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from faultline.kv.commands import Command, Put, encode
from faultline.kv.server import KVNode
from faultline.kv.state_machine import Result, StateMachine
from faultline.lsm import WriteBatch
from faultline.raft.node import Role


@dataclass(frozen=True)
class Bug:
    about: str
    owner: type
    method: str
    broken: Callable[..., Any]


def _read_without_read_index(self: KVNode, key: bytes, ctx: int) -> bool:
    # The leader answers from its own data at once. It never checks with a
    # majority that it is still the leader, so a leader that was cut off and
    # replaced keeps answering with old data.
    if self.raft.role is not Role.LEADER:
        return False
    self._read_results.append((ctx, self.machine.get(key)))
    return True


def _run_without_sessions(self: StateMachine, command: Command, batch: WriteBatch) -> Result:
    # Every command runs, even one this client sent before
    return self._run(command.op, batch)


def _ack_before_commit(self: KVNode, command: Command) -> bool:
    index = self.raft.propose(encode(command))
    if index is None:
        return False
    if isinstance(command.op, Put):
        # Answer now, as if the put were already done. If this leader loses
        # its job before the entry commits, the entry can be thrown away.
        self._results.append((command, Result(ok=True)))
    else:
        self._waiting[index] = command
    self._drain()
    return True


BUGS = {
    "leader-read": Bug(
        "a leader serves reads from its own data, without ReadIndex",
        KVNode,
        "read",
        _read_without_read_index,
    ),
    "no-sessions": Bug(
        "the state machine forgets sessions, so a retried write runs twice",
        StateMachine,
        "_run_once",
        _run_without_sessions,
    ),
    "early-ack": Bug(
        "a leader says ok to a put before it is committed",
        KVNode,
        "submit",
        _ack_before_commit,
    ),
}


@contextmanager
def switched_on(name: str | None) -> Iterator[None]:
    """Run the body with bug `name` in place. None means no bug."""
    if name is None:
        yield
        return
    bug = BUGS[name]
    original = getattr(bug.owner, bug.method)
    setattr(bug.owner, bug.method, bug.broken)
    try:
        yield
    finally:
        setattr(bug.owner, bug.method, original)
