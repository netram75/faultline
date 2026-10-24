"""ReadIndex: answering reads safely without putting them in the log.

Raft thesis, section 6.4. A read changes nothing, so writing it to the log
would waste a disk sync on every node. But the leader can't simply answer
from its own data either. It may have been replaced without knowing: cut off
from the others, while they elected a new leader and committed new writes.
Its answer would be old, and a client could see a value go back in time.

ReadIndex makes a read safe in four steps:

1. The leader must have committed an entry in its own term (its no-op).
   Until then it doesn't know how far the log is really committed.
2. It notes its commit index, the read index. Every write that finished
   before the read started is at or below it.
3. It sends a round of heartbeats. If a majority answers, nobody had replaced
   it when the read started, so the read index was current.
4. Once its state machine has applied up to the read index, it answers from
   its own data.

Rounds: every append the leader sends carries its current round number, and
the reply echoes it. A read started in round r is confirmed once a majority,
the leader included, has answered round r or a later one. A reply to an
older append doesn't count: it may have been sent before the read started.
"""

from collections.abc import Callable
from dataclasses import dataclass

from faultline.raft.messages import NodeId


@dataclass(frozen=True)
class _Read:
    ctx: int  # the caller's id for this read
    index: int  # the read index
    round: int  # the heartbeat round that must confirm it


class ReadTracker:
    """A leader's reads waiting for their heartbeat round. One per term as
    leader: a node that steps down drops the ones still waiting, and their
    callers time out and try again."""

    def __init__(self, leader: NodeId) -> None:
        self.round = 0
        self._leader = leader
        self._answered: dict[NodeId, int] = {}  # latest round each follower answered
        self._waiting: list[_Read] = []
        self._ready: list[tuple[int, int]] = []

    def start(self, ctx: int, index: int) -> None:
        """A new read at `index`. The caller must now send a heartbeat round."""
        self.round += 1
        self._waiting.append(_Read(ctx, index, self.round))

    def answered(self, follower: NodeId, round_: int) -> None:
        # max(), because replies can arrive out of order
        self._answered[follower] = max(self._answered.get(follower, 0), round_)

    def confirm(self, is_majority: Callable[[set[NodeId]], bool]) -> None:
        """Move every read whose round a majority answered to the ready list."""
        while self._waiting:
            read = self._waiting[0]
            voters = {self._leader} | {f for f, r in self._answered.items() if r >= read.round}
            if not is_majority(voters):
                # Reads wait in round order, so the later ones can't be ready either
                return
            self._waiting.pop(0)
            self._ready.append((read.ctx, read.index))

    def take_ready(self) -> list[tuple[int, int]]:
        ready, self._ready = self._ready, []
        return ready
