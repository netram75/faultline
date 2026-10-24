"""The simulator's clock and its list of things to do.

The simulator runs a whole cluster inside one Python process, without
threads, sockets or real time. Everything that happens is an event: a node's
timer ticks, a message arrives, a client sends a request, a disk loses power.
Each event has a time, and the loop runs them in time order, moving its
clock straight to the next one. Nothing ever waits for real time to pass, so
a minute of cluster time takes a fraction of a second.

This is called discrete event simulation. It is how FoundationDB tests its
database, and why a bug it finds can be replayed exactly: same seed, same
events, same order, same bug.

Time is in milliseconds. Two events at the same time run in the order they
were scheduled.

Randomness: every part of the simulation (each node's disk, the network,
each client, the fault schedule) gets its own random generator, seeded from
the run's seed and the part's name (seed_for). So adding a client changes
what that client does, and nothing else.
"""

import hashlib
import heapq
from collections.abc import Callable

type Action = Callable[[], None]


class EventLoop:
    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.now = 0  # milliseconds since the start
        # (time, order, action). order breaks ties, so actions are never compared.
        self._queue: list[tuple[int, int, Action]] = []
        self._order = 0

    def seed_for(self, name: str) -> int:
        """A seed for one part of the simulation, from the run's seed and the
        part's name. Uses a real hash: Python's hash() of a str changes every
        time Python starts, which would make runs impossible to repeat."""
        digest = hashlib.blake2b(f"{self.seed}/{name}".encode(), digest_size=8).digest()
        return int.from_bytes(digest, "big")

    def at(self, time: int, action: Action) -> None:
        """Run `action` at `time`. A time in the past means now."""
        heapq.heappush(self._queue, (max(time, self.now), self._order, action))
        self._order += 1

    def after(self, delay: int, action: Action) -> None:
        self.at(self.now + delay, action)

    def run_until(self, time: int) -> None:
        """Run every event up to and including `time`, then move the clock to
        `time`. Events can schedule more events, and those run too if they
        fall before `time`."""
        while self._queue and self._queue[0][0] <= time:
            when, _, action = heapq.heappop(self._queue)
            self.now = when
            action()
        self.now = max(self.now, time)

    @property
    def pending(self) -> int:
        """Events waiting to run."""
        return len(self._queue)
