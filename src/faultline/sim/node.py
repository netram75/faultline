"""One simulated node: a KVNode on its own fake disk, ticking on its own clock.

* **Its own disk.** A FakeFileSystem (fs/fake.py) that only this node uses,
  so a crash loses exactly what this node had not synced.
* **Its own clock.** The node ticks every tick_ms of simulated time, times
  its skew. A skew of 1.1 means its clock runs 10% slow: it ticks less often
  and times out later than the others. Real clocks drift like this, and Raft
  must not care, because it only ever counts its own ticks.
* **Crashes.** crash() drops the KVNode on the spot, without closing
  anything, and crashes the disk: unsynced data is lost or torn. restart()
  opens a fresh KVNode on the same disk, which recovers from what is there.
* **Power cuts in the middle of a write.** cut_power(n) lets the disk take n
  more calls and fail the next one with PowerCut. That can land halfway
  through a flush, a compaction or a snapshot. The node is down from that
  moment, like after crash().

Ticks from before a crash must not keep running after a restart, or the
node would tick twice as fast. Every crash bumps a generation number, and a
tick from an older generation does nothing.
"""

import random
from collections.abc import Callable
from functools import partial

from faultline.fs.fake import FakeFileSystem, PowerCut
from faultline.kv.commands import Command
from faultline.kv.server import KVNode
from faultline.kv.state_machine import Result
from faultline.lsm import Options
from faultline.raft.messages import Message, NodeId
from faultline.raft.node import Config
from faultline.sim.loop import EventLoop

# Small sizes, so flushes, compactions and snapshots all happen in short runs
SMALL_ENGINE = Options(memtable_size=4096, l0_limit=3, table_size=8192)


class SimNode:
    def __init__(
        self,
        loop: EventLoop,
        send: Callable[[Message], None],
        node_id: NodeId,
        peers: list[NodeId],
        config: Config | None = None,
        tick_ms: int = 10,
        snapshot_every: int = 200,
    ) -> None:
        self.id = node_id
        self.disk = FakeFileSystem(loop.seed_for(f"disk{node_id}"))
        self.kv: KVNode | None = None
        self.skew = 1.0
        self.ticks = 0
        self.crashes = 0
        self.power_cuts = 0
        # Where answers go. The simulation sets these.
        self.on_result: Callable[[Command, Result], None] = lambda command, result: None
        self.on_read: Callable[[int, bytes | None], None] = lambda ctx, value: None
        # Called after every step the node takes, for the safety checks
        self.after_step: Callable[[NodeId], None] = lambda node_id: None
        self._loop = loop
        self._send = send
        self._peers = peers
        self._config = config
        self._tick_ms = tick_ms
        self._snapshot_every = snapshot_every
        self._rng = random.Random(loop.seed_for(f"node{node_id}"))
        self._generation = 0

    @property
    def up(self) -> bool:
        return self.kv is not None

    def start(self) -> None:
        if self.up:
            return
        # A new seed every start, like a real restart picks new timeouts
        seed = self._rng.randrange(2**63)
        try:
            self.kv = KVNode.open(
                self.disk,
                f"/node{self.id}",
                self.id,
                self._peers,
                self._config,
                seed,
                SMALL_ENGINE,
                self._snapshot_every,
            )
        except PowerCut:
            self._power_went()
            return
        self._flush()
        self._schedule_tick()

    def crash(self, keep_names: bool = False) -> None:
        """Pull the plug, between two calls."""
        if not self.up:
            return
        self.kv = None
        self.disk.crash(keep_names)
        self._generation += 1
        self.crashes += 1

    def cut_power(self, steps: int, keep_names: bool = False) -> None:
        """Pull the plug `steps` disk calls from now, in the middle of whatever
        the node is doing then."""
        self.disk.cut_power_after(steps, keep_names)

    def receive(self, message: Message) -> None:
        if self.kv is not None:
            self._run(partial(self.kv.step, message))

    def submit(self, command: Command) -> bool:
        """Hand a client's write to this node. False if it is down or not
        the leader."""
        if self.kv is None:
            return False
        return self._run(partial(self.kv.submit, command)) is True

    def read(self, key: bytes, ctx: int) -> bool:
        if self.kv is None:
            return False
        return self._run(partial(self.kv.read, key, ctx)) is True

    def _schedule_tick(self) -> None:
        delay = max(1, round(self._tick_ms * self.skew))
        self._loop.after(delay, partial(self._tick, self._generation))

    def _tick(self, generation: int) -> None:
        if generation != self._generation or self.kv is None:
            return  # a timer from before a crash
        self.ticks += 1
        self._run(self.kv.tick)
        if self.up:
            self._schedule_tick()

    def _run[T](self, action: Callable[[], T]) -> T | None:
        try:
            result = action()
        except PowerCut:
            self._power_went()
            return None
        self._flush()
        return result

    def _power_went(self) -> None:
        # The disk already crashed itself (see FakeFileSystem.cut_power_after)
        self.kv = None
        self._generation += 1
        self.power_cuts += 1

    def _flush(self) -> None:
        """Send what the node wants sent and hand out its answers."""
        assert self.kv is not None
        for message in self.kv.take_messages():
            self._send(message)
        for command, result in self.kv.take_results():
            self.on_result(command, result)
        for ctx, value in self.kv.take_read_results():
            self.on_read(ctx, value)
        self.after_step(self.id)
