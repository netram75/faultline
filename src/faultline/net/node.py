"""Run one node for real: a KVNode on the real disk, on the real clock,
talking to the other nodes over TCP.

KVNode (kv/server.py) does all the thinking and none of the waiting: it has
no clock and no sockets. NodeRunner gives it those, on one asyncio event
loop:

* **Clock.** Every tick_ms it calls tick().
* **Peers.** A message for another node goes out on one TCP connection to
  that node, in a frame (frames.py). Each node also listens on its own peer
  address, reads the frames the others send, and hands each message to
  step().
* **Answers.** submit() and read() give back a future, and the runner fills
  it in when the node hands out the answer.

Everything runs on the event loop's one thread, so nothing needs a lock.
Disk calls block that thread for a moment (an fsync takes milliseconds).
That is fine: Raft only needs ticks to come roughly on time.

A broken connection is just lost messages, and Raft is built to cope with
those. So the runner does nothing clever about it: a message that can't go
now is dropped, and the link connects again in the background.
"""

import asyncio
import logging
import random
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Self

from faultline.fs.real import OSFileSystem
from faultline.kv.commands import Command
from faultline.kv.server import KVNode
from faultline.kv.state_machine import Result
from faultline.lsm import Options
from faultline.net.frames import FrameError, FrameReader, frame
from faultline.raft.messages import CorruptMessage, NodeId, decode, encode
from faultline.raft.node import Config, Role

log = logging.getLogger("faultline")

# Slower than the simulator's defaults. A real disk and a real network have
# hiccups, and an election every time one happens helps nobody. With 10 ms
# ticks: a heartbeat every 50 ms, an election after 300 to 600 ms of silence.
REAL_CONFIG = Config(election_ticks=30, heartbeat_ticks=5)


@dataclass(frozen=True)
class Address:
    host: str
    port: int

    @classmethod
    def parse(cls, text: str) -> Self:
        host, _, port = text.rpartition(":")
        if not host or not port.isdigit():
            raise ValueError(f"expected host:port, got {text!r}")
        return cls(host, int(port))

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


class NodeRunner:
    def __init__(
        self,
        node_id: NodeId,
        cluster: dict[NodeId, Address],
        directory: str,
        tick_ms: int = 10,
        config: Config = REAL_CONFIG,
        options: Options | None = None,
        snapshot_every: int = 1000,
    ) -> None:
        """`cluster` has the peer address of every node, this one included."""
        self.id = node_id
        self.cluster = cluster
        self.directory = directory
        self._tick_ms = tick_ms
        self._config = config
        self._options = options
        self._snapshot_every = snapshot_every
        self._kv: KVNode | None = None
        self._links = {i: _Link(address) for i, address in cluster.items() if i != node_id}
        self._server: asyncio.Server | None = None
        self._incoming: set[asyncio.StreamWriter] = set()
        self._stopping = False
        self._ticker: asyncio.Task[None] | None = None
        self._writes: dict[tuple[int, int], list[asyncio.Future[Result]]] = {}
        self._reads: dict[int, asyncio.Future[bytes | None]] = {}
        self._next_ctx = 1
        self._last_seen: tuple[Role, int, NodeId | None] | None = None

    @property
    def kv(self) -> KVNode:
        assert self._kv is not None, "the node is not running"
        return self._kv

    @property
    def running(self) -> bool:
        return self._kv is not None

    async def start(self) -> None:
        # A new random seed every start, so nodes pick different timeouts
        seed = random.SystemRandom().randrange(2**63)
        self._stopping = False
        self._kv = KVNode.open(
            OSFileSystem(),
            self.directory,
            self.id,
            sorted(self.cluster),
            self._config,
            seed,
            self._options,
            self._snapshot_every,
        )
        address = self.cluster[self.id]
        self._server = await asyncio.start_server(self._serve_peer, address.host, address.port)
        self._ticker = asyncio.create_task(self._tick_forever())
        self._flush()
        log.info("node %d started, data in %s, peers on %s", self.id, self.directory, address)

    async def stop(self) -> None:
        """Stop and close everything. Answers still waiting are cancelled:
        their writes may or may not happen."""
        self._stopping = True
        if self._ticker is not None:
            self._ticker.cancel()
        if self._server is not None:
            self._server.close()
        for writer in list(self._incoming):
            writer.close()
        for link in self._links.values():
            link.close()
        if self._server is not None:
            # Waits for every peer connection to close. One that was taken
            # just now, whose handler has not run yet, closes itself as soon
            # as it starts (see _serve_peer).
            await self._server.wait_closed()
        for futures in list(self._writes.values()):
            for future in futures:
                future.cancel()
        for read in list(self._reads.values()):
            read.cancel()
        if self._kv is not None:
            self._kv.close()
            self._kv = None
        log.info("node %d stopped", self.id)

    def submit(self, command: Command) -> "asyncio.Future[Result] | None":
        """Start a write. None if this node is not the leader. The future
        never finishes if the write is lost, say because the leader changed:
        the caller must have a timeout."""
        future: asyncio.Future[Result] = asyncio.get_running_loop().create_future()
        key = (command.client, command.seq)
        # Wait for the answer before asking, because on a cluster of one the
        # answer comes before submit() returns
        self._writes.setdefault(key, []).append(future)
        future.add_done_callback(partial(self._forget_write, key))
        if not self._run(partial(self.kv.submit, command)):
            future.cancel()
            return None
        return future

    def read(self, key: bytes) -> "asyncio.Future[bytes | None] | None":
        """Start a safe read. None if this node can't serve reads now."""
        ctx = self._next_ctx
        self._next_ctx += 1
        future: asyncio.Future[bytes | None] = asyncio.get_running_loop().create_future()
        self._reads[ctx] = future
        future.add_done_callback(lambda _: self._reads.pop(ctx, None))
        if not self._run(partial(self.kv.read, key, ctx)):
            future.cancel()
            return None
        return future

    def dropped(self) -> int:
        """Messages to other nodes that could not be sent."""
        return sum(link.dropped for link in self._links.values())

    async def _tick_forever(self) -> None:
        while True:
            await asyncio.sleep(self._tick_ms / 1000)
            self._run(self.kv.tick)

    async def _serve_peer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Read messages from one other node until the connection ends."""
        if self._stopping:
            writer.close()
            return
        self._incoming.add(writer)
        frames = FrameReader()
        try:
            while data := await reader.read(64 * 1024):
                for payload in frames.feed(data):
                    message = decode(payload)
                    if message.receiver == self.id and self._kv is not None:
                        self._run(partial(self._kv.step, message))
        except (FrameError, CorruptMessage) as e:
            log.warning("node %d: closing a peer connection: %s", self.id, e)
        except ConnectionError:
            pass
        finally:
            self._incoming.discard(writer)
            writer.close()

    def _run[T](self, action: Callable[[], T]) -> T:
        result = action()
        self._flush()
        return result

    def _flush(self) -> None:
        """Send what the node wants sent and hand out its answers."""
        kv = self.kv
        for message in kv.take_messages():
            self._links[message.receiver].send(frame(encode(message)))
        for command, result in kv.take_results():
            for future in self._writes.get((command.client, command.seq), []):
                if not future.done():
                    future.set_result(result)
        for ctx, value in kv.take_read_results():
            read = self._reads.get(ctx)
            if read is not None and not read.done():
                read.set_result(value)
        self._log_changes()

    def _forget_write(self, key: tuple[int, int], future: "asyncio.Future[Result]") -> None:
        futures = self._writes.get(key, [])
        if future in futures:
            futures.remove(future)
        if not futures:
            self._writes.pop(key, None)

    def _log_changes(self) -> None:
        raft = self.kv.raft
        now = (raft.role, raft.term, raft.leader)
        if now != self._last_seen:
            self._last_seen = now
            log.info(
                "node %d: %s in term %d, leader %s",
                self.id,
                raft.role.value,
                raft.term,
                raft.leader,
            )


class _Link:
    """The connection to one other node. Opened when there is something to
    send, and again after it breaks, with a growing pause between tries."""

    QUEUE_LIMIT = 1000

    def __init__(self, address: Address) -> None:
        self.address = address
        self.dropped = 0
        self._queue: asyncio.Queue[bytes] = asyncio.Queue(self.QUEUE_LIMIT)
        self._task: asyncio.Task[None] | None = None

    def send(self, data: bytes) -> None:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())
        try:
            self._queue.put_nowait(data)
        except asyncio.QueueFull:
            self.dropped += 1  # the other node is slow or gone

    def close(self) -> None:
        if self._task is not None:
            self._task.cancel()

    async def _run(self) -> None:
        pause = 0.05
        while True:
            try:
                _, writer = await asyncio.open_connection(self.address.host, self.address.port)
            except OSError:
                # Nobody there. What waited for it is lost, like on a network.
                self._drop_waiting()
                await asyncio.sleep(pause)
                pause = min(pause * 2, 1.0)
                continue
            pause = 0.05
            try:
                while True:
                    writer.write(await self._queue.get())
                    while not self._queue.empty():
                        writer.write(self._queue.get_nowait())
                    await writer.drain()
            except OSError:
                pass  # the other side went away, connect again
            finally:
                writer.close()

    def _drop_waiting(self) -> None:
        while not self._queue.empty():
            self._queue.get_nowait()
            self.dropped += 1
