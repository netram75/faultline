"""A whole cluster of real faultline processes on this machine.

Used by the tests, the chaos runs and the benchmark. Every node is its own
`python -m faultline serve` process, on free ports on 127.0.0.1, with its
data and its log in a folder of its own:

    directory/
      cluster.toml
      node1/          node 1's data
      node1.log       what node 1 printed

The point of real processes is that they fail like real ones: kill -9
stops a node dead without closing anything, and SIGSTOP freezes it, timers
and all, while its sockets stay open. Nothing in one process can fake that.
"""

import asyncio
import os
import signal
import socket
import subprocess
import sys
import time
from types import TracebackType
from typing import Self

from faultline.net.client import Client
from faultline.net.node import Address
from faultline.net.resp import RespError
from faultline.raft.messages import NodeId


def free_ports(count: int) -> list[int]:
    """Ports nobody is using right now. Hold them all open at once, so we
    don't get the same one twice."""
    sockets = [socket.socket() for _ in range(count)]
    try:
        for s in sockets:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


class LocalProcesses:
    def __init__(self, directory: str, size: int = 3, timeout: float = 2.0) -> None:
        self.directory = directory
        self.timeout = timeout
        self.ids = list(range(1, size + 1))
        ports = free_ports(2 * size)
        self.peers = {i: Address("127.0.0.1", ports[i - 1]) for i in self.ids}
        self.clients = {i: Address("127.0.0.1", ports[size + i - 1]) for i in self.ids}
        self._processes: dict[NodeId, subprocess.Popen[bytes]] = {}
        self._paused: set[NodeId] = set()
        os.makedirs(directory, exist_ok=True)
        self.cluster_file = os.path.join(directory, "cluster.toml")
        with open(self.cluster_file, "w") as f:
            for i in self.ids:
                f.write(f'[nodes.{i}]\npeer = "{self.peers[i]}"\nclient = "{self.clients[i]}"\n\n')

    def start(self, i: NodeId) -> None:
        if self.running(i):
            return
        command = [sys.executable, "-m", "faultline", "serve", "--id", str(i)]
        command += ["--cluster", self.cluster_file, "--timeout", str(self.timeout)]
        command += ["--data", os.path.join(self.directory, f"node{i}")]
        # The log goes to a file, not a pipe: nobody reads a pipe while the
        # node runs, and a full pipe would freeze the node
        with open(os.path.join(self.directory, f"node{i}.log"), "ab") as log:
            self._processes[i] = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=log)

    def start_all(self) -> None:
        for i in self.ids:
            self.start(i)

    def running(self, i: NodeId) -> bool:
        process = self._processes.get(i)
        return process is not None and process.poll() is None

    def kill(self, i: NodeId) -> None:
        """kill -9: the node stops dead, in the middle of whatever it was doing."""
        self._end(i, signal.SIGKILL)

    def stop(self, i: NodeId) -> int:
        """Ask the node to stop and wait for it. Returns its exit code."""
        return self._end(i, signal.SIGTERM)

    def pause(self, i: NodeId) -> None:
        """Freeze the node: no ticks, no reads from its sockets. To the
        others it looks like a very slow or cut-off node."""
        if self.running(i) and i not in self._paused:
            self._processes[i].send_signal(signal.SIGSTOP)
            self._paused.add(i)

    def resume(self, i: NodeId) -> None:
        if i in self._paused:
            self._paused.discard(i)
            if self.running(i):
                self._processes[i].send_signal(signal.SIGCONT)

    def close(self) -> None:
        """Kill whatever is still running. Safe to call more than once."""
        for i in list(self._processes):
            self.kill(i)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    async def leader(self, timeout: float = 15) -> NodeId:
        """Ask the running nodes until one of them can serve a safe read.
        Only the leader can, so that one is it."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for i in self.ids:
                if self.running(i) and i not in self._paused and await self._can_read(i):
                    return i
            await asyncio.sleep(0.05)
        raise TimeoutError("no leader")

    async def _can_read(self, i: NodeId) -> bool:
        try:
            client = await Client.connect(self.clients[i])
        except OSError:
            return False  # still starting
        try:
            reply = await asyncio.wait_for(client.call("GET", "probe"), self.timeout + 1)
            return not isinstance(reply, RespError)
        except (OSError, TimeoutError):
            return False
        finally:
            await client.close()

    def _end(self, i: NodeId, sig: signal.Signals) -> int:
        process = self._processes.pop(i, None)
        if process is None:
            return 0
        if process.poll() is None:
            if i in self._paused:
                process.send_signal(signal.SIGCONT)  # a frozen node can't stop
            process.send_signal(sig)
        self._paused.discard(i)
        return process.wait(timeout=30)
