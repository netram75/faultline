"""One node of the database: a Raft node and a state machine, wired together.

    raft      agrees with the other nodes on the order of commands
    machine   applies the committed ones to this node's storage engine

KVNode is still a state machine in the same sense as raft.Node: no clock, no
sockets, no threads. Whoever runs it (a test, the simulator, later a real
server) calls tick(), step() and take_messages(), and hands it client
requests with submit() and read().

Writes: submit() gives the command to Raft. When that log index is applied,
the result comes out of take_results(). If another command ends up at that
index instead (we lost leadership before it committed), the command is
dropped without an answer. The client times out and sends it again, and its
session makes sure it runs once however many times it was sent.

Reads: read() starts a ReadIndex (see raft/read_index.py). Once Raft has
confirmed it and the machine has applied up to the read index, the value
comes out of take_read_results().

After a restart Raft hands out committed entries from the start of its log
again. The machine already applied some of them before the crash, and knows
which (its applied index is saved with the data), so those are skipped.

Snapshots: once `snapshot_every` entries were applied since the log's base,
the machine's data goes to Raft as a snapshot and Raft drops the log up to
there (see raft/snapshot.py). That is what keeps the log from growing
forever.

Folders: <directory>/raft for Raft's term, vote and log, and
<directory>/data for the storage engine.
"""

import os
from typing import Self

from faultline.fs import FileSystem
from faultline.kv.commands import Command, encode
from faultline.kv.state_machine import Result, StateMachine
from faultline.lsm import Options
from faultline.raft.messages import Message, NodeId
from faultline.raft.node import Config, Node


class KVNode:
    def __init__(self, raft: Node, machine: StateMachine, snapshot_every: int = 1000) -> None:
        # Use KVNode.open()
        self.raft = raft
        self.machine = machine
        self._snapshot_every = snapshot_every
        self._waiting: dict[int, Command] = {}  # log index -> command submitted here
        self._results: list[tuple[Command, Result]] = []
        self._read_keys: dict[int, bytes] = {}  # ctx -> key, reads not answered yet
        self._confirmed: list[tuple[int, int]] = []  # (ctx, read_index) waiting to be applied
        self._read_results: list[tuple[int, bytes | None]] = []

    @classmethod
    def open(
        cls,
        fs: FileSystem,
        directory: str,
        node_id: NodeId,
        peers: list[NodeId],
        config: Config | None = None,
        seed: int = 0,
        options: Options | None = None,
        snapshot_every: int = 1000,
    ) -> Self:
        if not fs.exists(directory):
            fs.mkdir(directory)
            fs.sync_dir(os.path.dirname(os.path.normpath(directory)) or ".")
        raft = Node.open(fs, os.path.join(directory, "raft"), node_id, peers, config, seed)
        machine = StateMachine.open(fs, os.path.join(directory, "data"), options)
        node = cls(raft, machine, snapshot_every)
        node._drain()
        return node

    @property
    def id(self) -> NodeId:
        return self.raft.id

    def submit(self, command: Command) -> bool:
        """Start a write. False if this node is not the leader."""
        index = self.raft.propose(encode(command))
        if index is None:
            return False
        self._waiting[index] = command
        self._drain()
        return True

    def read(self, key: bytes, ctx: int) -> bool:
        """Start a safe read of `key`. False if this node can't serve reads
        right now (not the leader, or a brand new one)."""
        if not self.raft.read_index(ctx):
            return False
        self._read_keys[ctx] = key
        self._drain()
        return True

    def tick(self) -> None:
        self.raft.tick()
        self._drain()

    def step(self, message: Message) -> None:
        self.raft.step(message)
        self._drain()

    def take_messages(self) -> list[Message]:
        return self.raft.take_messages()

    def take_results(self) -> list[tuple[Command, Result]]:
        """Writes submitted here that have been applied, with their results."""
        results, self._results = self._results, []
        return results

    def take_read_results(self) -> list[tuple[int, bytes | None]]:
        """Reads that are done, as (ctx, value). None means the key is missing."""
        results, self._read_results = self._read_results, []
        return results

    def close(self) -> None:
        self.raft.log.close()
        self.machine.close()

    def _drain(self) -> None:
        """Do everything Raft's last step made possible."""
        self._apply_committed()
        self._answer_reads()
        self._maybe_snapshot()

    def _apply_committed(self) -> None:
        for index, entry in self.raft.take_committed():
            if index <= self.machine.applied_index:
                continue  # applied before a restart
            result = self.machine.apply(index, entry.command)
            command = self._waiting.pop(index, None)
            if command is None:
                continue  # someone else's command, or a no-op
            # Our command, or another one that took its place after a change
            # of leader. Only answer for our own.
            if entry.command == encode(command) and result is not None:
                self._results.append((command, result))

    def _maybe_snapshot(self) -> None:
        applied = self.machine.applied_index
        # Only once Raft has caught up with the machine after a restart, so
        # the snapshot and Raft agree on what index it is at
        if applied != self.raft.last_applied:
            return
        if applied - self.raft.log.base_index >= self._snapshot_every:
            self.raft.compact(applied, self.machine.snapshot())

    def _answer_reads(self) -> None:
        self._confirmed += self.raft.take_reads()
        still_waiting = []
        for ctx, read_index in self._confirmed:
            if self.machine.applied_index >= read_index:
                key = self._read_keys.pop(ctx)
                self._read_results.append((ctx, self.machine.get(key)))
            else:
                still_waiting.append((ctx, read_index))
        self._confirmed = still_waiting
