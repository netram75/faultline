"""A whole simulated cluster: nodes, the network between them, and clients.

World only wires the parts together:

    node -> network -> node        Raft messages (network.py)
    client -> node                 requests: submit a write, start a read
    node -> client                 answers, routed by client id or read ctx
    client -> history              every operation's start and end (history.py)
    node -> checker                after every step, Raft's safety rules (checks.py)

Requests and answers between clients and nodes take a few milliseconds
too. An answer can be lost on the way, at the network's drop rate, which is
one of the ways a client ends up not knowing whether its write happened.
"""

import random
from functools import partial

from faultline.kv.commands import Command
from faultline.kv.state_machine import Result
from faultline.raft.messages import Message, NodeId
from faultline.raft.node import Config, Role
from faultline.sim.checks import SafetyChecker
from faultline.sim.client import CTX_SHIFT, ClientTimeouts, SimClient
from faultline.sim.history import History
from faultline.sim.loop import EventLoop
from faultline.sim.network import Network, NetworkFaults
from faultline.sim.node import SimNode


class World:
    def __init__(
        self,
        seed: int,
        nodes: int = 5,
        clients: int = 3,
        keys: int = 3,
        config: Config | None = None,
        faults: NetworkFaults | None = None,
        timeouts: ClientTimeouts | None = None,
        snapshot_every: int = 200,
    ) -> None:
        self.loop = EventLoop(seed)
        self.network = Network(self.loop, self._deliver, faults)
        ids = list(range(1, nodes + 1))
        self.nodes = {
            i: SimNode(self.loop, self.network.send, i, ids, config, snapshot_every=snapshot_every)
            for i in ids
        }
        key_names = [b"k%d" % k for k in range(keys)]
        self.clients = {
            c: SimClient(self.loop, c, ids, self._submit, self._read, key_names, timeouts)
            for c in range(1, clients + 1)
        }
        self.history = History(self.loop)
        for client in self.clients.values():
            client.on_start = self.history.invoke
            client.on_done = self.history.complete
        self._rng = random.Random(self.loop.seed_for("answers"))
        self.checker = SafetyChecker(self.loop, self.nodes)
        for node in self.nodes.values():
            node.on_result = self._send_result
            node.on_read = self._send_read
            node.after_step = self.checker.check

    def start(self) -> None:
        for node in self.nodes.values():
            node.start()
        for client in self.clients.values():
            client.start()

    def run_until(self, time: int) -> None:
        self.loop.run_until(time)

    def leader(self) -> NodeId | None:
        """The running node that is leader in the newest term, if any."""
        leaders = [
            (node.kv.raft.term, i)
            for i, node in self.nodes.items()
            if node.kv is not None and node.kv.raft.role is Role.LEADER
        ]
        return max(leaders)[1] if leaders else None

    def _deliver(self, message: Message) -> None:
        self.nodes[message.receiver].receive(message)

    def _submit(self, node: NodeId, command: Command) -> bool:
        return self.nodes[node].submit(command)

    def _read(self, node: NodeId, key: bytes, ctx: int) -> bool:
        return self.nodes[node].read(key, ctx)

    def _send_result(self, command: Command, result: Result) -> None:
        client = self.clients.get(command.client)
        if client is None or self._answer_lost():
            return  # lost, or a request made by hand (in a test)
        self.loop.after(self._rng.randint(1, 5), partial(client.answer, command, result))

    def _send_read(self, ctx: int, value: bytes | None) -> None:
        client = self.clients.get(ctx >> CTX_SHIFT)
        if client is None or self._answer_lost():
            return
        self.loop.after(self._rng.randint(1, 5), partial(client.answer_read, ctx, value))

    def _answer_lost(self) -> bool:
        return self._rng.random() < self.network.faults.drop_rate
