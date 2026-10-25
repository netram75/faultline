"""The whole database under fire: clients that retry, a network that loses,
repeats and reorders messages and splits in two, and nodes that crash, some
of them in the middle of a disk write.

Each client owns one key and is the only one that writes it. That gives a
simple and strict check that needs no model of the cluster at all: the client
knows the value its last finished write left there, so

* every write it gets an answer for must say "done" (a compare-and-set from
  the value the client knows to a new one can only fail if something went
  wrong, like a retry that ran twice),
* every read must return exactly that value (an older one means a stale
  read, a newer one means a write that was never answered ran anyway),
* at the end, every node has exactly that value.

The full check, for clients that share keys, is the linearizability checker
in src/faultline/checker.
"""

import random

import pytest

from faultline.kv.commands import Command, CompareAndSet
from faultline.raft.node import Config
from tests.kv.cluster import KVCluster

TIMEOUT = 30  # ticks a client waits for an answer before sending again


class Client:
    def __init__(self, client_id: int, rng: random.Random) -> None:
        self.id = client_id
        self.key = b"key%d" % client_id
        self.rng = rng
        self.value: bytes | None = None  # what the last finished write left
        self.seq = 0
        self.writes_done = 0
        self.reads_done = 0
        self.pending: Command | None = None
        self.read_ctx: int | None = None
        self.waited = 0

    def act(self, cluster: KVCluster) -> None:
        """Start a new request, or send the waiting one again after a timeout."""
        if self.pending is None and self.read_ctx is None:
            if self.value is not None and self.rng.random() < 0.3:
                self.read_ctx = self.rng.randrange(2**62)
            else:
                self.seq += 1
                new = b"%d-%d" % (self.id, self.seq)
                self.pending = Command(self.id, self.seq, CompareAndSet(self.key, self.value, new))
            self.waited = TIMEOUT  # send right away
        self.waited += 1
        if self.waited < TIMEOUT:
            return
        # Any node may be asked, a stale leader included. A node that is not
        # leader says no straight away, and we ask another one next tick.
        node = self.rng.choice(cluster.ids)
        if self.pending is not None:
            accepted = cluster.submit(node, self.pending)
        else:
            # A new ctx for every try, so a late answer to an old try is ignored
            self.read_ctx = self.rng.randrange(2**62)
            accepted = cluster.read(node, self.key, self.read_ctx)
        self.waited = 0 if accepted else TIMEOUT - 1

    def check_answers(self, cluster: KVCluster) -> None:
        for command, result in cluster.results:
            if command.client != self.id:
                continue
            assert isinstance(command.op, CompareAndSet)
            # A retry gets the first answer, so every answer must be a yes
            assert result.ok, f"client {self.id} seq {command.seq}: {result}"
            if self.pending is not None and command.seq == self.pending.seq:
                self.value = command.op.value
                self.pending = None
                self.writes_done += 1
        if self.read_ctx is not None and self.read_ctx in cluster.read_results:
            value = cluster.read_results.pop(self.read_ctx)
            assert value == self.value, f"client {self.id} read {value!r}, expected {self.value!r}"
            self.read_ctx = None
            self.reads_done += 1


def run(
    seed: int, rounds: int, faults: bool, config: Config | None = None
) -> tuple[KVCluster, list[Client]]:
    rng = random.Random(seed)
    cluster = KVCluster(
        5,
        seed,
        config,
        drop_rate=0.05 if faults else 0.0,
        repeat_rate=0.05 if faults else 0.0,
        snapshot_every=20,
    )
    clients = [Client(i, random.Random(rng.randrange(2**32))) for i in range(1, 4)]
    for _ in range(rounds):
        if faults:
            make_trouble(cluster, rng)
        for client in clients:
            client.act(cluster)
        cluster.tick()
        for client in clients:
            client.check_answers(cluster)
        cluster.results.clear()
    return cluster, clients


def make_trouble(cluster: KVCluster, rng: random.Random) -> None:
    roll = rng.random()
    up = [i for i in cluster.ids if i not in cluster.down]
    if roll < 0.01:
        side = set(rng.sample(cluster.ids, rng.randint(1, 2)))
        cluster.partition(side)
    elif roll < 0.03:
        cluster.heal()
    elif roll < 0.035 and len(up) > 3:
        cluster.crash(rng.choice(up))
    elif roll < 0.04 and len(up) > 3:
        cluster.cut_power(rng.choice(up), rng.randrange(1, 40))
    elif roll < 0.06 and cluster.down:
        cluster.restart(rng.choice(sorted(cluster.down)))


def settle(cluster: KVCluster, clients: list[Client]) -> None:
    """Heal everything and let every client finish what it started."""
    cluster.heal()
    cluster.drop_rate = cluster.repeat_rate = 0.0
    for i in sorted(cluster.down):
        cluster.restart(i)
    for _ in range(2000):
        if all(c.pending is None and c.read_ctx is None for c in clients):
            break
        for client in clients:
            if client.pending is not None or client.read_ctx is not None:
                client.act(cluster)
        cluster.tick()
        for client in clients:
            client.check_answers(cluster)
        cluster.results.clear()
    assert all(c.pending is None and c.read_ctx is None for c in clients), "clients stuck"
    cluster.tick(20)


def check_final_data(cluster: KVCluster, clients: list[Client]) -> None:
    cluster.check_same_data([c.key for c in clients])
    for i in cluster.ids:
        node = cluster.nodes[i]
        if node.machine.applied_index == max(
            n.machine.applied_index for n in cluster.nodes.values()
        ):
            for client in clients:
                assert node.machine.get(client.key) == client.value, (
                    f"node {i} has {node.machine.get(client.key)!r} for client {client.id}"
                )


@pytest.mark.parametrize("seed", range(5))
def test_quiet_network(seed: int) -> None:
    cluster, clients = run(seed, rounds=400, faults=False)
    settle(cluster, clients)

    check_final_data(cluster, clients)
    assert all(c.writes_done > 5 and c.reads_done > 0 for c in clients)


@pytest.mark.parametrize("seed", range(30))
def test_everything_goes_wrong(seed: int) -> None:
    cluster, clients = run(seed, rounds=800, faults=True)
    settle(cluster, clients)

    check_final_data(cluster, clients)
    assert sum(c.writes_done for c in clients) > 10


@pytest.mark.parametrize("seed", range(15))
def test_stale_leaders_hang_around(seed: int) -> None:
    # Without check quorum a leader that was cut off keeps thinking it leads
    # for as long as the split lasts, and clients keep asking it. Its data
    # goes stale as soon as the other side commits something. Only ReadIndex
    # stops it from answering reads with that stale data.
    cluster, clients = run(seed, rounds=800, faults=True, config=Config(check_quorum=False))
    settle(cluster, clients)

    check_final_data(cluster, clients)
