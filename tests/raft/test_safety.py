"""Whole clusters copying a log while things go wrong. The network checks
Raft's safety rules all the time (see cluster.py), and at the end every
node must have the same log and must have applied the same commands."""

import random

import pytest

from faultline.raft.node import Config
from tests.raft.cluster import Cluster


def wait_until_applied_everywhere(cluster: Cluster, index: int, max_rounds: int = 2000) -> None:
    for _ in range(max_rounds):
        if all(node.last_applied >= index for node in cluster.nodes.values()):
            return
        cluster.tick()
    raise AssertionError(f"entry {index} was not applied everywhere after {max_rounds} rounds")


def commit_everywhere(cluster: Cluster, command: bytes, max_rounds: int = 2000) -> int:
    """Act like a client: hand the command to the leader, and hand it again
    now and then, since a leader can take a command and lose it before it is
    committed. Once it is committed, wait until every node applied it."""
    for round in range(max_rounds):
        for index, entry in cluster.committed.items():
            if entry.command == command:
                wait_until_applied_everywhere(cluster, index)
                return index
        if round % 20 == 0:
            cluster.propose(command)
        cluster.tick()
    raise AssertionError(f"{command!r} was never committed")


@pytest.mark.parametrize("size", [1, 3, 5])
def test_every_node_applies_every_command_in_order(size: int) -> None:
    cluster = Cluster(size, seed=1)
    cluster.wait_for_leader()

    for i in range(50):
        cluster.propose(f"set x {i}".encode())
        cluster.tick()
    commit_everywhere(cluster, b"last")

    commands = [cluster.committed[i].command for i in sorted(cluster.committed)]
    assert [c for c in commands if c.startswith(b"set")] == [
        f"set x {i}".encode() for i in range(50)
    ]
    cluster.check_logs()


def test_a_follower_that_was_down_catches_up() -> None:
    cluster = Cluster(3, seed=2, config=Config(max_entries_per_message=8))
    leader = cluster.wait_for_leader()
    follower = next(i for i in cluster.ids if i != leader)

    cluster.crash(follower)
    for i in range(100):
        cluster.propose(f"cmd{i}".encode())
        cluster.tick()
    cluster.restart(follower)
    commit_everywhere(cluster, b"last")

    cluster.check_logs()
    assert cluster.nodes[follower].log.entries_from(1) == cluster.nodes[leader].log.entries_from(1)


def test_a_cut_off_leader_loses_its_uncommitted_entries() -> None:
    cluster = Cluster(5, seed=3)
    old = cluster.wait_for_leader()

    cluster.partition({old})
    assert cluster.propose(b"lost") is not None  # the old leader takes it, but can't commit it
    cluster.tick(200)
    new = cluster.leader()
    assert new is not None and new != old
    cluster.nodes[new].propose(b"kept")
    cluster.tick(50)
    cluster.heal()
    commit_everywhere(cluster, b"kept")

    commands = [entry.command for entry in cluster.committed.values()]
    assert b"kept" in commands
    assert b"lost" not in commands
    cluster.check_logs()


def test_figure_8() -> None:
    """The case from figure 8 of the Raft paper, step by step: an entry from
    an old term sits on a majority, and a later leader still overwrites it.
    So the leader must not call it committed just because a majority has it.
    The network checks that every new leader has every committed entry, so
    a leader that counted copies of the old entry fails here."""
    config = Config(election_ticks=10, heartbeat_ticks=1, pre_vote=False, max_entries_per_message=1)
    cluster = Cluster(5, seed=0, config=config)
    nodes = cluster.nodes

    # (a) Node 1 leads. Everyone gets its no-op, then entry X reaches node 2 only.
    cluster.elect(1)
    for _ in range(5):
        cluster.deliver()
    cluster.partition({1, 2})
    x = nodes[1].propose(b"X")
    assert x == 2
    for _ in range(5):
        cluster.deliver()
    cluster.crash(1)

    # (b) Node 5 wins with votes from 3 and 4, and puts its own no-op at
    # index 2. Then it crashes before anyone sees it.
    cluster.partition({3, 4, 5})
    cluster.elect(5)
    cluster.crash(5)

    # (c) Node 1 comes back and wins with votes from 3 and 4. It copies X to
    # them, one entry per message. Now nodes 1 to 4 have X: a majority. We cut
    # node 1 off just before its own no-op from this term gets anywhere.
    cluster.restart(1)
    cluster.partition({1, 3, 4})
    cluster.elect(1)
    old_term = nodes[1].log.term_at(x)
    for _ in range(20):
        if all(nodes[i].log.matches(x, old_term) for i in (3, 4)):
            break
        cluster.deliver()
    cluster.deliver()  # node 1 hears that 3 and 4 have X
    assert all(nodes[i].log.last_index == x for i in (3, 4))
    cluster.crash(1)

    # (d) Node 5 comes back. Its last term is newer than anything 2, 3 and 4
    # have, so they vote for it, and it overwrites X everywhere. That is fine
    # only because X was never committed.
    cluster.restart(5)
    cluster.partition({2, 3, 4, 5})
    cluster.elect(5)
    for _ in range(10):
        cluster.deliver()

    assert all(nodes[i].log.term_at(x) != old_term for i in (2, 3, 4, 5))
    assert b"X" not in [entry.command for entry in cluster.committed.values()]


@pytest.mark.parametrize("pre_vote", [True, False])
@pytest.mark.parametrize("seed", range(100))
def test_replication_through_chaos(seed: int, pre_vote: bool) -> None:
    """Lost, late and repeated messages, partitions, crashes and quick
    reboots, with commands coming in all the time. Safety is checked all
    along. When the trouble stops, everything must come together."""
    rng = random.Random(seed)
    cluster = Cluster(
        rng.choice([3, 5]),
        seed,
        Config(
            election_ticks=rng.randint(4, 10),
            heartbeat_ticks=1,
            pre_vote=pre_vote,
            max_entries_per_message=rng.choice([1, 4, 64]),
        ),
        drop_rate=rng.uniform(0, 0.3),
        delay_rate=rng.uniform(0, 0.3),
        repeat_rate=0.1,
        reboot_rate=0.03,
    )
    proposed = 0
    for _ in range(50):
        event = rng.random()
        if event < 0.2:
            cluster.crash(rng.choice(cluster.ids), keep_names=rng.random() < 0.5)
        elif event < 0.4:
            for i in list(cluster.down):
                cluster.restart(i)
        elif event < 0.55:
            nodes = cluster.ids[:]
            rng.shuffle(nodes)
            cut = rng.randint(1, len(nodes) - 1)
            cluster.partition(set(nodes[:cut]), set(nodes[cut:]))
        elif event < 0.7:
            cluster.heal()
        for _ in range(rng.randint(1, 10)):
            if rng.random() < 0.5:
                cluster.propose(f"cmd{proposed}".encode())
                proposed += 1
            cluster.tick()

    cluster.heal()
    for i in list(cluster.down):
        cluster.restart(i)
    cluster.drop_rate = cluster.delay_rate = cluster.reboot_rate = 0.0
    last = commit_everywhere(cluster, b"last")

    cluster.check_logs()
    logs = [node.log.entries_from(1, last) for node in cluster.nodes.values()]
    assert all(log == logs[0] for log in logs)
