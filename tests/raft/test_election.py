"""Whole clusters electing leaders. Every run also checks that no term ever
has two leaders (see cluster.py)."""

import random

import pytest

from faultline.raft.node import Config, Role
from tests.raft.cluster import Cluster

NO_PRE_VOTE = Config(pre_vote=False)


@pytest.mark.parametrize("size", [1, 3, 5])
@pytest.mark.parametrize("seed", range(10))
def test_a_leader_is_elected(size: int, seed: int) -> None:
    cluster = Cluster(size, seed)

    leader = cluster.wait_for_leader()

    term = cluster.nodes[leader].term
    assert all(node.term == term and node.leader == leader for node in cluster.nodes.values())


@pytest.mark.parametrize("seed", range(10))
def test_leader_stays_while_the_network_is_fine(seed: int) -> None:
    cluster = Cluster(5, seed)
    leader = cluster.wait_for_leader()
    term = cluster.nodes[leader].term

    cluster.tick(300)

    assert cluster.leader() == leader
    assert all(node.term == term for node in cluster.nodes.values())


@pytest.mark.parametrize("seed", range(10))
def test_new_leader_after_the_leader_crashes(seed: int) -> None:
    cluster = Cluster(5, seed)
    old = cluster.wait_for_leader()
    old_term = cluster.nodes[old].term

    cluster.crash(old)
    new = cluster.wait_for_leader()

    assert new != old
    assert cluster.nodes[new].term > old_term


@pytest.mark.parametrize("seed", range(10))
def test_a_minority_never_elects_a_leader(seed: int) -> None:
    cluster = Cluster(5, seed)
    leader = cluster.wait_for_leader()
    minority = set(random.Random(seed).sample([i for i in cluster.ids if i != leader], 2))

    cluster.partition(minority, set(cluster.ids) - minority)
    cluster.tick(300)

    assert all(cluster.nodes[i].role is not Role.LEADER for i in minority)
    assert cluster.leader() == leader


@pytest.mark.parametrize("seed", range(10))
def test_cut_off_node_comes_back_without_a_fuss(seed: int) -> None:
    # With pre-vote, a node that was cut off can't win pre-votes, so its term
    # never moves, and coming back changes nothing
    cluster = Cluster(5, seed)
    leader = cluster.wait_for_leader()
    term = cluster.nodes[leader].term
    loner = next(i for i in cluster.ids if i != leader)

    cluster.partition({loner})
    cluster.tick(300)
    assert cluster.nodes[loner].term == term
    cluster.heal()
    cluster.tick(100)

    assert cluster.leader() == leader
    assert all(node.term == term for node in cluster.nodes.values())


def test_without_pre_vote_a_cut_off_node_forces_an_election() -> None:
    # The problem pre-vote is there to fix
    cluster = Cluster(5, 0, NO_PRE_VOTE)
    leader = cluster.wait_for_leader()
    term = cluster.nodes[leader].term
    loner = next(i for i in cluster.ids if i != leader)

    cluster.partition({loner})
    cluster.tick(300)
    assert cluster.nodes[loner].term > term + 5
    cluster.heal()
    cluster.wait_for_leader()

    assert all(node.term > term + 5 for node in cluster.nodes.values())


@pytest.mark.parametrize("pre_vote", [True, False])
@pytest.mark.parametrize("seed", range(150))
def test_one_leader_per_term_through_chaos(seed: int, pre_vote: bool) -> None:
    """Lost, late and repeated messages, partitions that come and go, and
    nodes crashing and restarting. Election safety is checked all along."""
    rng = random.Random(seed)
    cluster = Cluster(
        rng.choice([3, 5]),
        seed,
        Config(election_ticks=rng.randint(4, 10), heartbeat_ticks=1, pre_vote=pre_vote),
        drop_rate=rng.uniform(0, 0.3),
        delay_rate=rng.uniform(0, 0.3),
        repeat_rate=0.1,
        reboot_rate=0.05,
    )
    for _ in range(60):
        event = rng.random()
        if event < 0.3:
            cluster.crash(rng.choice(cluster.ids), keep_names=rng.random() < 0.5)
        elif event < 0.6:
            for i in list(cluster.down):
                cluster.restart(i)
        elif event < 0.8:
            nodes = cluster.ids[:]
            rng.shuffle(nodes)
            cut = rng.randint(1, len(nodes) - 1)
            cluster.partition(set(nodes[:cut]), set(nodes[cut:]))
        else:
            cluster.heal()
        cluster.tick(rng.randint(1, 15))

    # When the trouble stops, a leader must still get elected
    cluster.heal()
    for i in list(cluster.down):
        cluster.restart(i)
    cluster.drop_rate = cluster.delay_rate = 0.0
    cluster.wait_for_leader(max_rounds=1000)
