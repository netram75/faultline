from faultline.sim.client import Outcome
from faultline.sim.network import NetworkFaults
from faultline.sim.world import World


def test_clients_get_work_done() -> None:
    world = World(seed=1)
    world.start()

    world.run_until(5000)

    for client in world.clients.values():
        assert client.counts[Outcome.OK] > 30
        assert client.counts[Outcome.UNKNOWN] == 0


def test_work_goes_on_after_the_leader_crashes() -> None:
    world = World(seed=2)
    world.start()
    world.run_until(2000)
    leader = world.leader()
    assert leader is not None
    done_before = sum(c.counts[Outcome.OK] for c in world.clients.values())

    world.nodes[leader].crash()
    world.run_until(6000)

    assert world.leader() not in (None, leader)
    assert sum(c.counts[Outcome.OK] for c in world.clients.values()) > done_before + 50


def test_writes_to_a_cut_off_cluster_end_unknown_or_failed() -> None:
    world = World(seed=3)
    world.start()
    world.run_until(1000)

    # No node can reach any other: nothing can commit
    world.network.partition(*({i} for i in world.nodes))
    before = {c.id: dict(c.counts) for c in world.clients.values()}
    world.run_until(6000)

    for client in world.clients.values():
        assert client.counts[Outcome.OK] - before[client.id][Outcome.OK] <= 1
        stuck = client.counts[Outcome.UNKNOWN] + client.counts[Outcome.FAIL]
        assert stuck > before[client.id][Outcome.UNKNOWN] + before[client.id][Outcome.FAIL]


def test_lossy_network_still_makes_progress() -> None:
    world = World(seed=4, faults=NetworkFaults(1, 20, drop_rate=0.1, duplicate_rate=0.1))
    world.start()

    world.run_until(10_000)

    assert sum(c.counts[Outcome.OK] for c in world.clients.values()) > 100


def test_same_seed_same_world() -> None:
    def run() -> list[dict[Outcome, int]]:
        world = World(seed=5, faults=NetworkFaults(1, 20, drop_rate=0.05))
        world.start()
        world.run_until(3000)
        return [dict(c.counts) for c in world.clients.values()]

    assert run() == run()
