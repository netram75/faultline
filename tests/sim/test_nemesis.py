from faultline.sim.client import Outcome
from faultline.sim.nemesis import PROFILES, Faults, Nemesis
from faultline.sim.world import World


def run(seed: int, faults: Faults, until: int) -> tuple[World, Nemesis]:
    world = World(seed)
    nemesis = Nemesis(world, faults)
    world.start()
    nemesis.start()
    world.run_until(until)
    return world, nemesis


def test_no_faults_does_nothing() -> None:
    _, nemesis = run(1, PROFILES["none"], 5000)

    assert nemesis.log == []


def test_heavy_does_every_kind_of_fault() -> None:
    world, nemesis = run(2, PROFILES["heavy"], 20_000)
    said = " ".join(what for _, what in nemesis.log)

    for word in [
        "partition",
        "crash node",
        "reboot",
        "power cut",
        "loss burst",
        "clock",
        "restart",
    ]:
        assert word in said
    assert sum(node.crashes + node.power_cuts for node in world.nodes.values()) > 3


def test_never_more_than_max_down_nodes_down() -> None:
    world = World(seed=3)
    nemesis = Nemesis(
        world, Faults(mean_gap=100, partition=0, reboot=0, loss_burst=0, clock_skew=0)
    )
    world.start()
    nemesis.start()
    most_down = 0
    for time in range(0, 20_000, 10):
        world.run_until(time)
        most_down = max(most_down, sum(1 for n in world.nodes.values() if not n.up))

    # A power cut fires some time after it is planned, so one more may be
    # planned while another is still waiting. Never a majority though.
    assert 1 <= most_down <= 3


def test_cluster_recovers_after_calming_down() -> None:
    world, nemesis = run(4, PROFILES["heavy"], 10_000)

    nemesis.calm_down()
    ok_before = sum(c.counts[Outcome.OK] for c in world.clients.values())
    world.run_until(15_000)

    assert all(node.up for node in world.nodes.values())
    assert sum(c.counts[Outcome.OK] for c in world.clients.values()) > ok_before + 50


def test_same_seed_same_trouble() -> None:
    assert run(5, PROFILES["heavy"], 10_000)[1].log == run(5, PROFILES["heavy"], 10_000)[1].log
