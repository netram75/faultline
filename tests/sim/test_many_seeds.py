"""The simulator's promises, tested: the same seed gives the same run, and
no seed ever breaks a safety rule, gets the cluster stuck, or gives clients
answers that are not linearizable.

Each run here is short, so the whole file takes a few seconds. Longer and
many more runs are for the command line: python -m faultline.sim --runs 1000
"""

import pytest

from faultline.sim.run import simulate


def test_same_seed_same_run() -> None:
    first, _, first_nemesis = simulate(seed=42, seconds=10)
    second, _, second_nemesis = simulate(seed=42, seconds=10)

    assert first == second
    assert first_nemesis.log == second_nemesis.log


def test_different_seeds_different_runs() -> None:
    digests = {simulate(seed, seconds=3)[0].history_digest for seed in range(5)}

    assert len(digests) == 5


@pytest.mark.parametrize("seed", range(40))
def test_heavy_faults_five_nodes(seed: int) -> None:
    summary, _, _ = simulate(seed, seconds=10, faults="heavy")

    assert not summary.stuck
    assert summary.linearizable == "ok"


@pytest.mark.parametrize("seed", range(20))
def test_heavy_faults_three_nodes(seed: int) -> None:
    # With three nodes, one down and one cut off is already no majority
    summary, _, _ = simulate(seed + 1000, seconds=10, faults="heavy", nodes=3)

    assert not summary.stuck
    assert summary.linearizable == "ok"


@pytest.mark.parametrize("seed", range(100))
def test_reboot_storm(seed: int) -> None:
    # Many short runs find more than a few long ones: most bugs show up in
    # the first seconds, if they show up at all
    summary, _, _ = simulate(seed + 3000, seconds=3, faults="reboots")

    assert not summary.stuck
    assert summary.linearizable == "ok"


@pytest.mark.parametrize("seed", range(10))
def test_light_faults_many_clients(seed: int) -> None:
    summary, _, _ = simulate(seed + 2000, seconds=10, faults="light", clients=8, keys=2)

    assert not summary.stuck
    assert summary.linearizable == "ok"
    assert summary.ok > 300
