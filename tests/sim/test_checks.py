import pytest

from faultline.kv.commands import Command, Put
from faultline.raft.messages import Entry
from faultline.raft.node import Role
from faultline.sim.checks import SafetyViolation
from faultline.sim.nemesis import PROFILES, Nemesis
from faultline.sim.world import World


@pytest.mark.parametrize("seed", range(5))
def test_heavy_faults_break_no_rule(seed: int) -> None:
    world = World(seed)
    nemesis = Nemesis(world, PROFILES["heavy"])
    world.start()
    nemesis.start()

    world.run_until(10_000)  # raises SafetyViolation if a rule breaks

    assert world.checker.checks > 1000


def running_world() -> tuple[World, int, int]:
    world = World(seed=1, clients=0)
    world.start()
    world.run_until(1000)
    leader = world.leader()
    assert leader is not None
    for seq in range(1, 4):
        world.nodes[leader].submit(Command(1, seq, Put(b"k", b"v%d" % seq)))
    world.run_until(1500)
    follower = next(i for i in world.nodes if i != leader)
    return world, leader, follower


def test_catches_two_leaders_in_one_term() -> None:
    world, _, follower = running_world()
    node = world.nodes[follower].kv
    assert node is not None

    node.raft.role = Role.LEADER  # a bug that makes a follower think it won

    with pytest.raises(SafetyViolation, match="two leaders in term 1"):
        world.checker.check(follower)


def test_catches_a_committed_entry_that_changed() -> None:
    world, _, follower = running_world()
    world.nodes[follower].crash()
    world.nodes[follower].start()
    node = world.nodes[follower].kv
    assert node is not None

    # A bug in recovery that brought back a different command at index 2.
    # Once the leader tells it index 2 is committed, the checker sees it.
    node.raft.log._entries[1] = Entry(1, b"not what was committed")

    with pytest.raises(SafetyViolation, match="at index 2"):
        world.run_until(3000)


def test_message_has_the_seed_and_time() -> None:
    world, _, follower = running_world()
    node = world.nodes[follower].kv
    assert node is not None
    node.raft.role = Role.LEADER

    with pytest.raises(SafetyViolation, match=r"^seed 1, at 1500 ms: "):
        world.checker.check(follower)
