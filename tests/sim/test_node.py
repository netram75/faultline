from faultline.kv.commands import Command, Put
from faultline.kv.state_machine import Result
from faultline.raft.messages import Message, NodeId
from faultline.raft.node import Role
from faultline.sim.loop import EventLoop
from faultline.sim.network import Network
from faultline.sim.node import SimNode


class Little:
    """Three SimNodes on a network, wired up by hand."""

    def __init__(self, seed: int = 1) -> None:
        self.loop = EventLoop(seed)
        self.network = Network(self.loop, self._deliver)
        ids = [1, 2, 3]
        self.nodes = {i: SimNode(self.loop, self.network.send, i, ids) for i in ids}
        self.results: list[tuple[Command, Result]] = []
        for node in self.nodes.values():
            node.on_result = lambda command, result: self.results.append((command, result))
            node.start()

    def _deliver(self, message: Message) -> None:
        self.nodes[message.receiver].receive(message)

    def run(self, ms: int) -> None:
        self.loop.run_until(self.loop.now + ms)

    def leader(self) -> NodeId | None:
        for i, node in self.nodes.items():
            if node.kv is not None and node.kv.raft.role is Role.LEADER:
                return i
        return None


def test_cluster_elects_a_leader_and_writes() -> None:
    little = Little()
    little.run(1000)
    leader = little.leader()
    assert leader is not None

    put = Command(1, 1, Put(b"k", b"v"))
    assert little.nodes[leader].submit(put)
    little.run(200)

    assert little.results == [(put, Result(ok=True))]
    for node in little.nodes.values():
        assert node.kv is not None and node.kv.machine.get(b"k") == b"v"


def test_crashed_node_comes_back_with_its_data() -> None:
    little = Little()
    little.run(1000)
    leader = little.leader()
    assert leader is not None
    little.nodes[leader].submit(Command(1, 1, Put(b"k", b"v")))
    little.run(200)

    little.nodes[leader].crash()
    assert not little.nodes[leader].up
    little.run(1000)
    new = little.leader()
    assert new is not None and new != leader

    little.nodes[leader].start()
    little.run(1000)

    node = little.nodes[leader].kv
    assert node is not None and node.machine.get(b"k") == b"v"
    assert little.nodes[leader].crashes == 1


def test_power_cut_takes_the_node_down_mid_write() -> None:
    little = Little()
    little.run(1000)
    leader = little.leader()
    assert leader is not None
    victim = little.nodes[1]
    victim.cut_power(3)
    # Heartbeats don't touch the disk. Writes do, on every node.
    for seq in range(1, 6):
        little.nodes[leader].submit(Command(1, seq, Put(b"k", b"v%d" % seq)))
        little.run(100)

    assert not victim.up
    assert victim.power_cuts == 1

    victim.start()
    little.run(1000)
    assert victim.up


def test_a_slow_clock_ticks_less() -> None:
    little = Little()
    little.nodes[2].skew = 2.0
    little.run(1000)

    assert little.nodes[1].ticks == 100
    assert little.nodes[2].ticks == 50


def test_no_extra_ticks_after_a_restart() -> None:
    little = Little()
    little.run(500)
    node = little.nodes[3]
    node.crash()
    node.start()
    before = node.ticks

    little.run(1000)

    assert node.ticks - before == 100
