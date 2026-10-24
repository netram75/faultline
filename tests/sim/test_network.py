from faultline.raft.messages import AppendReply, Message
from faultline.sim.loop import EventLoop
from faultline.sim.network import Network, NetworkFaults


def message(sender: int, receiver: int, n: int = 0) -> Message:
    return Message(sender, receiver, 1, AppendReply(success=True, match_index=n))


def setup(faults: NetworkFaults | None = None) -> tuple[EventLoop, Network, list[Message]]:
    loop = EventLoop(seed=1)
    arrived: list[Message] = []
    return loop, Network(loop, arrived.append, faults), arrived


def test_messages_arrive_after_some_latency() -> None:
    loop, network, arrived = setup(NetworkFaults(min_latency=5, max_latency=8))
    network.send(message(1, 2))

    loop.run_until(4)
    assert arrived == []
    loop.run_until(8)
    assert arrived == [message(1, 2)]
    assert network.delivered == 1


def test_messages_get_reordered() -> None:
    loop, network, arrived = setup(NetworkFaults(min_latency=1, max_latency=50))
    for n in range(100):
        network.send(message(1, 2, n))

    loop.run_until(100)

    order = [m.body.match_index for m in arrived if isinstance(m.body, AppendReply)]
    assert sorted(order) == list(range(100))
    assert order != list(range(100))


def test_drops_and_duplicates() -> None:
    loop, network, arrived = setup(NetworkFaults(drop_rate=0.2, duplicate_rate=0.2))
    for n in range(1000):
        network.send(message(1, 2, n))

    loop.run_until(100)

    assert 150 < network.dropped < 250
    assert 120 < network.duplicated < 200
    assert len(arrived) == 1000 - network.dropped + network.duplicated


def test_partition_drops_messages_between_groups() -> None:
    loop, network, arrived = setup()
    network.partition({1, 2}, {3})

    network.send(message(1, 2))
    network.send(message(1, 3))
    network.send(message(3, 2))
    loop.run_until(100)

    assert arrived == [message(1, 2)]
    assert network.dropped == 2


def test_a_bridge_talks_to_both_sides() -> None:
    _, network, _ = setup()
    network.partition({1, 2, 3}, {3, 4, 5})

    assert network.can_talk(1, 3) and network.can_talk(5, 3)
    assert not network.can_talk(1, 5)


def test_a_node_in_no_group_is_cut_off() -> None:
    _, network, _ = setup()
    network.partition({1, 2})

    assert not network.can_talk(3, 1)
    assert not network.can_talk(3, 4)


def test_messages_on_the_wire_are_lost_when_the_network_splits() -> None:
    loop, network, arrived = setup(NetworkFaults(min_latency=10, max_latency=10))
    network.send(message(1, 2))

    loop.run_until(5)
    network.partition({1}, {2})
    loop.run_until(20)

    assert arrived == []


def test_heal() -> None:
    loop, network, arrived = setup()
    network.partition({1}, {2})
    network.heal()

    network.send(message(1, 2))
    loop.run_until(100)

    assert arrived == [message(1, 2)]


def test_same_seed_same_traffic() -> None:
    def run() -> list[Message]:
        loop, network, arrived = setup(NetworkFaults(1, 30, 0.1, 0.1))
        for n in range(200):
            network.send(message(n % 3, 4, n))
        loop.run_until(100)
        return arrived

    assert run() == run()
