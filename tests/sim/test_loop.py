import random
from functools import partial

from faultline.sim.loop import EventLoop


def test_events_run_in_time_order() -> None:
    loop = EventLoop(seed=1)
    ran: list[str] = []
    loop.at(30, lambda: ran.append("c"))
    loop.at(10, lambda: ran.append("a"))
    loop.at(20, lambda: ran.append("b"))

    loop.run_until(100)

    assert ran == ["a", "b", "c"]
    assert loop.now == 100


def test_same_time_runs_in_the_order_scheduled() -> None:
    loop = EventLoop(seed=1)
    ran: list[int] = []
    for i in range(10):
        loop.at(5, partial(ran.append, i))

    loop.run_until(5)

    assert ran == list(range(10))


def test_clock_jumps_to_each_event() -> None:
    loop = EventLoop(seed=1)
    seen: list[int] = []
    loop.at(7, lambda: seen.append(loop.now))
    loop.at(1000, lambda: seen.append(loop.now))

    loop.run_until(2000)

    assert seen == [7, 1000]


def test_run_until_stops_at_the_time_given() -> None:
    loop = EventLoop(seed=1)
    ran: list[int] = []
    loop.at(10, lambda: ran.append(10))
    loop.at(11, lambda: ran.append(11))

    loop.run_until(10)

    assert ran == [10]
    assert loop.now == 10
    assert loop.pending == 1


def test_events_can_schedule_more_events() -> None:
    loop = EventLoop(seed=1)
    ticks: list[int] = []

    def tick() -> None:
        ticks.append(loop.now)
        loop.after(10, tick)

    loop.after(10, tick)
    loop.run_until(55)

    assert ticks == [10, 20, 30, 40, 50]


def test_a_time_in_the_past_means_now() -> None:
    loop = EventLoop(seed=1)
    loop.run_until(100)
    seen: list[int] = []

    loop.at(5, lambda: seen.append(loop.now))
    loop.run_until(100)

    assert seen == [100]


def test_seeds_depend_on_the_run_and_the_name_only() -> None:
    a, b = EventLoop(seed=7), EventLoop(seed=7)

    assert a.seed_for("node1") == b.seed_for("node1")
    assert a.seed_for("node1") != a.seed_for("node2")
    assert a.seed_for("node1") != EventLoop(seed=8).seed_for("node1")


def test_same_seed_same_run() -> None:
    def run(seed: int) -> list[tuple[int, int]]:
        loop = EventLoop(seed)
        rng = random.Random(loop.seed_for("test"))
        log: list[tuple[int, int]] = []

        def note(i: int) -> None:
            log.append((loop.now, i))

        for i in range(50):
            loop.at(rng.randrange(1000), partial(note, i))
        loop.run_until(1000)
        return log

    assert run(3) == run(3)
    assert run(3) != run(4)
