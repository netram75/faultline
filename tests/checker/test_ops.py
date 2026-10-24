import pytest

from faultline.checker.ops import BadHistory, Op, operations
from faultline.sim.history import Event, EventType, History
from faultline.sim.world import World


def history(*events: Event) -> History:
    h = History()
    h.events = list(events)
    return h


def put(client: int, type: EventType, value: str, time: int, key: str = "k") -> Event:
    return Event(client, type, "put", key, value, None, True if type == "ok" else None, time)


def get(client: int, type: EventType, value: str | None, time: int, key: str = "k") -> Event:
    return Event(client, type, "get", key, value, None, None, time)


def test_pairs_each_invoke_with_its_answer() -> None:
    ops = operations(
        history(
            put(1, "invoke", "a", 10),
            get(2, "invoke", None, 12),
            put(1, "ok", "a", 20),
            get(2, "ok", "a", 25),
        )
    )

    assert ops == [
        Op(0, 1, "put", "k", "a", None, True, 0, 2, 10),
        Op(1, 2, "get", "k", "a", None, None, 1, 3, 12),
    ]


def test_cas_keeps_expected_and_result() -> None:
    invoke = Event(3, "invoke", "cas", "k", "new", "old", None, 5)
    ok = Event(3, "ok", "cas", "k", "new", "old", False, 9)

    assert operations(history(invoke, ok)) == [Op(0, 3, "cas", "k", "new", "old", False, 0, 1, 5)]


def test_failed_operations_are_dropped() -> None:
    ops = operations(
        history(
            put(1, "invoke", "a", 10),
            put(1, "fail", "a", 20),
            get(1, "invoke", None, 30),
            get(1, "fail", None, 40),
        )
    )

    assert ops == []


def test_write_with_no_answer_has_no_end() -> None:
    ops = operations(history(put(1, "invoke", "a", 10), put(1, "info", "a", 300)))

    assert ops == [Op(0, 1, "put", "k", "a", None, None, 0, None, 10)]


def test_read_with_no_answer_is_dropped() -> None:
    ops = operations(history(get(1, "invoke", None, 10), get(1, "info", None, 300)))

    assert ops == []


def test_operations_still_running_at_the_end() -> None:
    ops = operations(
        history(put(1, "invoke", "a", 10), get(2, "invoke", None, 11), put(3, "invoke", "c", 12))
    )

    # The two writes may still happen, the read told us nothing
    assert ops == [
        Op(0, 1, "put", "k", "a", None, None, 0, None, 10),
        Op(1, 3, "put", "k", "c", None, None, 2, None, 12),
    ]


def test_sorted_by_start_not_by_end() -> None:
    ops = operations(
        history(
            put(1, "invoke", "slow", 10),
            put(2, "invoke", "fast", 11),
            put(2, "ok", "fast", 12),
            put(1, "ok", "slow", 50),
        )
    )

    assert [(op.id, op.value) for op in ops] == [(0, "slow"), (1, "fast")]


def test_two_invokes_in_a_row() -> None:
    with pytest.raises(BadHistory, match="started twice"):
        operations(history(put(1, "invoke", "a", 10), put(1, "invoke", "b", 11)))


def test_end_without_invoke() -> None:
    with pytest.raises(BadHistory, match="never started"):
        operations(history(put(1, "ok", "a", 10)))


def test_end_does_not_match_invoke() -> None:
    with pytest.raises(BadHistory, match="started a put on k but ended a put on j"):
        operations(history(put(1, "invoke", "a", 10), put(1, "ok", "a", 20, key="j")))


def test_end_before_invoke() -> None:
    with pytest.raises(BadHistory, match="before it started"):
        operations(history(put(1, "invoke", "a", 10), put(1, "ok", "a", 5)))


def test_same_millisecond_still_has_an_order() -> None:
    ops = operations(
        history(
            put(1, "invoke", "a", 10),
            put(1, "ok", "a", 20),
            get(1, "invoke", None, 20),
            get(1, "ok", "a", 30),
        )
    )

    # The get started after the put ended, even in the same ms
    assert ops[0].end is not None
    assert ops[0].end < ops[1].start


def test_history_from_a_real_run() -> None:
    world = World(seed=3)
    world.start()
    world.run_until(5000)

    ops = operations(world.history)

    assert ops
    assert [op.id for op in ops] == list(range(len(ops)))
    assert [op.start for op in ops] == sorted(op.start for op in ops)
    for op in ops:
        assert op.end is None or op.end > op.start
        assert op.end is not None or op.kind != "get"
