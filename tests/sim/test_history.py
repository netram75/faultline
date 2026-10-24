from pathlib import Path

from faultline.kv.state_machine import Result
from faultline.sim.client import Operation, Outcome
from faultline.sim.history import Event, History
from faultline.sim.network import NetworkFaults
from faultline.sim.world import World


def test_events_for_each_kind_of_operation() -> None:
    history = History()
    cas = Operation("cas", b"k0", value=b"1-2", expected=b"1-1")

    history.invoke(1, cas)
    history.complete(1, cas, Outcome.OK, Result(ok=False, value=b"3-1"))
    get = Operation("get", b"k0")
    history.invoke(2, get)
    history.complete(2, get, Outcome.OK, b"3-1")
    put = Operation("put", b"k1", value=b"1-3")
    history.invoke(1, put)
    history.complete(1, put, Outcome.UNKNOWN, None)

    assert history.events == [
        Event(1, "invoke", "cas", "k0", "1-2", "1-1", None, 0),
        Event(1, "ok", "cas", "k0", "1-2", "1-1", False, 0),
        Event(2, "invoke", "get", "k0", None, None, None, 0),
        Event(2, "ok", "get", "k0", "3-1", None, None, 0),
        Event(1, "invoke", "put", "k1", "1-3", None, None, 0),
        Event(1, "info", "put", "k1", "1-3", None, None, 0),
    ]


def test_jsonl_round_trip(tmp_path: Path) -> None:
    world = World(seed=1)
    world.start()
    world.run_until(2000)
    path = tmp_path / "history.jsonl"

    world.history.save(str(path))

    loaded = History.from_jsonl(path.read_text())
    assert loaded.events == world.history.events
    assert len(loaded.events) > 50


def test_history_of_a_run_is_well_formed() -> None:
    world = World(seed=2, faults=NetworkFaults(1, 20, drop_rate=0.1))
    world.start()
    world.run_until(5000)

    open_ops: dict[int, Event] = {}
    last_time = 0
    for event in world.history.events:
        assert event.time >= last_time  # in time order
        last_time = event.time
        if event.type == "invoke":
            # One operation at a time per client
            assert event.client not in open_ops
            open_ops[event.client] = event
        else:
            started = open_ops.pop(event.client)
            assert (started.op, started.key) == (event.op, event.key)
    assert {e.type for e in world.history.events} >= {"invoke", "ok"}


def test_same_seed_same_history() -> None:
    def digest(seed: int) -> str:
        world = World(seed, faults=NetworkFaults(1, 20, drop_rate=0.05, duplicate_rate=0.05))
        world.start()
        world.run_until(3000)
        return world.history.digest()

    assert digest(7) == digest(7)
    assert digest(7) != digest(8)
