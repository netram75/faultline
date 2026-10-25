from faultline import checker
from faultline.checker.check import check
from faultline.checker.ops import Op
from faultline.sim.run import simulate
from tests.checker.helpers import get, numbered, put


def test_nothing_to_check() -> None:
    report = check([])

    assert report.verdict == "ok"
    assert report.keys == []


def test_each_key_gets_its_own_verdict() -> None:
    ops = numbered(
        put("a", 0, 1, key="k1"),
        get("a", 2, 3, key="k1"),
        put("a", 4, 5, key="k2"),
        get(None, 6, 7, key="k2"),  # stale: the put on k2 had ended
    )

    report = check(ops)

    assert report.verdict == "bad"
    assert [(result.key, result.verdict) for result in report.keys] == [
        ("k1", "ok"),
        ("k2", "bad"),
    ]
    assert [result.key for result in report.bad_keys] == ["k2"]
    assert report.operations == 4


def test_a_write_to_one_key_is_not_seen_on_another() -> None:
    ops = numbered(put("a", 0, 1, key="k1"), get("a", 2, 3, key="k2"))

    assert check(ops).verdict == "bad"


def test_an_ok_key_comes_with_an_order() -> None:
    ops = numbered(put("a", 0, 3), get(None, 1, 2))

    [result] = check(ops).keys

    assert result.order == [1, 0]


def hard(key: str = "k") -> list[Op]:
    # Ten puts, all overlapping, then a read of a value none of them wrote.
    # Proving that bad means trying every set of the puts that could have
    # gone first, with every one of them last.
    return [put(str(i), i, 100 + i, key) for i in range(10)] + [get("nope", 200, 201, key)]


def test_out_of_budget_is_unknown_not_bad() -> None:
    ops = numbered(*hard())

    assert check(ops, budget=50).verdict == "unknown"
    assert check(ops).verdict == "bad"


def test_unknown_does_not_hide_a_bad_key() -> None:
    ops = numbered(*hard("k1"), put("a", 300, 301, key="k2"), get("b", 302, 303, key="k2"))

    report = check(ops, budget=50)

    assert [result.verdict for result in report.keys] == ["unknown", "bad"]
    assert report.verdict == "bad"


def test_a_real_run_is_linearizable() -> None:
    _, world, _ = simulate(seed=5, seconds=10)

    report = check(world.history)

    assert report.verdict == "ok"
    assert report.operations > 100
    for result in report.keys:
        assert result.order is not None
        # Every op with an answer is in the order
        answered = {op.id for op in result.ops if op.end is not None}
        assert answered <= set(result.order)


def test_the_example_in_the_docs() -> None:
    # docs/checker.md shows this, on a run with a known bug switched on
    _, world, _ = simulate(seed=0, seconds=5, bug="early-ack")

    report = checker.check(world.history)

    assert report.verdict == "bad"
    for result in report.bad_keys:
        explanation = checker.explain(result)
        assert explanation is not None
        assert explanation.lines()[0].endswith("no order of its operations gives these answers.")
