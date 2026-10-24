from faultline.checker.model import State, step
from faultline.checker.ops import Op
from faultline.sim.client import Kind


def op(
    kind: Kind,
    value: str | None = None,
    expected: str | None = None,
    result: bool | None = None,
) -> Op:
    return Op(0, 1, kind, "k", value, expected, result, start=0, end=1, time=0)


def test_put_always_works() -> None:
    assert step(None, op("put", "a")) == (True, "a")
    assert step("x", op("put", "a")) == (True, "a")


def test_get_must_see_the_current_value() -> None:
    assert step("a", op("get", "a")) == (True, "a")
    assert step("a", op("get", "b"))[0] is False
    assert step(None, op("get", None)) == (True, None)
    assert step("a", op("get", None))[0] is False


def test_cas_that_matched() -> None:
    assert step("old", op("cas", "new", "old", True)) == (True, "new")
    # It said it matched, but the value was something else
    assert step("other", op("cas", "new", "old", True))[0] is False


def test_cas_that_did_not_match() -> None:
    assert step("other", op("cas", "new", "old", False)) == (True, "other")
    # It said no match, but the value was the expected one
    assert step("old", op("cas", "new", "old", False))[0] is False


def test_cas_can_expect_a_missing_key() -> None:
    assert step(None, op("cas", "new", None, True)) == (True, "new")
    assert step("a", op("cas", "new", None, False)) == (True, "a")


def test_cas_with_no_answer_writes_only_if_it_matches() -> None:
    no_answer = op("cas", "new", "old", None)

    assert step("old", no_answer) == (True, "new")
    assert step("other", no_answer) == (True, "other")


def test_state_is_a_value_or_missing() -> None:
    states: list[State] = [None, "a"]
    for state in states:
        assert step(state, op("get", state)) == (True, state)
