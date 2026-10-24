from faultline.checker.linear import linearize
from faultline.checker.ops import Op
from tests.checker.helpers import cas, get, numbered, put


def values(ops: list[Op], order: list[int] | None) -> list[str | None]:
    assert order is not None
    return [ops[i].value for i in order]


def test_nothing_to_order() -> None:
    assert linearize([]) == []


def test_one_after_another() -> None:
    ops = numbered(put("a", 0, 1), get("a", 2, 3), put("b", 4, 5), get("b", 6, 7))

    assert linearize(ops) == [0, 1, 2, 3]


def test_the_example_from_the_docstring() -> None:
    ops = numbered(put("a", 0, 3), put("b", 1, 9), get("a", 4, 6))

    assert values(ops, linearize(ops)) == ["a", "a", "b"]


def test_overlapping_ops_may_go_in_either_order() -> None:
    # The get overlaps the put, so it may read the old value or the new one
    assert linearize(numbered(put("a", 0, 5), get(None, 1, 6))) is not None
    assert linearize(numbered(put("a", 0, 5), get("a", 1, 6))) is not None


def test_a_read_after_a_write_ended_must_see_it() -> None:
    ops = numbered(put("a", 0, 1), get(None, 2, 3))

    assert linearize(ops) is None


def test_a_read_of_a_value_nobody_wrote() -> None:
    assert linearize(numbered(put("a", 0, 1), get("z", 2, 3))) is None


def test_must_backtrack_to_find_the_order() -> None:
    # The search tries put a, then put b, and gets stuck at get a. It has to
    # go back and put the get before put b.
    ops = numbered(put("a", 0, 10), put("b", 1, 11), get("a", 2, 12), get("b", 13, 14))

    assert linearize(ops) == [0, 2, 1, 3]


def test_no_answer_does_not_have_to_happen() -> None:
    ops = numbered(put("a", 0, None), get(None, 5, 6))

    assert linearize(ops) == [1]


def test_no_answer_may_happen_late() -> None:
    # The put got no answer, so it may take effect long after it started
    ops = numbered(put("a", 0, None), get(None, 5, 6), get("a", 7, 8))

    assert linearize(ops) == [1, 0, 2]


def test_two_cas_cannot_both_win_from_the_same_value() -> None:
    ops = numbered(
        put("x", 0, 1),
        cas("x", "y", True, 2, 10),
        cas("x", "z", True, 3, 11),
    )

    assert linearize(ops) is None


def test_cas_with_no_answer_may_be_the_one_that_wrote() -> None:
    ops = numbered(put("x", 0, 1), cas("x", "y", None, 2, None), get("y", 5, 6))

    assert linearize(ops) == [0, 1, 2]


def test_same_ops_placed_but_a_different_value() -> None:
    # put a, put b and put b, put a place the same two ops but leave
    # different values. The memo must tell them apart: only the second one
    # works for the get.
    ops = numbered(put("a", 0, 10), put("b", 1, 11), get("a", 12, 13))

    assert linearize(ops) == [1, 0, 2]
