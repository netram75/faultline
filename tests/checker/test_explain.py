import random
from dataclasses import replace

from faultline.checker.check import check
from faultline.checker.explain import Explanation, cut, explain
from faultline.checker.linear import final_states
from faultline.checker.ops import Op
from tests.checker.helpers import cas, get, numbered, put, register_history


def explained(ops: list[Op]) -> Explanation:
    [bad] = check(ops).bad_keys
    explanation = explain(bad)
    assert explanation is not None
    return explanation


def test_stale_read() -> None:
    ops = numbered(put("a", 0, 1), put("b", 2, 3), get("a", 4, 5))

    explanation = explained(ops)

    assert explanation.culprit == ops[2]
    assert explanation.could_hold == {"b"}
    assert explanation.around == ops[:2]


def test_two_cas_that_both_won() -> None:
    #  [- put x -]  [------ cas x->y: matched ------]
    #                  [------ cas x->z: matched -------]
    ops = numbered(put("x", 0, 1), cas("x", "y", True, 2, 10), cas("x", "z", True, 3, 11))

    explanation = explained(ops)

    # The first cas alone is fine. The second answer is the one that can't be
    # explained, and when it started the first may or may not have happened.
    assert explanation.culprit == ops[2]
    assert explanation.could_hold == {"x", "y"}
    assert explanation.around == ops[:2]


def test_finds_the_first_problem_in_a_long_history() -> None:
    ops = register_history(random.Random(7), 400)
    # Make one read in the middle return a value it can't have
    middle = next(op for op in ops[200:] if op.kind == "get")
    ops[middle.id] = replace(middle, value="not written")

    explanation = explained(ops)

    assert explanation.culprit.id == middle.id
    assert len(explanation.around) < 12


def test_lines_for_a_person() -> None:
    ops = numbered(put("a", 0, 1), put("b", 2, 3), get("a", 4, 5))

    assert explained(ops).lines() == [
        "key k: no order of its operations gives these answers.",
        "The first answer that can't be explained:",
        "  #2     client 2   at      4 ms  get -> a  (ok)",
        "When it started, k could only hold: b",
        "Operations around it:",
        "  #0     client 0   at      0 ms  put a  (ok)",
        "  #1     client 1   at      2 ms  put b  (ok)",
    ]


def test_cut_keeps_what_had_happened_by_then() -> None:
    ops = numbered(put("a", 0, 5), get("a", 1, 3), cas(None, "b", False, 2, 9), get("b", 7, 8))

    # At 4: the put and the cas are still running, so they lose their
    # answer. The get that ended at 3 stays. The one that starts at 7 is gone.
    assert cut(ops, 4) == [
        replace(ops[0], end=None, result=None),
        ops[1],
        replace(ops[2], end=None, result=None),
    ]


def test_final_states() -> None:
    # The two puts overlap, so either can be last
    assert final_states(numbered(put("a", 0, 5), put("b", 1, 6))) == {"a", "b"}
    # A write with no answer may or may not have happened
    assert final_states(numbered(put("a", 0, 1), put("b", 2, None))) == {"a", "b"}
    assert final_states(numbered(put("a", 0, 1), get("b", 2, 3))) == set()
