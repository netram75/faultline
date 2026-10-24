"""Small histories written by hand, each showing one rule.

The pictures show each operation's window, from start to end. An operation
takes effect at one moment somewhere inside its window. A history is fine
if we can pick those moments so that every answer is what a single copy of
the data would give.
"""

import random
import time

from faultline.checker.check import check
from faultline.checker.ops import Op
from faultline.sim.history import Event, History
from tests.checker.helpers import cas, get, numbered, put, register_history


def ok(*ops: Op) -> bool:
    verdict = check(numbered(*ops)).verdict
    assert verdict != "unknown"
    return verdict == "ok"


def test_one_at_a_time() -> None:
    assert ok(
        put("a", 0, 1),
        get("a", 2, 3),
        cas("a", "b", True, 4, 5),
        get("b", 6, 7),
        cas("a", "c", False, 8, 9),
        get("b", 10, 11),
    )


def test_a_read_during_a_write_may_see_either_value() -> None:
    #  [------ put a ------]
    #       [-- get --]
    assert ok(put("a", 0, 5), get(None, 1, 2))
    assert ok(put("a", 0, 5), get("a", 1, 2))


def test_stale_read() -> None:
    #  [- put a -]
    #               [- get -] -> missing
    assert not ok(put("a", 0, 1), get(None, 2, 3))


def test_read_of_a_value_nobody_wrote() -> None:
    assert not ok(put("a", 0, 1), get("b", 2, 3))


def test_lost_write() -> None:
    #  [- put a -]  [- put b -]  [- get -] -> a
    assert not ok(put("a", 0, 1), put("b", 2, 3), get("a", 4, 5))


def test_once_a_read_sees_the_new_value_later_reads_cannot_see_the_old() -> None:
    #  [- put a -]
    #             [-------------- put b --------------]
    #                [- get -] -> b    [- get -] -> a
    # put b is still running, but the first get saw it, so it has happened.
    # Going back to a would need a second copy of the data.
    assert not ok(put("a", 0, 1), put("b", 2, 10), get("b", 3, 4), get("a", 5, 6))
    # Seeing them the other way round is fine: b happens between the reads
    assert ok(put("a", 0, 1), put("b", 2, 10), get("a", 3, 4), get("b", 5, 6))


def test_two_cas_cannot_both_win_from_the_same_value() -> None:
    #  [- put x -]  [-- cas x->y: yes --]
    #                 [-- cas x->z: yes --]
    # This is the lost update a compare-and-set exists to stop
    assert not ok(put("x", 0, 1), cas("x", "y", True, 2, 10), cas("x", "z", True, 3, 11))
    # One wins, the other sees the new value and says no
    assert ok(put("x", 0, 1), cas("x", "y", True, 2, 10), cas("x", "z", False, 3, 11))


def test_cas_that_says_no_when_the_value_matched() -> None:
    assert not ok(put("x", 0, 1), cas("x", "y", False, 2, 3))


def test_cas_that_said_no_did_not_write() -> None:
    assert not ok(put("z", 0, 1), cas("x", "y", False, 2, 3), get("y", 4, 5))


def test_cas_with_no_answer_writes_only_if_it_can_match() -> None:
    assert ok(put("x", 0, 1), cas("x", "y", None, 2, None), get("y", 4, 5))
    assert not ok(put("z", 0, 1), cas("x", "y", None, 2, None), get("y", 4, 5))


def test_write_with_no_answer_that_shows_up_late() -> None:
    #  [- put a, no answer ...
    #                 [- get -] -> missing  [- get -] -> a
    assert ok(put("a", 0, None), get(None, 5, 6), get("a", 7, 8))


def test_write_with_no_answer_that_never_shows_up() -> None:
    assert ok(put("a", 0, None), get(None, 5, 6), get(None, 7, 8))


def test_write_with_no_answer_still_cannot_go_back_in_time() -> None:
    # The put of a started after the read saw b. It can't be what the first
    # read saw.
    assert not ok(put("b", 0, 1), get("a", 2, 3), put("a", 4, None))


def test_write_with_no_answer_happens_at_most_once() -> None:
    #  [- put a, no answer ...
    #     [- put b -]  [- get -] -> a  [- get -] -> b ... and a again
    assert ok(put("a", 0, None), put("b", 1, 2), get("a", 3, 4))
    assert not ok(put("a", 0, None), put("b", 1, 2), get("a", 3, 4), get("b", 5, 6), get("a", 7, 8))


def test_one_clients_ops_in_the_same_millisecond_keep_their_order() -> None:
    # The client's put ended at 20 ms and its get started at 20 ms too. The
    # get still came after, and must see the put.
    history = History()
    history.events = [
        Event(1, "invoke", "put", "k", "a", None, None, 10),
        Event(1, "ok", "put", "k", "a", None, True, 20),
        Event(1, "invoke", "get", "k", None, None, None, 20),
        Event(1, "ok", "get", "k", None, None, None, 30),
    ]

    assert check(history).verdict == "bad"


def test_keys_do_not_mix() -> None:
    assert ok(put("a", 0, 1, key="k1"), put("b", 2, 3, key="k2"), get("a", 4, 5, key="k1"))
    assert not ok(put("a", 0, 1, key="k1"), get("a", 2, 3, key="k2"))


def test_a_long_history_is_quick() -> None:
    ops = register_history(random.Random(1), 3000)

    started = time.perf_counter()
    report = check(ops)

    assert report.verdict == "ok"
    assert time.perf_counter() - started < 5
