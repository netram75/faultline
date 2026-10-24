"""The fast checker against a slow one that can't be wrong.

The slow checker tries every order of every set of operations: all the ones
with an answer, plus any subset of the ones without. It only keeps orders
that respect time (nothing goes before an op that ended before it started)
and replays each one on a plain dict. It shares no code with the real
checker, not even the model, and it is far too slow for anything but tiny
histories. On tiny histories the two must always agree.
"""

import itertools
import random
from collections.abc import Sequence
from dataclasses import replace

from faultline.checker.check import check
from faultline.checker.ops import Op
from tests.checker.helpers import cas, get, numbered, put, register_history

VALUES = ["0", "1", None]


def brute_force(ops: Sequence[Op]) -> bool:
    answered = [op for op in ops if op.end is not None]
    no_answer = [op for op in ops if op.end is None]
    for size in range(len(no_answer) + 1):
        for chosen in itertools.combinations(no_answer, size):
            for order in itertools.permutations(answered + list(chosen)):
                if respects_time(order) and answers_match(order):
                    return True
    return False


def respects_time(order: Sequence[Op]) -> bool:
    return not any(
        later.end is not None and later.end < earlier.start
        for earlier, later in itertools.combinations(order, 2)
    )


def answers_match(order: Sequence[Op]) -> bool:
    values: dict[str, str | None] = {}
    for op in order:
        current = values.get(op.key)
        if op.kind == "put":
            values[op.key] = op.value
        elif op.kind == "get":
            if op.value != current:
                return False
        else:
            matched = current == op.expected
            if op.result is not None and op.result != matched:
                return False
            if matched:
                values[op.key] = op.value
    return True


def junk(rng: random.Random) -> list[Op]:
    """Random ops with random answers. Starts are even and ends are odd, so
    a start never equals an end."""
    ops = []
    for _ in range(rng.randint(1, 6)):
        start = 2 * rng.randrange(20)
        end = start + 1 + 2 * rng.randrange(8)
        key = rng.choice(["k1", "k2"])
        kind = rng.choice(["put", "get", "cas"])
        if kind == "get":
            ops.append(get(rng.choice(VALUES), start, end, key))
            continue
        value = rng.choice(["0", "1"])
        answer = rng.random() < 0.8
        if kind == "put":
            ops.append(put(value, start, end if answer else None, key))
        else:
            result = rng.choice([True, False]) if answer else None
            ops.append(cas(rng.choice(VALUES), value, result, start, end if answer else None, key))
    return numbered(*ops)


def nearly_right(rng: random.Random) -> list[Op]:
    """A history that is linearizable on purpose, on one or two keys. Then
    maybe one answer gets changed, and some writes lose their answer."""
    ops = [replace(op, key="k1") for op in register_history(rng, rng.randint(1, 4), 2)]
    if rng.random() < 0.5:
        ops += [replace(op, key="k2") for op in register_history(rng, rng.randint(1, 3), 2)]
    if rng.random() < 0.7:
        i = rng.randrange(len(ops))
        if ops[i].kind == "get":
            ops[i] = replace(ops[i], value=rng.choice(VALUES))
        elif ops[i].kind == "cas":
            ops[i] = replace(ops[i], result=not ops[i].result)
    for i, op in enumerate(ops):
        if op.kind != "get" and rng.random() < 0.2:
            ops[i] = replace(op, end=None, result=None)
    return numbered(*ops)


def agree(ops: list[Op]) -> bool:
    fast = check(ops).verdict
    slow = brute_force(ops)
    assert fast == ("ok" if slow else "bad"), ops
    return slow


def test_agree_on_random_junk() -> None:
    results = [agree(junk(random.Random(seed))) for seed in range(3000)]

    # Both answers must come up a fair number of times, or the test proves
    # little
    assert 500 < sum(results) < 2500


def test_agree_on_nearly_right_histories() -> None:
    results = [agree(nearly_right(random.Random(seed))) for seed in range(3000)]

    assert 500 < sum(results) < 2500
