"""Short ways to write operations by hand in tests.

start and end are positions in the history. end=None means no answer came.
"""

import random
from dataclasses import replace

from faultline.checker.ops import Op


def put(value: str, start: int, end: int | None, key: str = "k") -> Op:
    return Op(0, 0, "put", key, value, None, None if end is None else True, start, end, start)


def get(value: str | None, start: int, end: int, key: str = "k") -> Op:
    return Op(0, 0, "get", key, value, None, None, start, end, start)


def cas(
    expected: str | None,
    value: str,
    result: bool | None,
    start: int,
    end: int | None,
    key: str = "k",
) -> Op:
    return Op(0, 0, "cas", key, value, expected, result, start, end, start)


def numbered(*ops: Op) -> list[Op]:
    """Sort by start and give each op its own id (and its own client)."""
    ordered = sorted(ops, key=lambda op: op.start)
    return [replace(op, id=i, client=i) for i, op in enumerate(ordered)]


def register_history(rng: random.Random, count: int, values: int = 3) -> list[Op]:
    """A history that is linearizable on purpose. Each op takes effect at
    its own moment, inside a random window around it, on one register. The
    answers are what a single copy would give."""
    state: str | None = None
    ops: list[Op] = []
    for moment in range(1, count + 1):
        # Windows of different ops overlap, but a start never equals an end
        start = 10 * moment - 10 * rng.randint(0, 3) - 1
        end = 10 * moment + 10 * rng.randint(0, 3) + 1
        value = str(rng.randrange(values))
        kind = rng.choice(["put", "get", "cas"])
        if kind == "put":
            ops.append(put(value, start, end))
            state = value
        elif kind == "get":
            ops.append(get(state, start, end))
        else:
            expected = rng.choice([None, *map(str, range(values))])
            matched = state == expected
            ops.append(cas(expected, value, matched, start, end))
            if matched:
                state = value
    return numbered(*ops)
