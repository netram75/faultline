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
    return numbered(*_spread(ops))


def _spread(ops: list[Op]) -> list[Op]:
    """Give every start and every end its own position, like in a real
    history. Ties are broken by op, and no start moves past an end, so the
    checker sees the same history."""
    marks = sorted(
        [(op.start, i, "start") for i, op in enumerate(ops)]
        + [(op.end, i, "end") for i, op in enumerate(ops) if op.end is not None]
    )
    new = {(i, which): position for position, (_, i, which) in enumerate(marks)}
    return [replace(op, start=new[i, "start"], end=new[i, "end"]) for i, op in enumerate(ops)]
