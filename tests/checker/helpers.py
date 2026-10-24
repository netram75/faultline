"""Short ways to write operations by hand in tests.

start and end are positions in the history. end=None means no answer came.
"""

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
