"""Search for an order of one key's operations that a single copy could run.

This is the heart of the checker. The idea is from Wing and Gong (1993),
with the memo from Lowe (2017). The Knossos and Porcupine checkers use the
same search.

We build the order one operation at a time, from the front. Which ones may
go next? An operation takes effect at some moment inside its window, between
its start and its end. So op x may go next only if no other waiting
operation ended before x started, because that one must come before x:

    client 1:  [--- put a ---]
    client 2:                    [-- get -> a --]
    client 3:        [------------- put b -------------]

put a ended before the get started, so it must go first. put b overlaps
both, so it may go anywhere, as long as the get still reads a. The order
put a, get, put b works.

For each operation that may go next we run the model (model.py). If the
answer we saw is possible, we place it and carry on. If we get stuck, we go
back and try the next one. We are done when every operation with an answer
is placed. Operations with no answer (end None) never have to be placed:
they may never have happened. One gets placed only when some answer needs
it.

The memo: different paths often reach the same place, the same set of
placed operations with the same value. What can happen from there depends
only on those two things, so the second time we skip it. Without the memo
the search is exponential on nearly every history.
"""

from collections.abc import Iterator, Sequence

from faultline.checker.model import State, step
from faultline.checker.ops import Op


class OutOfBudget(Exception):
    """The search took more steps than it was allowed."""


def linearize(ops: Sequence[Op], budget: int | None = None) -> list[int] | None:
    """Find an order for the ops of one key, sorted by start. Returns the op
    ids in that order (ops with no answer only if they were needed), or None
    if there is no such order.

    budget is the most model steps to try. Past it, OutOfBudget is raised.
    """
    # Sets of ops are ints: bit i is ops[i]
    must = sum(1 << i for i, op in enumerate(ops) if op.end is not None)
    seen: set[tuple[int, State]] = set()

    # The order so far: each placed op, with the state just before it, so we
    # can go back
    path: list[tuple[int, State]] = []
    placed = 0
    state: State = None  # every key starts missing
    first = 0  # try only candidates from here on, the ones before failed
    steps = 0

    while placed & must != must:
        for i in _candidates(ops, placed, first):
            steps += 1
            if budget is not None and steps > budget:
                raise OutOfBudget(f"no answer after {budget} steps")
            possible, after = step(state, ops[i])
            if possible and (placed | 1 << i, after) not in seen:
                seen.add((placed | 1 << i, after))
                path.append((i, state))
                placed |= 1 << i
                state = after
                first = 0
                break
        else:
            # Stuck. Take the last op back out and try the one after it.
            if not path:
                return None
            i, state = path.pop()
            placed &= ~(1 << i)
            first = i + 1
    return [ops[i].id for i, _ in path]


def _candidates(ops: Sequence[Op], placed: int, first: int) -> Iterator[int]:
    """The waiting ops that may go next, from index `first` on."""
    # The earliest end among the waiting ops. Nothing that started after it
    # may go next. (An op's own end is after its own start, so it is fine
    # to count it here too.)
    earliest_end = min(
        (op.end for i, op in enumerate(ops) if op.end is not None and not placed >> i & 1),
        default=None,
    )
    for i in range(first, len(ops)):
        if earliest_end is not None and ops[i].start > earliest_end:
            break  # sorted by start, so the rest started later still
        if not placed >> i & 1:
            yield i
