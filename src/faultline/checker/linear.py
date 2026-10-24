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
the search is exponential on nearly every history. (It is a little
smarter than that about ops with no answer, see _Memo.)

A few things keep it fast on long histories:

* The waiting operations are kept as one linked list of their starts and
  ends, in history order. The ones that may go next are the starts before
  the first end, so we only ever look at the front of the list. Taking an
  op out and putting it back are both a couple of pointer changes.
* Operations with an answer are tried before the ones without. An op with
  no answer has no end, so it can always wait.
* An op with no answer (call it X) is only placed if it changes the value,
  and only if the next op could not have gone without it. If the next op N
  was possible anyway, then N first and X after leads to the same place, or
  N alone leads to the same value with X still free to use later. Either
  way that place gets searched without X going first. This matters a lot:
  otherwise every old write with no answer gets tried in front of every
  cas that "did not match", and the search never ends.
* Written values that nobody ever reads count as one value, and ops with no
  answer that do the same thing count as one op. See
  _unseen_values_merged() and _Waiting.next_ops().
* The key is never moved off a value that a waiting read still needs, if
  nothing left can write that value again (_Search._strands). Without this,
  a write with no answer could be used up early, and the search would only
  find out at the read that needed it, maybe thousands of ops later.
"""

from collections import Counter
from collections.abc import Iterator, Sequence
from dataclasses import replace

from faultline.checker.model import State, step
from faultline.checker.ops import Op

# Stands for every written value that no get reads and no cas expects. See
# _unseen_values_merged().
UNSEEN = "\0unseen"


class OutOfBudget(Exception):
    """The search took more steps than it was allowed."""


def linearize(ops: Sequence[Op], budget: int | None = None) -> list[int] | None:
    """Find an order for the ops of one key, sorted by start. Returns the op
    ids in that order (ops with no answer only if they were needed), or None
    if there is no such order.

    budget is the most model steps to try. Past it, OutOfBudget is raised.
    """
    must = _answered(ops)
    search = _Search(_unseen_values_merged(ops), budget)
    for placed, _ in search.places():
        if placed & must == must:
            return [ops[i].id for i in search.order()]
    return None


def final_states(ops: Sequence[Op], budget: int | None = None) -> set[State]:
    """Every value the key can hold after the ops, over all orders that
    work. Empty if no order works. This one explores everything instead of
    stopping at the first order, so it is slower. It is only used to
    explain a failure."""
    must = _answered(ops)
    return {state for placed, state in _Search(ops, budget).places() if placed & must == must}


def _unseen_values_merged(ops: Sequence[Op]) -> list[Op]:
    """Give every written value that nobody ever looks at the same name.

    If no get reads a value and no cas expects it, nothing can tell that
    value apart from any other such value. So they may as well be one value,
    and then the memo sees that two places that differ only there are the
    same place. This matters for writes with no answer: each one may happen
    at any moment, and without this, each one would be tried everywhere.
    """
    looked_at = {op.value for op in ops if op.kind == "get"}
    looked_at |= {op.expected for op in ops if op.kind == "cas"}
    return [
        op if op.kind == "get" or op.value in looked_at else replace(op, value=UNSEEN) for op in ops
    ]


def _answered(ops: Sequence[Op]) -> int:
    # Sets of ops are ints: bit i is ops[i]
    return sum(1 << i for i, op in enumerate(ops) if op.end is not None)


class _Search:
    """Depth first over the orders of the ops."""

    def __init__(self, ops: Sequence[Op], budget: int | None) -> None:
        self._ops = ops
        self._budget = budget
        self._waiting = _Waiting(ops)
        # Each op placed so far, with the state just before it and the ops
        # still left to try in its place, so we can go back
        self._path: list[tuple[int, State, list[int]]] = []
        # For each value: how many waiting ops may still write it, and how
        # many waiting ops with an answer can only go while the key holds it
        self._writers: Counter[State] = Counter(op.value for op in ops if op.kind != "get")
        self._needers: Counter[State] = Counter(value for needs, value in map(_needs, ops) if needs)

    def order(self) -> list[int]:
        return [i for i, _, _ in self._path]

    def places(self) -> Iterator[tuple[int, State]]:
        """Every (placed ops, state) the search can reach, each one once.
        While the caller looks at one, order() is how we got there."""
        placed = 0
        state: State = None  # every key starts missing
        memo = _Memo(self._ops)
        to_try = self._waiting.next_ops()
        steps = 0
        yield placed, state

        while True:
            while to_try:
                i = to_try.pop()
                steps += 1
                if self._budget is not None and steps > self._budget:
                    raise OutOfBudget(f"no answer after {self._budget} steps")
                possible, after = self._step(state, i)
                if possible and self._strands(state, after, i):
                    continue
                if possible and memo.first_visit(placed | 1 << i, after):
                    self._path.append((i, state, to_try))
                    self._waiting.take(i)
                    self._count(i, -1)
                    placed |= 1 << i
                    before, state = state, after
                    to_try = self._waiting.next_ops()
                    if self._ops[i].end is None:
                        # Only what this op made possible may come next
                        to_try = [j for j in to_try if not self._step(before, j)[0]]
                    yield placed, state
                    break
            else:
                # Stuck. Take the last op back out and try the next one in
                # its place.
                if not self._path:
                    return
                i, state, to_try = self._path.pop()
                self._waiting.put_back(i)
                self._count(i, +1)
                placed &= ~(1 << i)

    def _strands(self, state: State, after: State, i: int) -> bool:
        """Would op i move the key off a value that some waiting op still
        needs, when no waiting op can write that value again? Then that op
        can never go, and we can stop right here instead of finding out at
        the end of the history."""
        if after == state:
            return False
        # Op i itself may be one that needs the old value (a cas that
        # matched). It is placed now, so it doesn't count.
        needs, value = _needs(self._ops[i])
        needers = self._needers[state] - (needs and value == state)
        return needers > 0 and self._writers[state] == 0

    def _count(self, i: int, change: int) -> None:
        op = self._ops[i]
        if op.kind != "get":
            self._writers[op.value] += change
        needs, value = _needs(op)
        if needs:
            self._needers[value] += change

    def _step(self, state: State, i: int) -> tuple[bool, State]:
        """Like model.step(), but an op with no answer that would change
        nothing counts as not possible. Placing it is pointless."""
        possible, after = step(state, self._ops[i])
        if self._ops[i].end is None and after == state:
            return False, state
        return possible, after


def _needs(op: Op) -> tuple[bool, State]:
    """Whether op can only go while the key holds one value, and which."""
    if op.end is None:
        return False, None
    if op.kind == "get":
        return True, op.value
    if op.kind == "cas" and op.result:
        return True, op.expected
    return False, None


class _Memo:
    """The places the search has been. A place is the set of ops placed and
    the value. Split the set in two: the ops with an answer, and the ones
    without. If we were already at the same answered ops and value, having
    used only some of the unanswered ops we have used now, there is nothing
    new here. That earlier place can do everything this one can, and it
    still has more unanswered ops free to use."""

    def __init__(self, ops: Sequence[Op]) -> None:
        self._answered = _answered(ops)
        self._seen: dict[tuple[int, State], list[int]] = {}

    def first_visit(self, placed: int, state: State) -> bool:
        no_answer = placed & ~self._answered
        used_before = self._seen.setdefault((placed & self._answered, state), [])
        if any(old & ~no_answer == 0 for old in used_before):
            return False  # we were here before, with no more used
        used_before.append(no_answer)
        return True


class _Waiting:
    """The ops not placed yet, as a linked list of their starts and ends in
    history order. Ops must be put back in the reverse order they were
    taken, which a depth first search always does."""

    def __init__(self, ops: Sequence[Op]) -> None:
        self._ops = ops
        # Every start and end, in order. At the same position a start comes
        # first, so the two ops count as overlapping.
        marks = sorted(
            [(op.start, 0, i) for i, op in enumerate(ops)]
            + [(op.end, 1, i) for i, op in enumerate(ops) if op.end is not None]
        )
        # Node 0 is the head of the list and node n + 1 the tail. Mark k is
        # node k + 1.
        count = len(marks) + 2
        self._next = list(range(1, count + 1))
        self._prev = list(range(-1, count - 1))
        self._op = [-1] + [i for _, _, i in marks] + [-1]
        self._is_start = [False] + [kind == 0 for _, kind, _ in marks] + [False]
        self._nodes: list[list[int]] = [[] for _ in ops]
        for node, (_, _, i) in enumerate(marks, start=1):
            self._nodes[i].append(node)

    def next_ops(self) -> list[int]:
        """The ops that may go next: the starts before the first end. Made
        so that pop() gives the ones with an answer first, earliest start
        first."""
        answered: list[int] = []
        no_answer: list[int] = []
        alike: set[tuple[str, State, State]] = set()
        node = self._next[0]
        while self._is_start[node]:
            i = self._op[node]
            op = self._ops[i]
            if op.end is not None:
                answered.append(i)
            elif (op.kind, op.expected, op.value) not in alike:
                # Two ops with no answer that do the same thing can be
                # swapped for each other, so only the first is worth trying
                alike.add((op.kind, op.expected, op.value))
                no_answer.append(i)
            node = self._next[node]
        return no_answer[::-1] + answered[::-1]

    def take(self, i: int) -> None:
        for node in self._nodes[i]:
            self._next[self._prev[node]] = self._next[node]
            self._prev[self._next[node]] = self._prev[node]

    def put_back(self, i: int) -> None:
        for node in reversed(self._nodes[i]):
            self._next[self._prev[node]] = node
            self._prev[self._next[node]] = node
