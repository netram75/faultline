"""Explain a bad key in a few lines.

A bad key from a 30 second run can have hundreds of operations, and the
problem is usually in three or four of them. Here is how we find them.

**The first answer that can't be explained.** Cut the history at some
position: keep what happened before it, and treat operations still running
there as if no answer came (they may still happen). Cutting can only
remove problems, never add one. So there is a first position where the cut
history is already bad, and the answer that arrived right there is the
first one no order can explain. Binary search finds it.

**What the key could hold.** Cut again where that operation started, and
collect every value the key could have had at that moment, over all orders
that work up to there.

**The operations around it.** The ones that overlap it, and the last few
that ended just before it.

Why not shrink the history by dropping operations one at a time, while it
stays bad? Because dropping a write can make a good history look bad. In
put x, put y, then a cas from x that said "no match", dropping put y makes
the cas look wrong. The explanation would point at a problem that isn't
there. Cutting at a position never does that.
"""

from collections.abc import Sequence
from dataclasses import dataclass, replace

from faultline.checker.check import BUDGET, KeyResult
from faultline.checker.linear import OutOfBudget, final_states, linearize
from faultline.checker.model import State
from faultline.checker.ops import Op

# How many ops that ended before the bad one to show
_BEFORE = 3


@dataclass(frozen=True)
class Explanation:
    key: str
    culprit: Op  # its answer is the first one no order can explain
    could_hold: set[State]  # the values the key could have when it started
    around: list[Op]  # ops that overlap it or just ended before it

    def lines(self) -> list[str]:
        values = sorted(_show(value) for value in self.could_hold)
        return [
            f"key {self.key}: no order of its operations gives these answers.",
            "The first answer that can't be explained:",
            describe(self.culprit),
            f"When it started, {self.key} could only hold: {', '.join(values)}",
            "Operations around it:",
            *(describe(op) for op in self.around),
        ]


def explain(result: KeyResult, budget: int = BUDGET) -> Explanation | None:
    """Explain a bad key. None if the search runs out of budget."""
    ops = result.ops
    ends = sorted(op.end for op in ops if op.end is not None)
    try:
        # The first end where the cut history is bad. The whole history is
        # bad, so the last end always is.
        low, high = 0, len(ends) - 1
        while low < high:
            middle = (low + high) // 2
            if linearize(cut(ops, ends[middle]), budget) is None:
                high = middle
            else:
                low = middle + 1
        culprit = next(op for op in ops if op.end == ends[low])
        could_hold = final_states(cut(ops, culprit.start), budget)
    except OutOfBudget:
        return None
    return Explanation(result.key, culprit, could_hold, _around(ops, culprit))


def cut(ops: Sequence[Op], position: int) -> list[Op]:
    """The history as it looked at `position`. Ops that had not started are
    gone. Writes still running lose their answer. Reads still running are
    gone, since a read with no answer tells us nothing."""
    kept: list[Op] = []
    for op in ops:
        if op.start >= position:
            continue
        if op.end is None or op.end <= position:
            kept.append(op)
        elif op.kind != "get":
            kept.append(replace(op, end=None, result=None))
    return kept


def describe(op: Op) -> str:
    if op.kind == "put":
        what = f"put {op.value}"
    elif op.kind == "get":
        what = f"get -> {_show(op.value)}"
    else:
        what = f"cas {_show(op.expected)} -> {op.value}"
    if op.end is None:
        answer = "no answer"
    elif op.kind == "cas":
        answer = "matched" if op.result else "did not match"
    else:
        answer = "ok"
    return f"  #{op.id:<5} client {op.client:<3} at {op.time:>6} ms  {what}  ({answer})"


def _around(ops: Sequence[Op], culprit: Op) -> list[Op]:
    assert culprit.end is not None
    overlapping = {
        op.id for op in ops if op.start < culprit.end and (op.end is None or op.end > culprit.start)
    }
    ended_before = sorted(
        (op.end, op.id) for op in ops if op.end is not None and op.end < culprit.start
    )
    shown = overlapping | {op_id for _, op_id in ended_before[-_BEFORE:]}
    return [op for op in ops if op.id in shown and op is not culprit]


def _show(value: State) -> str:
    return "missing" if value is None else value
