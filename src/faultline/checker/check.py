"""Check a whole history: did the cluster act like one copy of the data?

Linearizability is local (Herlihy and Wing 1990): a history is linearizable
exactly when the part of it for each key is. So each key is checked on its
own. That matters, because the search grows fast with the number of
operations, and splitting by key cuts it down a lot.

Each key gets a budget of search steps. Some histories, with many writes
that got no answer and all overlap, can make the search huge. When the
budget runs out, the verdict for that key is "unknown": we could not show
it is fine, and we could not show it is bad either. An honest unknown is
better than a check that never ends.
"""

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from faultline.checker.linear import OutOfBudget, linearize
from faultline.checker.ops import Op, operations
from faultline.sim.history import History

type Verdict = Literal["ok", "bad", "unknown"]

# Model steps per key, about a second of searching. A 30 second simulator
# run needs a few thousand.
BUDGET = 2_000_000


@dataclass(frozen=True)
class KeyResult:
    key: str
    verdict: Verdict
    ops: list[Op]
    order: list[int] | None  # op ids in an order that works, when ok


@dataclass(frozen=True)
class Report:
    keys: list[KeyResult]  # sorted by key

    @property
    def verdict(self) -> Verdict:
        # One bad key makes the whole history bad. Otherwise, one we could
        # not decide makes the whole thing undecided.
        verdicts = {result.verdict for result in self.keys}
        if "bad" in verdicts:
            return "bad"
        if "unknown" in verdicts:
            return "unknown"
        return "ok"

    @property
    def ok(self) -> bool:
        return self.verdict == "ok"

    @property
    def bad_keys(self) -> list[KeyResult]:
        return [result for result in self.keys if result.verdict == "bad"]

    @property
    def operations(self) -> int:
        return sum(len(result.ops) for result in self.keys)


def check(history: History | Sequence[Op], budget: int = BUDGET) -> Report:
    """Check a history, or a list of operations from it."""
    ops = operations(history) if isinstance(history, History) else history
    by_key: dict[str, list[Op]] = defaultdict(list)
    for op in sorted(ops, key=lambda op: op.start):
        by_key[op.key].append(op)
    return Report([_check_key(key, by_key[key], budget) for key in sorted(by_key)])


def _check_key(key: str, ops: list[Op], budget: int) -> KeyResult:
    try:
        order = linearize(ops, budget)
    except OutOfBudget:
        return KeyResult(key, "unknown", ops, None)
    return KeyResult(key, "bad" if order is None else "ok", ops, order)
