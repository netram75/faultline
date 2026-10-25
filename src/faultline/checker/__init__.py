"""The linearizability checker: did the cluster act like one copy of the data?

See docs/checker.md for how it works.
"""

from faultline.checker.check import KeyResult, Report, Verdict, check
from faultline.checker.explain import Explanation, explain
from faultline.checker.ops import BadHistory, Op, operations

__all__ = [
    "BadHistory",
    "Explanation",
    "KeyResult",
    "Op",
    "Report",
    "Verdict",
    "check",
    "explain",
    "operations",
]
