"""Turn a history of events into operations the checker can work with.

The history (sim/history.py) has two events per operation: the invoke and
the end. The checker wants one record per operation, with the window of time
in which it may have taken effect:

    start   the position of the invoke in the history
    end     the position of the answer

Positions, not milliseconds. The history is in the order things happened,
and two events in the same millisecond still have an order. With times, a
client that starts its next operation in the same millisecond the last one
ended would look like it ran both at once, and the checker would let them
swap.

What each kind of ending means here:

* **ok:** it happened, at some moment between start and end, and gave the
  result we saw.
* **fail:** it surely did not happen, so it is dropped.
* **info:** nobody knows. A write may have happened or not, at any moment
  after start, even after the history ends. It is kept with end = None,
  which means "no end", and the checker tries both ways. A read that ended
  info changed nothing and told us nothing, so it is dropped.

An operation still running when the history stops is treated like info:
it never got an answer.
"""

from dataclasses import dataclass

from faultline.sim.client import Kind
from faultline.sim.history import Event, History


class BadHistory(Exception):
    """The events don't pair up the way a real client makes them."""


@dataclass(frozen=True)
class Op:
    id: int  # its place in the list, handy in error messages
    client: int
    kind: Kind
    key: str
    value: str | None  # put and cas: the value written. get: the value read
    expected: str | None  # cas: the value it must find
    result: bool | None  # cas: did the compare match. None if we don't know
    start: int
    end: int | None  # None: no answer came, it may take effect any time after start
    time: int  # ms when it was invoked, for people reading a report


def operations(history: History) -> list[Op]:
    """Pair each invoke with its end. Returns the operations that may have
    happened, in the order they were invoked."""
    running: dict[int, tuple[int, Event]] = {}
    pairs: list[tuple[int, Event, int | None, Event | None]] = []
    for position, event in enumerate(history.events):
        if event.type == "invoke":
            if event.client in running:
                raise BadHistory(f"client {event.client} started twice without an end between")
            running[event.client] = (position, event)
            continue
        start, invoke = running.pop(event.client, (-1, None))
        if invoke is None:
            raise BadHistory(f"client {event.client} ended an operation it never started")
        if (invoke.op, invoke.key) != (event.op, event.key):
            raise BadHistory(
                f"client {event.client} started a {invoke.op} on {invoke.key}"
                f" but ended a {event.op} on {event.key}"
            )
        if event.time < invoke.time:
            raise BadHistory(f"client {event.client} ended an operation before it started")
        pairs.append((start, invoke, position, event))
    pairs += [(start, invoke, None, None) for start, invoke in running.values()]

    pairs.sort(key=lambda pair: pair[0])
    ops: list[Op] = []
    for start, invoke, end, answer in pairs:
        if answer is not None and answer.type == "fail":
            continue
        if answer is None or answer.type == "info":
            if invoke.op == "get":
                continue
            ops.append(_op(len(ops), invoke, start))
        else:
            ops.append(_op(len(ops), invoke, start, end, answer))
    return ops


def _op(
    id: int, invoke: Event, start: int, end: int | None = None, answer: Event | None = None
) -> Op:
    # A write's value comes from the invoke: it is the value it writes, known
    # even if no answer came back. A read's value only comes with the answer.
    value = invoke.value
    result = None
    if answer is not None:
        result = answer.result
        if invoke.op == "get":
            value = answer.value
    return Op(
        id,
        invoke.client,
        invoke.op,
        invoke.key,
        value,
        invoke.expected,
        result,
        start=start,
        end=end,
        time=invoke.time,
    )
