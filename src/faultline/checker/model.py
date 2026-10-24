"""A model of one key: what a single copy of the data would do.

The checker asks: is there an order of the operations that a single copy,
doing one at a time, could have run and given exactly the answers we saw?
To try an order, it needs to know what each operation does to one key. That
is this file.

The state of a key is its value, or None if it is missing. step() runs one
operation on a state. It says whether the answer we saw is possible there,
and what the state is after it.
"""

from faultline.checker.ops import Op

type State = str | None


def step(state: State, op: Op) -> tuple[bool, State]:
    """Run op on state. Returns (possible, state after). When it is not
    possible, the state after means nothing."""
    if op.kind == "put":
        return True, op.value
    if op.kind == "get":
        return op.value == state, state

    # A compare-and-set
    matches = state == op.expected
    if op.result is None:
        # No answer came. If it ran, it wrote only when the compare matched.
        return True, op.value if matches else state
    if op.result:
        return matches, op.value
    return not matches, state
