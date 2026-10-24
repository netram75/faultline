"""The known bugs in sim/bugs.py. The checker must catch each one, and a bug
must never stay switched on after its run."""

import pytest

from faultline.kv.server import KVNode
from faultline.sim.__main__ import main
from faultline.sim.bugs import BUGS, switched_on
from faultline.sim.run import simulate

# For each bug, a run that catches it. A run is fixed by its seed, so these
# catch it every time.
CAUGHT = {
    "leader-read": (28, 10, "heavy"),
    "no-sessions": (0, 5, "heavy"),
    "early-ack": (0, 5, "heavy"),
}


def test_every_bug_has_a_run_that_catches_it() -> None:
    assert set(CAUGHT) == set(BUGS)


@pytest.mark.parametrize("bug", sorted(BUGS))
def test_the_checker_catches_each_bug(bug: str) -> None:
    seed, seconds, faults = CAUGHT[bug]

    with_bug, _, _ = simulate(seed, seconds, faults, bug=bug)
    without, _, _ = simulate(seed, seconds, faults)

    # The safety checks inside Raft passed both times (or simulate() would
    # have raised). These bugs are all in what a node tells its clients,
    # which only the checker looks at.
    assert with_bug.linearizable == "bad"
    assert without.linearizable == "ok"


def test_a_bug_is_only_on_inside_its_block() -> None:
    original = KVNode.read

    with switched_on("leader-read"):
        assert KVNode.read is not original
    assert KVNode.read is original


def test_a_bug_is_switched_off_even_after_an_error() -> None:
    original = KVNode.submit

    with pytest.raises(RuntimeError), switched_on("early-ack"):
        raise RuntimeError("boom")
    assert KVNode.submit is original


def test_no_bug_changes_nothing() -> None:
    original = KVNode.submit

    with switched_on(None):
        assert KVNode.submit is original


def test_cli_explains_what_went_wrong(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["--seed", "0", "--seconds", "5", "--bug", "no-sessions"])

    out = capsys.readouterr().out
    assert code == 1
    assert "with bug no-sessions switched on" in out
    assert "linearizable: bad  <- NOT LINEARIZABLE" in out
    assert "The first answer that can't be explained:" in out
    assert "--bug no-sessions --history history.jsonl" in out
