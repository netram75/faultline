from pathlib import Path

import pytest

from faultline.sim.__main__ import main
from faultline.sim.history import History
from faultline.sim.run import simulate


def test_a_run_sums_itself_up() -> None:
    summary, _, nemesis = simulate(seed=1, seconds=10, faults="heavy")

    assert summary.ok > 100
    assert summary.crashes + summary.power_cuts > 0
    assert summary.highest_term > 1
    assert summary.checks > 1000
    assert not summary.stuck
    assert len(nemesis.log) > 5


def test_quiet_run_has_no_trouble() -> None:
    summary, _, nemesis = simulate(seed=2, seconds=5, faults="none")

    assert nemesis.log == [(5000, "calm: network healed, every node started, clocks fixed")]
    assert summary.failed == summary.unknown == summary.crashes == 0
    assert summary.highest_term == 1


def test_cli_single_run(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = tmp_path / "history.jsonl"

    code = main(["--seed", "3", "--seconds", "5", "--history", str(path), "--log"])

    out = capsys.readouterr().out
    assert code == 0
    assert "seed 3, 5 s of heavy faults, then calm" in out
    assert "safety checks passed" in out
    assert "calm: network healed" in out
    assert len(History.from_jsonl(path.read_text()).events) > 50


def test_cli_many_runs(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["--seed", "10", "--runs", "3", "--seconds", "3"])

    lines = capsys.readouterr().out.splitlines()
    assert code == 0
    assert [line.split(":")[0] for line in lines] == ["seed 10", "seed 11", "seed 12"]
    assert all(": ok, " in line for line in lines)


def test_same_seed_same_summary() -> None:
    assert simulate(seed=4, seconds=5)[0] == simulate(seed=4, seconds=5)[0]
