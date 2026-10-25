import asyncio

from faultline.chaos.run import run_chaos


def test_a_short_chaos_run() -> None:
    result = asyncio.run(asyncio.wait_for(run_chaos(4, seed=5, faults=("kill",)), 60))

    assert result.faults, "the nemesis did nothing"
    assert result.counts["ok"] > 20
    assert result.report.verdict == "ok"
