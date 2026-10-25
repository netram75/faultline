import pytest

from faultline.bench import BenchResult, main


def test_percentiles() -> None:
    result = BenchResult("writes", 1, 2.0, latencies=[i / 1000 for i in range(1, 101)])

    assert result.per_second == 50
    assert result.percentile(50) == 0.051
    assert result.percentile(99) == 0.1
    assert "50 ops/s" in result.line()


def test_a_short_run(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--seconds", "0.5", "--clients", "2", "--modes", "writes,reads"]) == 0

    lines = capsys.readouterr().out.splitlines()
    assert [line.split()[0] for line in lines] == ["writes", "reads"]
    for line in lines:
        assert "errors 0" in line
        assert float(line.split()[3]) > 0  # ops/s
