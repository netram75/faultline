from importlib.metadata import version

import pytest

import faultline
from faultline.cli import main


def test_package_has_a_version() -> None:
    assert faultline.__version__ == "0.1.0"


def test_the_version_matches_the_package_metadata() -> None:
    assert version("faultline") == faultline.__version__


def test_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exit:
        main(["--version"])

    assert exit.value.code == 0
    assert capsys.readouterr().out.strip() == "faultline 0.1.0"
