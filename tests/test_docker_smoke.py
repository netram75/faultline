"""CI runs docker/smoke.py on the Docker cluster. Here it runs on a cluster
of plain processes, so we know the script itself works."""

import subprocess
import sys
from pathlib import Path

from faultline.net.local import LocalProcesses

SMOKE = Path(__file__).parents[1] / "docker" / "smoke.py"


def test_smoke_script(tmp_path: Path) -> None:
    with LocalProcesses(str(tmp_path)) as cluster:
        cluster.start_all()
        ports = [str(cluster.clients[i].port) for i in cluster.ids]
        result = subprocess.run(
            [sys.executable, str(SMOKE), *ports], capture_output=True, text=True, timeout=90
        )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "SET and GET work" in result.stdout
