import asyncio
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from faultline.cli import main
from faultline.net.client import Client
from faultline.net.node import Address
from faultline.net.resp import RespError, SimpleString
from tests.net.helpers import free_ports, run


def one_node_cluster(tmp_path: Path) -> tuple[Path, Address]:
    peer, client = free_ports(2)
    path = tmp_path / "cluster.toml"
    path.write_text(f'[nodes.1]\npeer = "127.0.0.1:{peer}"\nclient = "127.0.0.1:{client}"\n')
    return path, Address("127.0.0.1", client)


def test_unknown_node(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path, _ = one_node_cluster(tmp_path)

    with pytest.raises(SystemExit):
        main(["serve", "--id", "2", "--cluster", str(path)])
    assert "node 2 is not in" in capsys.readouterr().err


def test_bad_metrics_address(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path, _ = one_node_cluster(tmp_path)

    with pytest.raises(SystemExit):
        main(["serve", "--id", "1", "--cluster", str(path), "--metrics", "nope"])
    assert "host:port" in capsys.readouterr().err


def test_missing_cluster_file(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        main(["serve", "--id", "1", "--cluster", str(tmp_path / "nope.toml")])
    assert "nope.toml" in capsys.readouterr().err


def test_serve_for_real(tmp_path: Path) -> None:
    # The real command, in its own process: start it, use it, stop it
    path, client_address = one_node_cluster(tmp_path)
    [metrics_port] = free_ports(1)
    command = [sys.executable, "-m", "faultline", "serve", "--id", "1", "--cluster", str(path)]
    command += ["--metrics", f"127.0.0.1:{metrics_port}"]
    process = subprocess.Popen(
        [*command, "--data", str(tmp_path / "data")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:

        async def body() -> None:
            client = None
            while client is None:
                try:
                    client = await Client.connect(client_address)
                except OSError:
                    await asyncio.sleep(0.05)  # not listening yet
            assert await client.call("PING") == SimpleString("PONG")
            while isinstance(await client.call("SET", "k", "v"), RespError):
                await asyncio.sleep(0.05)  # no leader yet
            assert await client.call("GET", "k") == b"v"
            await client.close()

            reader, writer = await asyncio.open_connection("127.0.0.1", metrics_port)
            writer.write(b"GET /metrics HTTP/1.1\r\n\r\n")
            assert b"faultline_is_leader 1" in await reader.read()
            writer.close()

        run(body, timeout=20)
    finally:
        process.send_signal(signal.SIGTERM)
        _, err = process.communicate(timeout=10)

    assert process.returncode == 0, err.decode()
    assert b"node 1 stopped" in err
