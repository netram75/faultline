from pathlib import Path
from typing import Any

import pytest

from faultline.net.cluster import BadCluster, load_cluster, parse_cluster
from faultline.net.node import Address

EXAMPLE = Path(__file__).parents[2] / "examples" / "local-cluster.toml"


def test_the_example_file() -> None:
    cluster = load_cluster(str(EXAMPLE))

    assert cluster.peers == {i: Address("127.0.0.1", 7000 + i) for i in (1, 2, 3)}
    assert cluster.clients == {i: Address("127.0.0.1", 6400 + i) for i in (1, 2, 3)}


@pytest.mark.parametrize(
    ("data", "problem"),
    [
        ({}, r"no \[nodes.N\] sections"),
        ({"nodes": {}}, r"no \[nodes.N\] sections"),
        ({"nodes": {"one": {}}}, "whole numbers"),
        ({"nodes": {"0": {}}}, "whole numbers"),
        ({"nodes": {"1": "x"}}, "must be a section"),
        ({"nodes": {"1": {"client": "h:1"}}}, "needs peer"),
        ({"nodes": {"1": {"peer": "h:1"}}}, "needs client"),
        ({"nodes": {"1": {"peer": "h", "client": "h:2"}}}, "host:port"),
        ({"nodes": {"1": {"peer": "h:1", "client": "h:1"}}}, "two addresses are the same"),
    ],
)
def test_mistakes(data: dict[str, Any], problem: str) -> None:
    with pytest.raises(BadCluster, match=problem):
        parse_cluster(data)


def test_not_toml(tmp_path: Path) -> None:
    path = tmp_path / "cluster.toml"
    path.write_text("[nodes.1\n")

    with pytest.raises(BadCluster, match=r"cluster\.toml"):
        load_cluster(str(path))
