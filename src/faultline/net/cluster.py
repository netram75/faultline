"""The cluster file: which nodes there are and where to reach them.

    [nodes.1]
    peer = "127.0.0.1:7001"     the other nodes connect here
    client = "127.0.0.1:6401"   Redis clients connect here

One [nodes.N] section per node, N being its id. Every node gets the same
file, so they all agree on who is in the cluster. (faultline can't add or
remove nodes while it runs. To change the cluster, stop it, change the
file everywhere, start it again.)
"""

import tomllib
from dataclasses import dataclass
from typing import Any

from faultline.net.node import Address
from faultline.raft.messages import NodeId


class BadCluster(Exception):
    """The cluster file is wrong. The message says how."""


@dataclass(frozen=True)
class Cluster:
    peers: dict[NodeId, Address]
    clients: dict[NodeId, Address]


def load_cluster(path: str) -> Cluster:
    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise BadCluster(f"{path}: {e}") from e
    return parse_cluster(data, path)


def parse_cluster(data: dict[str, Any], where: str = "cluster") -> Cluster:
    nodes = data.get("nodes")
    if not isinstance(nodes, dict) or not nodes:
        raise BadCluster(f"{where}: no [nodes.N] sections")
    peers: dict[NodeId, Address] = {}
    clients: dict[NodeId, Address] = {}
    for name, node in nodes.items():
        if not name.isdigit() or int(name) < 1:
            raise BadCluster(f"{where}: node ids are whole numbers from 1, not {name!r}")
        node_id = int(name)
        if not isinstance(node, dict):
            raise BadCluster(f"{where}: [nodes.{name}] must be a section")
        peers[node_id] = _address(node, "peer", f"{where}: [nodes.{name}]")
        clients[node_id] = _address(node, "client", f"{where}: [nodes.{name}]")
    taken = list(peers.values()) + list(clients.values())
    if len(set(taken)) != len(taken):
        raise BadCluster(f"{where}: two addresses are the same")
    return Cluster(peers, clients)


def _address(node: dict[str, Any], field: str, where: str) -> Address:
    text = node.get(field)
    if not isinstance(text, str):
        raise BadCluster(f'{where} needs {field} = "host:port"')
    try:
        return Address.parse(text)
    except ValueError as e:
        raise BadCluster(f"{where}: {e}") from e
