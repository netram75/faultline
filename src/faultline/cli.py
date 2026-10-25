"""The faultline command.

    faultline serve --id 1 --cluster cluster.toml [--data DIR] [--metrics HOST:PORT]
    faultline --version

starts one node of the cluster described in the cluster file (see
net/cluster.py), and runs until it gets Ctrl-C or SIGTERM. Start one per
node, each with its own id. Talk to any of them with redis-cli or any Redis
client, on the node's client address.

The simulator has its own command: python -m faultline.sim.
"""

import argparse
import asyncio
import logging
import os
import signal
import sys

from faultline import __version__
from faultline.net.cluster import BadCluster, Cluster, load_cluster
from faultline.net.metrics import MetricsServer, prometheus_text
from faultline.net.node import Address, NodeRunner
from faultline.net.server import ClientServer
from faultline.raft.messages import NodeId


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="faultline", description=__doc__.split("\n\n")[0])
    parser.add_argument("--version", action="version", version=f"faultline {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="run one node of a cluster")
    serve.add_argument("--id", type=int, required=True, help="this node's id in the cluster file")
    serve.add_argument("--cluster", required=True, help="the cluster file (TOML)")
    serve.add_argument("--data", help="where this node keeps its data (default: data/node<ID>)")
    serve.add_argument(
        "--timeout", type=float, default=2.0, help="seconds a client waits for an answer"
    )
    serve.add_argument("--metrics", help="serve Prometheus metrics on this host:port")
    args = parser.parse_args(argv)

    try:
        cluster = load_cluster(args.cluster)
    except (BadCluster, OSError) as e:
        parser.error(str(e))
    if args.id not in cluster.peers:
        parser.error(f"node {args.id} is not in {args.cluster}")
    try:
        metrics = Address.parse(args.metrics) if args.metrics else None
    except ValueError as e:
        parser.error(str(e))

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stderr)
    data = args.data or os.path.join("data", f"node{args.id}")
    asyncio.run(serve_node(args.id, cluster, data, args.timeout, metrics))
    return 0


async def serve_node(
    node_id: NodeId, cluster: Cluster, data: str, timeout: float, metrics: Address | None = None
) -> None:
    os.makedirs(data, exist_ok=True)
    runner = NodeRunner(node_id, cluster.peers, data)
    server = ClientServer(runner, cluster.clients, timeout)
    await runner.start()
    await server.start()
    scraper = None
    if metrics is not None:
        scraper = MetricsServer(metrics, lambda: prometheus_text(server.metrics, runner))
        await scraper.start()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()

    if scraper is not None:
        await scraper.stop()
    await server.stop()
    await runner.stop()
