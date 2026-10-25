"""Numbers about a running node, for people and for Prometheus.

Two ways to read them:

* **INFO**, over the Redis protocol (redis-cli INFO): one name:value per
  line, the way Redis prints its own INFO.
* **GET /metrics** over HTTP, in Prometheus' text format, when the node is
  started with --metrics host:port.

What is counted:

    requests by command, errors by kind (NOTLEADER, TIMEOUT, ...)
    how long each command took, in fixed buckets (a histogram)
    the node's state, read when asked: role, term, commit and applied
    index, where the log starts and ends, messages dropped, connections
"""

import asyncio
import logging
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field

from faultline.net.node import Address, NodeRunner
from faultline.raft.node import Role

log = logging.getLogger("faultline")

# Upper edges of the latency buckets, in seconds
BUCKETS = (0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0)


@dataclass
class Histogram:
    counts: list[int] = field(default_factory=lambda: [0] * len(BUCKETS))
    total: int = 0
    sum: float = 0.0

    def observe(self, seconds: float) -> None:
        self.total += 1
        self.sum += seconds
        for i, edge in enumerate(BUCKETS):
            if seconds <= edge:
                self.counts[i] += 1
                break


@dataclass
class Metrics:
    requests: Counter[str] = field(default_factory=Counter)
    errors: Counter[str] = field(default_factory=Counter)
    latency: dict[str, Histogram] = field(default_factory=dict)
    connections: int = 0

    def record(self, command: str, seconds: float, error: str | None = None) -> None:
        self.requests[command] += 1
        self.latency.setdefault(command, Histogram()).observe(seconds)
        if error is not None:
            self.errors[error] += 1


def node_state(runner: NodeRunner) -> dict[str, int]:
    raft = runner.kv.raft
    return {
        "node_id": runner.id,
        "is_leader": int(raft.role is Role.LEADER),
        "term": raft.term,
        "leader": raft.leader or 0,
        "commit_index": raft.commit_index,
        "applied_index": runner.kv.machine.applied_index,
        "log_first_index": raft.log.base_index + 1,
        "log_last_index": raft.log.last_index,
        "messages_dropped": runner.dropped(),
    }


def info_text(metrics: Metrics, runner: NodeRunner) -> str:
    """For the INFO command."""
    lines = ["# faultline", f"role:{runner.kv.raft.role.value}"]
    lines += [f"{name}:{value}" for name, value in node_state(runner).items()]
    lines.append(f"client_connections:{metrics.connections}")
    lines += [f"requests_{name}:{count}" for name, count in sorted(metrics.requests.items())]
    lines += [f"errors_{name.lower()}:{count}" for name, count in sorted(metrics.errors.items())]
    return "\r\n".join(lines) + "\r\n"


def prometheus_text(metrics: Metrics, runner: NodeRunner) -> str:
    """For GET /metrics. See https://prometheus.io/docs/instrumenting/exposition_formats/"""
    out: list[str] = []
    for name, value in node_state(runner).items():
        kind = "counter" if name == "messages_dropped" else "gauge"
        full = f"faultline_{name}_total" if kind == "counter" else f"faultline_{name}"
        out += [f"# TYPE {full} {kind}", f"{full} {value}"]
    out += ["# TYPE faultline_client_connections gauge"]
    out += [f"faultline_client_connections {metrics.connections}"]

    out.append("# TYPE faultline_requests_total counter")
    for command, count in sorted(metrics.requests.items()):
        out.append(f'faultline_requests_total{{command="{command}"}} {count}')
    out.append("# TYPE faultline_errors_total counter")
    for kind, count in sorted(metrics.errors.items()):
        out.append(f'faultline_errors_total{{kind="{kind}"}} {count}')

    out.append("# TYPE faultline_request_seconds histogram")
    for command, histogram in sorted(metrics.latency.items()):
        label = f'command="{command}"'
        running = 0
        for edge, count in zip(BUCKETS, histogram.counts, strict=True):
            running += count  # Prometheus buckets count everything up to their edge
            out.append(f'faultline_request_seconds_bucket{{{label},le="{edge}"}} {running}')
        out.append(f'faultline_request_seconds_bucket{{{label},le="+Inf"}} {histogram.total}')
        out.append(f"faultline_request_seconds_sum{{{label}}} {histogram.sum:.6f}")
        out.append(f"faultline_request_seconds_count{{{label}}} {histogram.total}")
    return "\n".join(out) + "\n"


class MetricsServer:
    """The smallest HTTP server that Prometheus can scrape: GET /metrics
    and nothing else. One request per connection."""

    def __init__(self, address: Address, render: Callable[[], str]) -> None:
        self.address = address
        self._render = render
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._serve, self.address.host, self.address.port)
        log.info("metrics on http://%s/metrics", self.address)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
            request_line = head.split(b"\r\n", 1)[0].split()
            if request_line[:2] == [b"GET", b"/metrics"]:
                body = self._render().encode()
                status = b"200 OK"
                kind = b"text/plain; version=0.0.4"
            else:
                body, status, kind = b"not found\n", b"404 Not Found", b"text/plain"
            writer.write(
                b"HTTP/1.1 "
                + status
                + b"\r\nContent-Type: "
                + kind
                + b"\r\nContent-Length: %d\r\nConnection: close\r\n\r\n" % len(body)
                + body
            )
            await writer.drain()
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError, OSError):
            pass  # not HTTP, too big or too slow: just hang up
        finally:
            writer.close()
