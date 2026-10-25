# The server: TCP, the Redis protocol, real processes

Everything before this ran inside one Python process: the simulator plays
the network, the disks and the clock. This part runs faultline for real.
Each node is its own process, with a real disk, a real clock, and TCP
between them. Clients talk to it with the Redis protocol, so `redis-cli` and
the usual Redis libraries work.

```
faultline serve --id 1 --cluster examples/local-cluster.toml
redis-cli -p 6401 SET greeting hello
```

([getting-started.md](getting-started.md) has the whole walk-through.)

## The pieces

```
              Redis clients                          other nodes
                   |                                      |
            client port (RESP)                    peer port (frames)
                   |                                      |
  net/server.py  ClientServer            net/node.py  NodeRunner
                   \______________________________________/
                                      |
                               kv/server.py  KVNode
                          (Raft + state machine, no I/O)
```

| File | What |
|---|---|
| `net/resp.py` | the Redis wire protocol: encode values, parse a byte stream |
| `net/requests.py` | turn a command like `[b"SET", b"k", b"v"]` into a request |
| `net/frames.py` | cut Raft messages out of a TCP stream: a length, then the bytes |
| `net/node.py` | `NodeRunner`: a KVNode on the real disk, ticking, talking to peers |
| `net/server.py` | `ClientServer`: the client port |
| `net/metrics.py` | counters, a latency histogram, INFO and `/metrics` |
| `net/cluster.py` | the cluster file |
| `net/client.py` | a small Redis client for the tests, chaos runs and the benchmark |
| `net/local.py` | a cluster of real processes on this machine, for tests and chaos |
| `cli.py` | the `faultline` command |

## One thread, one loop

A node is one asyncio event loop on one thread. The KVNode never waits for
anything (see [kv.md](kv.md)), so the runner can call it straight from the
loop: on every tick, on every message from a peer, on every client request.
After each call it sends whatever messages the node wants sent and hands out
the answers that are ready. No locks anywhere.

The disk is the one place that blocks. Every write to the Raft log and to the
storage engine ends in an fsync (on macOS, `F_FULLFSYNC`, see
[wal.md](wal.md)), and the loop waits for it. That is what makes an answer
mean "it is on disk". It is also why faultline is not fast (see
[benchmarks.md](benchmarks.md)).

## Between nodes

Raft messages ([raft/messages.py](../src/faultline/raft/messages.py)) already
have a byte format. On TCP each one goes in a frame: 4 bytes of length, then
the message. A node keeps one outgoing connection per peer and listens on its
own peer address for theirs. A message carries its sender, so there is no
handshake.

A connection that breaks is treated like lost messages, because to Raft
that is all it is. Messages that can't go out right now are dropped, and the
link connects again in the background, waiting a little longer each time (50
ms, doubling, up to a second). A peer that is alive but frozen fills up the
send queue, and after 1000 waiting messages the rest are dropped too.

Ticks are 10 ms, slower settings than the simulator's: a heartbeat every 50
ms, an election after 300 to 600 ms without one. A real machine has hiccups
(a slow fsync, a busy CPU), and an election for each of them would only make
things worse.

## The client port

Each connection is read with a RESP parser, and each command is answered in
the order it came, so a client may send many at once (pipelining).

| Command | Answer |
|---|---|
| `PING [message]` | `+PONG`, or the message |
| `GET key` | the value, or null |
| `SET key value` | `+OK` |
| `DEL key` | `:1`, always (faultline does not track whether the key was there) |
| `SETNX key value` | `:1` if it set the value, `:0` if the key already had one |
| `CAS key expected value` | `:1` if the key held `expected` and now holds `value`, else `:0` |
| `INFO` | numbers about this node, as text |

`CAS` is not a Redis command. Redis does compare-and-set with WATCH and
MULTI, which faultline doesn't have. `SETNX` is a CAS that expects the key to
be missing.

**Reads** go through ReadIndex ([raft.md](raft.md)), so a GET never returns
old data, even from a leader that was replaced and doesn't know it yet. Only
the leader answers them.

**Writes** go into the Raft log and are answered once they are applied.

**Errors** say what the client should do:

| Error | Meaning |
|---|---|
| `-NOTLEADER leader is node 2 at 127.0.0.1:6402` | ask that node |
| `-TRYAGAIN no leader right now` | an election is going on, ask again soon |
| `-TRYAGAIN the leader is just starting` | a new leader can't serve reads until it commits an entry of its own term |
| `-TIMEOUT ...` | no answer within the timeout (2 s by default) |
| `-ERR ...` | a bad command |

A `TIMEOUT` on a write means nobody knows whether it happened. It may still
happen later. NOTLEADER and TRYAGAIN on a write mean it surely did not
happen: the node never put it in its log.

### Sessions

Every connection is a client of its own: it gets a random 64-bit client id,
and its writes are numbered 1, 2, 3, ... like the simulator's clients (see
[kv.md](kv.md)). The server never retries anything itself, and a client that
reconnects is a new session. So a client that sends a write again after a
TIMEOUT may apply it twice, exactly like with real Redis.

**Sessions are never cleaned up.** Each client id that ever wrote something
keeps a small record in the state machine (its last seq and result), and
every connection is a new client id. So that table grows with the number of
connections ever made, forever. Real systems expire old sessions (the Raft
thesis, section 6.3, says how), and faultline doesn't yet. See
[limits.md](limits.md).

## Metrics

`INFO` over the client port, or Prometheus text at `/metrics` when the node
runs with `--metrics host:port`:

| Name | What |
|---|---|
| `faultline_is_leader`, `faultline_term`, `faultline_leader` | who leads, and in which term |
| `faultline_commit_index`, `faultline_applied_index` | how far the log is committed and applied |
| `faultline_log_first_index`, `faultline_log_last_index` | the log on disk, after snapshots |
| `faultline_messages_dropped_total` | messages to peers that could not be sent |
| `faultline_client_connections` | open client connections |
| `faultline_requests_total{command}`, `faultline_errors_total{kind}` | what clients asked, what went wrong |
| `faultline_request_seconds{command}` | how long commands took, as a histogram |

## How it is tested

* **Pieces on their own:** RESP and frames get random byte streams cut at
  random places. Requests, the cluster file and metrics have unit tests.
* **Nodes in one process** (`tests/net/test_node.py`, `test_server.py`):
  three runners and their client ports in one event loop, on real TCP.
  Election, replication, a leader that stops, a node that comes back and
  catches up, a full restart from disk, NOTLEADER, TRYAGAIN, TIMEOUT,
  pipelining, a command sent one byte at a time.
* **Real processes** (`tests/net/test_processes.py`): three `faultline serve`
  processes. kill -9 the leader, and every answered write is still there. Stop
  everything, start it again, the data is on disk. Freeze the leader with
  SIGSTOP while the others move on, and ask it for a key: it never answers
  with the old value. (In practice it answers NOTLEADER, because when it
  wakes, the new leader's messages are already waiting and it reads those
  first. So this test can't force the dangerous moment. The simulator can:
  see the `leader-read` bug in [BUGS.md](../BUGS.md).)
* **Chaos** (`python -m faultline.chaos`): real processes, kill -9 and
  SIGSTOP, clients over RESP, and the linearizability checker at the end. See
  [getting-started.md](getting-started.md#chaos).
* **Docker:** CI builds the image, starts three containers and runs
  `docker/smoke.py` against them.

We broke the new code on purpose to see that the tests notice. Caught: one
client id for every connection, a seq that never moves, NOTLEADER pointing at
the wrong node, a TIMEOUT answered as OK, reads that skip ReadIndex on a
follower, a frame length off by one, a frame that keeps its length bytes, and
in the chaos client, five wrong ways of writing down an outcome. Not caught:
a leader that answers reads from its own data without ReadIndex. As above,
real processes can't force that moment. The simulator catches it.
