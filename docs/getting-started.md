# Getting started

How to run a faultline cluster on your machine, talk to it, break it, and
check that it still gave correct answers. Everything below was run on a Mac
while writing this page, and the output shown is what it printed.

## Install

You need Python 3.12 or newer and [uv](https://docs.astral.sh/uv/).

```
git clone https://github.com/netram75/faultline
cd faultline
uv sync
make check        # lint, type check and about 2000 tests, under a minute
```

## Three nodes on one machine

[examples/local-cluster.toml](../examples/local-cluster.toml) describes three
nodes on 127.0.0.1. Each has a peer port (7001 to 7003) for the other nodes
and a client port (6401 to 6403) for you. Start each node in its own
terminal:

```
uv run faultline serve --id 1 --cluster examples/local-cluster.toml
uv run faultline serve --id 2 --cluster examples/local-cluster.toml
uv run faultline serve --id 3 --cluster examples/local-cluster.toml
```

Each keeps its data in `data/node<ID>` (change it with `--data`). Within a
second one of them wins an election:

```
15:23:12,140 node 1: follower in term 0, leader None
15:23:12,610 node 1: pre-candidate in term 0, leader None
15:23:12,622 node 1: candidate in term 1, leader None
15:23:12,651 node 1: leader in term 1, leader 1
```

## Talk to it

The client ports speak the Redis protocol. With `redis-cli`, if you have it:

```
redis-cli -p 6401 SET greeting hello
redis-cli -p 6401 GET greeting
```

(The tests in this repo don't use redis-cli. They use the small client in
[net/client.py](../src/faultline/net/client.py), which is also handy from
Python.)

```python
import asyncio
from faultline.net.client import Client
from faultline.net.node import Address


async def main() -> None:
    for port in (6401, 6402, 6403):
        client = await Client.connect(Address("127.0.0.1", port))
        print(port, await client.call("SET", "greeting", "hello"))
        await client.close()


asyncio.run(main())
```

```
6401 SimpleString(text='OK')
6402 RespError(message='NOTLEADER leader is node 1 at 127.0.0.1:6401')
6403 RespError(message='NOTLEADER leader is node 1 at 127.0.0.1:6401')
```

Only the leader takes commands. The others say where it is. Then, on the
leader:

```
GET greeting                 -> b'hello'
CAS greeting hello hi        -> 1    (it held hello, now it holds hi)
CAS greeting hello hey       -> 0    (it holds hi, so nothing changes)
```

The commands are PING, GET, SET, DEL, SETNX, CAS and INFO. See
[net.md](net.md) for what each answers and what the errors mean.

## Look inside

`INFO` on any node:

```
# faultline
role:leader
node_id:1
is_leader:1
term:1
leader:1
commit_index:4
applied_index:4
log_first_index:1
log_last_index:4
messages_dropped:0
client_connections:1
requests_cas:2
requests_get:1
requests_set:1
```

Start a node with `--metrics 127.0.0.1:9101` and Prometheus can read the same
numbers, plus a latency histogram, at `http://127.0.0.1:9101/metrics`.

## Break it by hand

Stop the leader with Ctrl-C (or `kill -9` it). After half a second or so, one
of the other two takes over, and everything that was answered is still
there. Start it again with the same command: it reads its data from disk and
catches up with whatever it missed.

Stop two of the three, and the last one can't do anything: no majority, no
leader, every write answers `TRYAGAIN`. That is on purpose. A node on its
own can't know whether the other two are dead or only cut off from it and
still working, so it must not take writes.

## Chaos

`python -m faultline.chaos` does the breaking for you, and then checks the
result. It starts its own cluster of real processes, runs clients against it
that write down every answer they get, and kills (`kill -9`) or freezes
(`SIGSTOP`) one node at a time, the leader too. At the end the
linearizability checker ([checker.md](checker.md)) reads what the clients
wrote down.

```
uv run python -m faultline.chaos --seconds 15 --seed 2 --faults kill,leader
```

```
  0.36s kill -9 node 3, back after 1.3s
  3.03s pause node 1 for 0.6s
  4.80s kill -9 node 2, back after 0.8s
  7.45s kill -9 node 3, back after 1.4s
 10.28s kill -9 node 2, back after 0.9s
 12.67s kill -9 node 3, back after 1.4s
operations: 989 ok, 0 failed, 12 unknown
linearizable: ok
```

"unknown" are writes whose answer never came, because the node died or
froze after taking them. They may or may not have happened, and the checker
allows both. Unlike the simulator, a chaos run is not repeatable: the seed
picks the operations and the faults, but real processes and real time decide
the rest.

## The simulator

The simulator runs a whole cluster inside one process, on a fake clock,
network and disks, so a run is fully repeatable from its seed
([simulator.md](simulator.md)):

```
uv run python -m faultline.sim --seed 7 --seconds 30
uv run python -m faultline.sim --seed 0 --seconds 5 --bug no-sessions
```

The second one switches on a bug on purpose and shows the checker catching
it ([BUGS.md](../BUGS.md)).

## Docker

```
docker compose up --build
```

starts three nodes from [docker-compose.yml](../docker-compose.yml), with
client ports 6401, 6402 and 6403 on your machine. Inside the compose network
the nodes call each other `node1`, `node2` and `node3`, so NOTLEADER says
`node2:6379` where you would use port 6402. CI builds the image and runs
[docker/smoke.py](../docker/smoke.py) against these three containers on every
push. (Docker was not installed on the machine this was written on, so the
only place the Docker setup runs is CI.)
