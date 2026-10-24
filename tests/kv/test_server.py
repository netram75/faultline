from faultline.fs.fake import FakeFileSystem
from faultline.kv.commands import Command, CompareAndSet, Put
from faultline.kv.server import KVNode
from faultline.kv.state_machine import Result
from faultline.raft.node import Role
from tests.kv.cluster import KVCluster


def solo() -> KVNode:
    node = KVNode.open(FakeFileSystem(), "/solo", 1, [1])
    while node.raft.role is not Role.LEADER:
        node.tick()
    return node


def test_cluster_of_one_writes_and_reads() -> None:
    node = solo()
    put = Command(1, 1, Put(b"name", b"faultline"))

    assert node.submit(put)
    assert node.take_results() == [(put, Result(ok=True))]
    assert node.read(b"name", ctx=9)
    assert node.take_read_results() == [(9, b"faultline")]


def test_write_is_answered_once_it_is_applied() -> None:
    cluster = KVCluster(3, seed=1)
    leader = cluster.wait_for_leader()
    put = Command(1, 1, Put(b"k", b"v"))

    assert cluster.submit(leader, put)
    assert cluster.results == []  # not committed yet: nobody else has it
    cluster.tick(3)
    assert cluster.results == [(put, Result(ok=True))]

    # Followers learn the commit index from the next heartbeat
    cluster.tick(3)
    assert all(node.machine.get(b"k") == b"v" for node in cluster.nodes.values())


def test_followers_turn_requests_away() -> None:
    cluster = KVCluster(3, seed=2)
    leader = cluster.wait_for_leader()
    follower = next(i for i in cluster.ids if i != leader)

    assert not cluster.submit(follower, Command(1, 1, Put(b"k", b"v")))
    assert not cluster.read(follower, b"k", ctx=1)


def test_read_sees_a_finished_write() -> None:
    cluster = KVCluster(3, seed=3)
    leader = cluster.wait_for_leader()
    cluster.submit(leader, Command(1, 1, Put(b"k", b"v")))
    cluster.tick(3)

    assert cluster.read(leader, b"k", ctx=5)
    cluster.tick(3)

    assert cluster.read_results == {5: b"v"}


def test_retry_through_the_leader_runs_once() -> None:
    cluster = KVCluster(3, seed=4)
    leader = cluster.wait_for_leader()
    cas = Command(7, 1, CompareAndSet(b"k", None, b"first"))

    cluster.submit(leader, cas)
    cluster.submit(leader, cas)  # the client gave up waiting and sent it again
    cluster.tick(3)

    assert cluster.results == [(cas, Result(True, b"first")), (cas, Result(True, b"first"))]


def test_restart_does_not_apply_anything_twice() -> None:
    cluster = KVCluster(3, seed=5)
    leader = cluster.wait_for_leader()
    for seq in range(1, 31):
        cluster.submit(leader, Command(1, seq, Put(b"k%d" % (seq % 4), b"v%d" % seq)))
        cluster.tick()
    cluster.tick(5)

    for i in cluster.ids:
        cluster.crash(i)
    for i in cluster.ids:
        cluster.restart(i)  # would raise if an index were applied twice
    cluster.wait_for_leader()
    cluster.tick(5)

    cluster.check_same_data([b"k0", b"k1", b"k2", b"k3"])
    assert cluster.nodes[1].machine.get(b"k2") == b"v30"
