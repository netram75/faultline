from faultline.fs.fake import FakeFileSystem
from faultline.kv.commands import Command, CompareAndSet, Put
from faultline.kv.server import KVNode
from faultline.kv.state_machine import Result
from faultline.raft.messages import InstallSnapshot, Message
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


def test_logs_stay_short_with_snapshots() -> None:
    cluster = KVCluster(3, seed=6, snapshot_every=10)
    leader = cluster.wait_for_leader()
    for seq in range(1, 61):
        cluster.submit(leader, Command(1, seq, Put(b"k%d" % (seq % 5), b"v%d" % seq)))
        cluster.tick()
    cluster.tick(5)

    for node in cluster.nodes.values():
        assert node.raft.snapshot is not None
        assert node.raft.log.base_index >= 50
        assert node.raft.log.last_index - node.raft.log.base_index <= 15
    cluster.check_same_data([b"k%d" % i for i in range(5)])

    for i in cluster.ids:
        cluster.crash(i)
    for i in cluster.ids:
        cluster.restart(i)
    cluster.wait_for_leader()
    cluster.tick(5)

    cluster.check_same_data([b"k%d" % i for i in range(5)])
    assert cluster.nodes[2].machine.get(b"k0") == b"v60"


def test_follower_far_behind_catches_up_from_a_snapshot() -> None:
    cluster = KVCluster(3, seed=7, snapshot_every=10)
    leader = cluster.wait_for_leader()
    behind = next(i for i in cluster.ids if i != leader)
    cluster.partition({behind})
    for seq in range(1, 41):
        cluster.submit(leader, Command(1, seq, Put(b"k%d" % (seq % 3), b"v%d" % seq)))
        cluster.tick()
    cluster.tick(3)
    assert cluster.nodes[leader].raft.log.base_index > 0

    cluster.heal()
    cluster.tick(20)

    assert cluster.nodes[behind].raft.snapshot is not None
    assert cluster.nodes[behind].machine.get(b"k1") == b"v40"
    cluster.check_same_data([b"k0", b"k1", b"k2"])


def test_snapshot_saved_but_not_loaded_is_loaded_at_the_next_start() -> None:
    cluster = KVCluster(3, seed=8, snapshot_every=10)
    leader = cluster.wait_for_leader()
    for seq in range(1, 21):
        cluster.submit(leader, Command(1, seq, Put(b"k", b"v%d" % seq)))
        cluster.tick()
    cluster.tick(3)
    snapshot = cluster.nodes[leader].raft.snapshot
    assert snapshot is not None

    # A brand new node gets the snapshot. Raft saves it, then the power goes
    # before the state machine loads it.
    fs = FakeFileSystem(seed=99)
    fresh = KVNode.open(fs, "/fresh", 4, [1, 2, 3, 4])
    fresh.raft.step(_install(snapshot.index, snapshot.term, snapshot.data))
    fs.crash()

    fresh = KVNode.open(fs, "/fresh", 4, [1, 2, 3, 4])

    assert fresh.machine.applied_index == snapshot.index
    assert fresh.machine.snapshot() == snapshot.data


def _install(index: int, term: int, data: bytes) -> Message:
    return Message(1, 4, term, InstallSnapshot(index, term, data))
