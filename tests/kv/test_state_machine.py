import pytest

from faultline.fs.fake import FakeFileSystem, PowerCut
from faultline.kv.commands import Command, CompareAndSet, Delete, Operation, Put, encode
from faultline.kv.state_machine import Result, StateMachine
from faultline.lsm import Options

DIR = "/node/data"
SMALL = Options(memtable_size=512, l0_limit=2, table_size=1024)


def cmd(op: Operation, client: int = 1, seq: int = 1) -> bytes:
    return encode(Command(client, seq, op))


def test_put_get_delete() -> None:
    sm = StateMachine.open(FakeFileSystem(), DIR)

    assert sm.apply(1, cmd(Put(b"k", b"v"))) == Result(ok=True)
    assert sm.get(b"k") == b"v"
    assert sm.apply(2, cmd(Delete(b"k"), seq=2)) == Result(ok=True)
    assert sm.get(b"k") is None
    assert sm.applied_index == 2


def test_compare_and_set() -> None:
    sm = StateMachine.open(FakeFileSystem(), DIR)

    assert sm.apply(1, cmd(CompareAndSet(b"k", None, b"a"), seq=1)) == Result(True, b"a")
    assert sm.apply(2, cmd(CompareAndSet(b"k", None, b"b"), seq=2)) == Result(False, b"a")
    assert sm.apply(3, cmd(CompareAndSet(b"k", b"a", b"c"), seq=3)) == Result(True, b"c")
    assert sm.apply(4, cmd(CompareAndSet(b"k", b"a", b"d"), seq=4)) == Result(False, b"c")
    assert sm.get(b"k") == b"c"


def test_no_op_moves_the_applied_index_only() -> None:
    sm = StateMachine.open(FakeFileSystem(), DIR)

    assert sm.apply(1, b"") is None
    assert sm.applied_index == 1


def test_indexes_must_come_in_order() -> None:
    sm = StateMachine.open(FakeFileSystem(), DIR)
    sm.apply(1, b"")

    with pytest.raises(ValueError, match="applying 3 after 1"):
        sm.apply(3, b"")
    with pytest.raises(ValueError, match="applying 1 after 1"):
        sm.apply(1, b"")


def test_user_keys_cant_clash_with_ours() -> None:
    sm = StateMachine.open(FakeFileSystem(), DIR)
    sm.apply(1, cmd(Put(b"m/applied", b"not a number")))

    assert sm.get(b"m/applied") == b"not a number"
    assert sm.applied_index == 1


def test_applied_index_survives_a_restart() -> None:
    fs = FakeFileSystem()
    sm = StateMachine.open(fs, DIR, SMALL)
    for i in range(1, 101):
        sm.apply(i, cmd(Put(f"key{i % 7}".encode(), f"value{i}".encode()), seq=i))
    fs.crash()

    sm = StateMachine.open(fs, DIR, SMALL)

    assert sm.applied_index == 100
    assert sm.get(b"key2") == b"value100"


def test_change_and_applied_index_are_saved_together() -> None:
    # Cut the power at every step of one apply. After the restart, either the
    # command is fully there (data and index) or not at all.
    steps = 0
    while True:
        fs = FakeFileSystem(seed=steps)
        sm = StateMachine.open(fs, DIR)
        sm.apply(1, cmd(Put(b"k", b"old")))
        fs.cut_power_after(steps)
        try:
            sm.apply(2, cmd(Put(b"k", b"new"), seq=2))
        except PowerCut:
            pass
        else:
            break  # the apply finished before the cut, every step was tried
        sm = StateMachine.open(fs, DIR)
        assert (sm.applied_index, sm.get(b"k")) in [(1, b"old"), (2, b"new")]
        steps += 1
    assert steps > 0


def test_retry_gets_the_saved_result_and_runs_once() -> None:
    sm = StateMachine.open(FakeFileSystem(), DIR)
    first = cmd(CompareAndSet(b"k", None, b"a"), client=7, seq=1)

    assert sm.apply(1, first) == Result(True, b"a")
    # The client never got the answer and sends it again. Run a second time,
    # the compare would fail. It must get the first answer instead.
    assert sm.apply(2, first) == Result(True, b"a")
    assert sm.get(b"k") == b"a"
    assert sm.applied_index == 2


def test_retry_does_not_undo_later_writes() -> None:
    sm = StateMachine.open(FakeFileSystem(), DIR)
    sm.apply(1, cmd(Put(b"k", b"one"), client=7, seq=1))
    sm.apply(2, cmd(Put(b"k", b"two"), client=8, seq=1))

    # A late copy of client 7's put must not overwrite client 8's newer value
    assert sm.apply(3, cmd(Put(b"k", b"one"), client=7, seq=1)) == Result(ok=True)
    assert sm.get(b"k") == b"two"


def test_older_command_is_ignored() -> None:
    sm = StateMachine.open(FakeFileSystem(), DIR)
    sm.apply(1, cmd(Put(b"k", b"a"), client=7, seq=1))
    sm.apply(2, cmd(Put(b"k", b"b"), client=7, seq=2))

    assert sm.apply(3, cmd(Put(b"k", b"a"), client=7, seq=1)) is None
    assert sm.get(b"k") == b"b"


def test_clients_have_their_own_sessions() -> None:
    sm = StateMachine.open(FakeFileSystem(), DIR)
    sm.apply(1, cmd(Put(b"a", b"1"), client=1, seq=1))
    sm.apply(2, cmd(Put(b"b", b"2"), client=2, seq=1))

    assert sm.get(b"a") == b"1"
    assert sm.get(b"b") == b"2"


def test_sessions_survive_a_restart() -> None:
    fs = FakeFileSystem()
    sm = StateMachine.open(fs, DIR, SMALL)
    first = cmd(CompareAndSet(b"k", None, b"a"), client=7, seq=1)
    sm.apply(1, first)
    fs.crash()

    sm = StateMachine.open(fs, DIR, SMALL)

    assert sm.apply(2, first) == Result(True, b"a")
    assert sm.get(b"k") == b"a"


def test_session_is_saved_with_the_change() -> None:
    # Power cut at every step of a compare-and-set, then the client retries.
    # Whether or not the first try made it, the retry must end with the key
    # set once and the answer the first try would have given.
    steps = 0
    while True:
        fs = FakeFileSystem(seed=steps)
        sm = StateMachine.open(fs, DIR)
        sm.apply(1, b"")
        first = cmd(CompareAndSet(b"k", None, b"a"), client=7, seq=1)
        fs.cut_power_after(steps)
        try:
            sm.apply(2, first)
        except PowerCut:
            pass
        else:
            break
        sm = StateMachine.open(fs, DIR)
        assert sm.apply(sm.applied_index + 1, first) == Result(True, b"a")
        assert sm.get(b"k") == b"a"
        steps += 1
    assert steps > 0


def test_snapshot_is_the_same_for_the_same_data() -> None:
    a = StateMachine.open(FakeFileSystem(seed=1), DIR, SMALL)
    b = StateMachine.open(FakeFileSystem(seed=2), DIR, SMALL)
    for i in range(1, 41):
        command = cmd(Put(b"k%d" % (i % 9), b"v%d" % i), seq=i)
        a.apply(i, command)
        b.apply(i, command)

    # Different disks, different flush timing, same data: same snapshot
    assert a.snapshot() == b.snapshot()
    assert b"k3" in a.snapshot()


def test_restore_replaces_everything() -> None:
    leader = StateMachine.open(FakeFileSystem(seed=1), DIR, SMALL)
    for i in range(1, 21):
        leader.apply(i, cmd(Put(b"k%d" % (i % 4), b"v%d" % i), seq=i))
    behind = StateMachine.open(FakeFileSystem(seed=2), DIR, SMALL)
    behind.apply(1, cmd(Put(b"only here", b"x"), client=9))

    behind.restore(leader.snapshot())

    assert behind.applied_index == 20
    assert behind.get(b"only here") is None
    assert behind.get(b"k0") == b"v20"
    assert behind.snapshot() == leader.snapshot()
    # The sessions came along: a late retry is still spotted
    assert behind.apply(21, cmd(Put(b"k0", b"v4"), seq=4)) is None
    assert behind.get(b"k0") == b"v20"
