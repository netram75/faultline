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

    assert sm.apply(1, cmd(CompareAndSet(b"k", None, b"a"))) == Result(True, b"a")
    assert sm.apply(2, cmd(CompareAndSet(b"k", None, b"b"))) == Result(False, b"a")
    assert sm.apply(3, cmd(CompareAndSet(b"k", b"a", b"c"))) == Result(True, b"c")
    assert sm.apply(4, cmd(CompareAndSet(b"k", b"a", b"d"))) == Result(False, b"c")
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
