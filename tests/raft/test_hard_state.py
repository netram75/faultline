import pytest

from faultline.fs.fake import FakeFileSystem, PowerCut
from faultline.raft.hard_state import CorruptState, HardState, load, save

ROOT = "/node1"


def new_fs() -> FakeFileSystem:
    fs = FakeFileSystem()
    fs.mkdir(ROOT)
    return fs


@pytest.mark.parametrize(
    "state", [HardState(), HardState(7, None), HardState(7, 0), HardState(2**40, 2**32 - 1)]
)
def test_round_trip(state: HardState) -> None:
    assert HardState.decode(state.encode()) == state


def test_a_new_node_starts_at_term_zero_with_no_vote() -> None:
    assert load(new_fs(), ROOT) == HardState(0, None)


def test_saved_state_survives_a_crash() -> None:
    fs = new_fs()
    save(fs, ROOT, HardState(3, 2))
    save(fs, ROOT, HardState(4, None))

    fs.crash()

    assert load(fs, ROOT) == HardState(4, None)


def test_every_flipped_bit_is_caught() -> None:
    data = HardState(5, 3).encode()
    for i in range(len(data) * 8):
        damaged = bytearray(data)
        damaged[i // 8] ^= 1 << (i % 8)
        with pytest.raises(CorruptState):
            HardState.decode(bytes(damaged))


def test_every_cut_is_caught() -> None:
    data = HardState(5, 3).encode()
    for cut in range(len(data)):
        with pytest.raises(CorruptState):
            HardState.decode(data[:cut])


def test_power_cut_during_a_save_keeps_old_or_new() -> None:
    old, new = HardState(1, 1), HardState(2, 3)
    for steps in range(10):
        fs = new_fs()
        save(fs, ROOT, old)
        fs.cut_power_after(steps)
        try:
            save(fs, ROOT, new)
        except PowerCut:
            assert load(fs, ROOT) in (old, new)
