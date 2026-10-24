"""Crash behaviour of the fake disk. The normal behaviour is in test_contract.py."""

import pytest

from faultline.fs.fake import FakeFileSystem, PowerCut


def new_fs(seed: int = 0) -> FakeFileSystem:
    fs = FakeFileSystem(seed)
    fs.mkdir("/data")
    return fs


def write_safely(fs: FakeFileSystem, path: str, data: bytes) -> None:
    """Write a file and do both syncs, so it must survive any crash."""
    f = fs.open_append(path)
    f.write(data)
    f.sync()
    f.close()
    fs.sync_dir("/data")


def test_synced_file_survives_a_crash() -> None:
    fs = new_fs()
    write_safely(fs, "/data/a", b"safe")

    fs.crash()

    assert fs.read_file("/data/a") == b"safe"


def test_new_file_is_gone_if_folder_was_not_synced() -> None:
    fs = new_fs()
    f = fs.open_append("/data/a")
    f.write(b"data")
    f.sync()  # the bytes are safe, but the name is not

    fs.crash()

    assert not fs.exists("/data/a")


def test_unsynced_tail_is_lost_kept_or_torn() -> None:
    seen = set()
    for seed in range(100):
        fs = new_fs(seed)
        write_safely(fs, "/data/a", b"synced|")
        f = fs.open_append("/data/a")
        f.write(b"not synced")

        fs.crash()

        data = fs.read_file("/data/a")
        assert data.startswith(b"synced|")
        tail = data.removeprefix(b"synced|")
        assert b"not synced".startswith(tail)
        if tail == b"":
            seen.add("lost")
        elif tail == b"not synced":
            seen.add("kept")
        else:
            seen.add("torn")

    assert seen == {"lost", "kept", "torn"}


def test_same_seed_crashes_the_same_way() -> None:
    def run(seed: int) -> bytes:
        fs = new_fs(seed)
        write_safely(fs, "/data/a", b"")
        f = fs.open_append("/data/a")
        f.write(b"0123456789" * 10)
        fs.crash()
        return fs.read_file("/data/a")

    assert run(7) == run(7)


def test_rename_is_undone_if_folder_was_not_synced() -> None:
    fs = new_fs()
    write_safely(fs, "/data/tmp", b"new")

    fs.rename("/data/tmp", "/data/current")
    fs.crash()

    assert fs.exists("/data/tmp")
    assert not fs.exists("/data/current")


def test_rename_survives_after_sync_dir() -> None:
    fs = new_fs()
    write_safely(fs, "/data/tmp", b"new")

    fs.rename("/data/tmp", "/data/current")
    fs.sync_dir("/data")
    fs.crash()

    assert fs.read_file("/data/current") == b"new"
    assert not fs.exists("/data/tmp")


def test_remove_is_undone_if_folder_was_not_synced() -> None:
    fs = new_fs()
    write_safely(fs, "/data/a", b"still here")

    fs.remove("/data/a")
    fs.crash()

    assert fs.read_file("/data/a") == b"still here"


def test_truncate_is_safe_right_away() -> None:
    fs = new_fs()
    write_safely(fs, "/data/a", b"0123456789")

    fs.truncate("/data/a", 4)
    fs.crash()

    assert fs.read_file("/data/a") == b"0123"


def test_old_handles_stop_working_after_a_crash() -> None:
    fs = new_fs()
    f = fs.open_append("/data/a")

    fs.crash()

    with pytest.raises(OSError):
        f.write(b"x")


def test_closed_file_cannot_be_written() -> None:
    fs = new_fs()
    f = fs.open_append("/data/a")
    f.close()

    with pytest.raises(OSError):
        f.write(b"x")


def test_paths_must_be_absolute() -> None:
    fs = new_fs()
    with pytest.raises(ValueError):
        fs.open_append("data/a")


def test_readers_from_before_a_crash_stop_working() -> None:
    fs = new_fs()
    write_safely(fs, "/data/a", b"data")
    r = fs.open_read("/data/a")

    fs.crash()

    with pytest.raises(OSError):
        r.read_at(0, 4)


def test_steps_count_every_change() -> None:
    fs = new_fs()
    start = fs.steps

    write_safely(fs, "/data/a", b"data")  # open, write, sync, sync_dir
    fs.read_file("/data/a")  # reads don't count
    fs.list_dir("/data")

    assert fs.steps == start + 4


def test_power_cut_stops_the_change_it_lands_on() -> None:
    fs = new_fs()
    f = fs.open_append("/data/a")
    f.write(b"synced")
    f.sync()
    fs.sync_dir("/data")

    fs.cut_power_after(1)
    f.write(b" kept for now")  # step 1 goes through
    with pytest.raises(PowerCut):
        f.sync()  # step 2 is the cut, so this sync never happens

    assert fs.crash_count == 1
    # The unsynced write may survive in part, but the synced part always does
    assert fs.read_file("/data/a").startswith(b"synced")
    with pytest.raises(OSError):
        f.write(b"x")  # the old handle died with the "process"


def test_disk_works_again_after_the_cut() -> None:
    fs = new_fs()
    fs.cut_power_after(0)
    with pytest.raises(PowerCut):
        fs.mkdir("/data/new")
    assert not fs.exists("/data/new")

    write_safely(fs, "/data/a", b"data")  # no second cut

    assert fs.read_file("/data/a") == b"data"


def test_cut_at_every_step_of_a_safe_write() -> None:
    # The whole point of the feature: walk a crash through every step of an
    # operation and check each outcome
    for n in range(4):
        fs = new_fs()
        fs.cut_power_after(n)
        with pytest.raises(PowerCut):
            write_safely(fs, "/data/a", b"data")
        # Until sync_dir (step 4) is done, the new name can be lost
        assert not fs.exists("/data/a")

    fs = new_fs()
    fs.cut_power_after(4)
    write_safely(fs, "/data/a", b"data")
    fs.crash()
    assert fs.read_file("/data/a") == b"data"


def test_keep_names_makes_unsynced_name_changes_stick() -> None:
    fs = new_fs()
    write_safely(fs, "/data/old", b"old")
    f = fs.open_append("/data/new")
    f.write(b"never synced")
    f.close()
    fs.remove("/data/old")

    fs.crash(keep_names=True)

    # The names changed as asked, but the unsynced bytes can still be torn
    assert fs.list_dir("/data") == ["new"]
    assert b"never synced".startswith(fs.read_file("/data/new"))


def test_power_cut_can_keep_names_too() -> None:
    fs = new_fs()
    write_safely(fs, "/data/a", b"data")

    fs.cut_power_after(1, keep_names=True)
    fs.remove("/data/a")
    with pytest.raises(PowerCut):
        fs.sync_dir("/data")

    assert not fs.exists("/data/a")


def test_planned_power_cut_can_be_cancelled() -> None:
    fs = FakeFileSystem()
    fs.cut_power_after(0)
    fs.cancel_power_cut()

    fs.mkdir("/a")

    assert fs.exists("/a")
    assert fs.crash_count == 0
