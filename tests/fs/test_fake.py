"""Crash behaviour of the fake disk. The normal behaviour is in test_contract.py."""

import pytest

from faultline.fs.fake import FakeFileSystem


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
