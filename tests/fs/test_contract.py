"""Behaviour every FileSystem must have.

These run against each implementation. If the fake disk ever behaves
differently from the real one, tests built on the fake would be lying to us,
so this file is what keeps it honest.
"""

import os
from pathlib import Path

import pytest

from faultline.fs import FileSystem
from faultline.fs.real import OSFileSystem


@pytest.fixture
def setup(tmp_path: Path) -> tuple[FileSystem, str]:
    """A file system and an empty folder to work in."""
    return OSFileSystem(), str(tmp_path)


def test_write_then_read(setup: tuple[FileSystem, str]) -> None:
    fs, root = setup
    path = os.path.join(root, "a.txt")

    f = fs.open_append(path)
    f.write(b"hello ")
    f.write(b"world")
    f.close()

    assert fs.read_file(path) == b"hello world"


def test_size_counts_every_write(setup: tuple[FileSystem, str]) -> None:
    fs, root = setup
    f = fs.open_append(os.path.join(root, "a.txt"))
    assert f.size == 0

    f.write(b"abc")
    f.write(b"de")
    assert f.size == 5
    f.close()


def test_open_append_keeps_old_data(setup: tuple[FileSystem, str]) -> None:
    fs, root = setup
    path = os.path.join(root, "a.txt")

    f = fs.open_append(path)
    f.write(b"one")
    f.close()

    f = fs.open_append(path)
    assert f.size == 3
    f.write(b"two")
    f.close()

    assert fs.read_file(path) == b"onetwo"


def test_truncate(setup: tuple[FileSystem, str]) -> None:
    fs, root = setup
    path = os.path.join(root, "a.txt")
    f = fs.open_append(path)
    f.write(b"0123456789")
    f.close()

    fs.truncate(path, 4)

    assert fs.read_file(path) == b"0123"


def test_rename_replaces_target(setup: tuple[FileSystem, str]) -> None:
    fs, root = setup
    src = os.path.join(root, "new")
    dst = os.path.join(root, "current")
    for path, data in [(src, b"new"), (dst, b"old")]:
        f = fs.open_append(path)
        f.write(data)
        f.close()

    fs.rename(src, dst)

    assert fs.read_file(dst) == b"new"
    assert not fs.exists(src)


def test_remove(setup: tuple[FileSystem, str]) -> None:
    fs, root = setup
    path = os.path.join(root, "a.txt")
    fs.open_append(path).close()
    assert fs.exists(path)

    fs.remove(path)

    assert not fs.exists(path)


def test_list_dir_is_sorted(setup: tuple[FileSystem, str]) -> None:
    fs, root = setup
    for name in ["c", "a", "b"]:
        fs.open_append(os.path.join(root, name)).close()

    assert fs.list_dir(root) == ["a", "b", "c"]


def test_mkdir_makes_parents_and_is_repeatable(setup: tuple[FileSystem, str]) -> None:
    fs, root = setup
    path = os.path.join(root, "x", "y", "z")

    fs.mkdir(path)
    fs.mkdir(path)

    assert fs.exists(path)
    assert fs.list_dir(os.path.join(root, "x")) == ["y"]


def test_sync_and_sync_dir_do_not_fail(setup: tuple[FileSystem, str]) -> None:
    fs, root = setup
    f = fs.open_append(os.path.join(root, "a.txt"))
    f.write(b"data")
    f.sync()
    f.close()

    fs.sync_dir(root)


def test_read_missing_file(setup: tuple[FileSystem, str]) -> None:
    fs, root = setup
    with pytest.raises(FileNotFoundError):
        fs.read_file(os.path.join(root, "nope"))


def test_open_in_missing_folder(setup: tuple[FileSystem, str]) -> None:
    fs, root = setup
    with pytest.raises(FileNotFoundError):
        fs.open_append(os.path.join(root, "no-such-dir", "a.txt"))
