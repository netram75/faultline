import pytest

from faultline.fs import replace_file
from faultline.fs.fake import FakeFileSystem, PowerCut

PATH = "/data/STATE"


def new_fs() -> FakeFileSystem:
    fs = FakeFileSystem()
    fs.mkdir("/data")
    return fs


def test_creates_the_file() -> None:
    fs = new_fs()

    replace_file(fs, PATH, b"one")

    assert fs.read_file(PATH) == b"one"
    assert fs.list_dir("/data") == ["STATE"]


def test_replaces_the_whole_file() -> None:
    fs = new_fs()
    replace_file(fs, PATH, b"a long first version")

    replace_file(fs, PATH, b"short")

    assert fs.read_file(PATH) == b"short"


def test_survives_a_crash_once_it_returns() -> None:
    fs = new_fs()
    replace_file(fs, PATH, b"one")
    replace_file(fs, PATH, b"two")

    fs.crash()

    assert fs.read_file(PATH) == b"two"


def test_leftover_temp_file_is_dropped() -> None:
    fs = new_fs()
    f = fs.open_append(PATH + ".tmp")
    f.write(b"half written")
    f.close()

    replace_file(fs, PATH, b"new")

    assert fs.read_file(PATH) == b"new"
    assert fs.list_dir("/data") == ["STATE"]


@pytest.mark.parametrize("keep_names", [False, True])
def test_power_cut_at_any_step_keeps_old_or_new(keep_names: bool) -> None:
    finished = False
    for steps in range(20):
        fs = new_fs()
        replace_file(fs, PATH, b"old")
        fs.cut_power_after(steps, keep_names)
        try:
            replace_file(fs, PATH, b"new")
        except PowerCut:
            assert fs.read_file(PATH) in (b"old", b"new")
            continue
        finished = True
        break
    assert finished
