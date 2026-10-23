import struct

import pytest

from faultline.fs.fake import FakeFileSystem, PowerCut
from faultline.raft.log import CorruptLog, RaftLog
from faultline.raft.messages import Entry


def log_with_terms(*terms: int) -> RaftLog:
    log = RaftLog()
    log.append([Entry(term, f"cmd{i}".encode()) for i, term in enumerate(terms, start=1)])
    return log


def test_empty_log() -> None:
    log = RaftLog()

    assert log.last_index == 0
    assert log.last_term == 0
    assert log.term_at(0) == 0
    assert log.entries_from(1) == ()


def test_append_and_read() -> None:
    log = log_with_terms(1, 1, 2)

    assert log.last_index == 3
    assert log.last_term == 2
    assert log.entry(1) == Entry(1, b"cmd1")
    assert log.term_at(3) == 2
    assert log.entries_from(2) == (Entry(1, b"cmd2"), Entry(2, b"cmd3"))
    assert log.entries_from(1, max_count=2) == (Entry(1, b"cmd1"), Entry(1, b"cmd2"))
    assert log.entries_from(4) == ()


def test_out_of_range() -> None:
    log = log_with_terms(1)

    with pytest.raises(IndexError):
        log.entry(0)
    with pytest.raises(IndexError):
        log.entry(2)
    with pytest.raises(IndexError):
        log.entries_from(3)
    with pytest.raises(IndexError):
        log.truncate_from(0)


def test_matches() -> None:
    log = log_with_terms(1, 1, 2)

    assert log.matches(0, 0)
    assert log.matches(3, 2)
    assert not log.matches(3, 1)
    assert not log.matches(4, 2)


def test_truncate_from() -> None:
    log = log_with_terms(1, 1, 2, 3)

    log.truncate_from(3)

    assert log.last_index == 2
    assert log.last_term == 1
    log.truncate_from(3)  # cutting at the end is fine and does nothing
    assert log.last_index == 2


@pytest.mark.parametrize(
    ("other_index", "other_term", "expected"),
    [
        (3, 2, True),  # same log end
        (4, 2, True),  # same last term, longer
        (2, 2, False),  # same last term, shorter
        (1, 3, True),  # later last term wins even when much shorter
        (9, 1, False),  # earlier last term loses even when much longer
    ],
)
def test_is_up_to_date(other_index: int, other_term: int, expected: bool) -> None:
    log = log_with_terms(1, 1, 2)

    assert log.is_up_to_date(other_index, other_term) == expected


DIR = "/node1/log"


def disk_log(fs: FakeFileSystem) -> RaftLog:
    return RaftLog.open(fs, DIR)


def new_fs() -> FakeFileSystem:
    fs = FakeFileSystem()
    fs.mkdir("/node1")
    return fs


def test_log_on_disk_comes_back_after_a_crash() -> None:
    fs = new_fs()
    log = disk_log(fs)
    log.append([Entry(1, b"a"), Entry(1, b"b")])
    log.append([Entry(2, b"")])

    fs.crash()
    reopened = disk_log(fs)

    assert reopened.entries_from(1) == (Entry(1, b"a"), Entry(1, b"b"), Entry(2, b""))


def test_cut_and_append_on_disk() -> None:
    fs = new_fs()
    log = disk_log(fs)
    log.append([Entry(1, b"a"), Entry(1, b"b"), Entry(1, b"c")])
    log.truncate_from(2)
    log.append([Entry(3, b"x")])

    fs.crash()
    reopened = disk_log(fs)

    assert reopened.entries_from(1) == (Entry(1, b"a"), Entry(3, b"x"))


def test_cutting_at_the_end_writes_nothing() -> None:
    fs = new_fs()
    log = disk_log(fs)
    log.append([Entry(1, b"a")])
    steps = fs.steps

    log.truncate_from(2)
    log.append([])

    assert fs.steps == steps


@pytest.mark.parametrize("keep_names", [False, True])
def test_power_cut_during_append_keeps_a_prefix(keep_names: bool) -> None:
    # Entries are written one after another and synced once at the end.
    # Whatever survives must be the old log plus some of the new entries, in
    # order, never a gap and never a damaged entry.
    old = [Entry(1, b"old")]
    new = [Entry(2, f"new{i}".encode()) for i in range(5)]
    # The append takes one write per entry and then one sync. Cut at each.
    for steps in range(len(new) + 1):
        fs = new_fs()
        log = disk_log(fs)
        log.append(old)
        fs.cut_power_after(steps, keep_names)
        with pytest.raises(PowerCut):
            log.append(new)
        survived = disk_log(fs).entries_from(1)
        assert survived == tuple(old + new)[: len(survived)]
        assert len(survived) >= len(old)


def test_record_out_of_order_is_caught() -> None:
    fs = new_fs()
    wal = disk_log(fs)._wal
    assert wal is not None
    wal.append(struct.pack("<BQQ", 1, 5, 1) + b"x")
    wal.sync()

    with pytest.raises(CorruptLog, match="entry 5 comes after entry 0"):
        disk_log(fs)


def test_unknown_record_is_caught() -> None:
    fs = new_fs()
    wal = disk_log(fs)._wal
    assert wal is not None
    wal.append(b"\x09junk")
    wal.sync()

    with pytest.raises(CorruptLog, match="bad log record"):
        disk_log(fs)
