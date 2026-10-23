from pathlib import Path

import pytest

from faultline.fs.fake import FakeFileSystem
from faultline.fs.real import OSFileSystem
from faultline.wal import CorruptWal, open_wal, record
from faultline.wal.recovery import scan_segment

WAL = "/data/wal"


def new_fs() -> FakeFileSystem:
    fs = FakeFileSystem()
    fs.mkdir("/data")
    return fs


def test_scan_reads_everything_when_nothing_is_wrong() -> None:
    data = record.encode(b"a") + record.encode(b"b")

    scan = scan_segment(data)

    assert scan.records == [b"a", b"b"]
    assert scan.good_size == len(data)
    assert scan.error is None


def test_scan_stops_at_a_torn_record() -> None:
    good = record.encode(b"a")
    data = good + record.encode(b"half written")[:5]

    scan = scan_segment(data)

    assert scan.records == [b"a"]
    assert scan.good_size == len(good)
    assert isinstance(scan.error, record.TornRecord)


def test_new_wal_is_empty() -> None:
    fs = new_fs()

    records, wal = open_wal(fs, WAL)

    assert records == []
    assert wal.segment == 1


def test_reopen_after_clean_close() -> None:
    fs = new_fs()
    _, wal = open_wal(fs, WAL)
    for payload in [b"one", b"two", b"three"]:
        wal.append(payload)
    wal.sync()
    wal.close()

    records, _ = open_wal(fs, WAL)

    assert records == [b"one", b"two", b"three"]


def test_reopen_reads_every_segment_in_order() -> None:
    fs = new_fs()
    _, wal = open_wal(fs, WAL, max_segment_size=40)
    expected = [f"record {i}".encode() for i in range(30)]
    for payload in expected:
        wal.append(payload)
    wal.sync()
    wal.close()

    records, wal = open_wal(fs, WAL, max_segment_size=40)

    assert records == expected
    assert wal.segment > 1


def test_torn_tail_is_cut_off_and_writing_goes_on() -> None:
    fs = new_fs()
    _, wal = open_wal(fs, WAL)
    wal.append(b"safe")
    wal.sync()
    wal.close()
    # What a crash in the middle of a write leaves behind
    f = fs.open_append(f"{WAL}/00000001.wal")
    f.write(record.encode(b"never finished")[:6])
    f.close()

    records, wal = open_wal(fs, WAL)
    wal.append(b"after the crash")
    wal.sync()
    wal.close()

    assert records == [b"safe"]
    assert open_wal(fs, WAL)[0] == [b"safe", b"after the crash"]


def test_damage_in_an_old_segment_raises() -> None:
    fs = new_fs()
    _, wal = open_wal(fs, WAL, max_segment_size=20)
    for _ in range(5):
        wal.append(b"x" * 20)
    wal.sync()
    wal.close()
    old = fs.read_file(f"{WAL}/00000001.wal")
    fs.remove(f"{WAL}/00000001.wal")
    f = fs.open_append(f"{WAL}/00000001.wal")
    f.write(old[:-1] + b"?")  # change the last byte
    f.close()

    with pytest.raises(CorruptWal):
        open_wal(fs, WAL, max_segment_size=20)


def test_works_on_the_real_disk(tmp_path: Path) -> None:
    fs = OSFileSystem()
    directory = str(tmp_path / "wal")
    _, wal = open_wal(fs, directory)
    wal.append(b"on a real disk")
    wal.sync()
    wal.close()

    records, _ = open_wal(fs, directory)

    assert records == [b"on a real disk"]


def test_empty_folder_starts_at_the_given_segment() -> None:
    fs = new_fs()

    records, wal = open_wal(fs, WAL, first_segment=7)

    assert records == []
    assert wal.segment == 7
    assert fs.list_dir(WAL) == ["00000007.wal"]
