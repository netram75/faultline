from faultline.fs.fake import FakeFileSystem
from faultline.wal import record
from faultline.wal.segments import list_segments, parse_segment_name, segment_name
from faultline.wal.writer import WalWriter

WAL = "/data/wal"


def new_fs() -> FakeFileSystem:
    fs = FakeFileSystem()
    fs.mkdir(WAL)
    return fs


def read_records(data: bytes) -> list[bytes]:
    found = []
    offset = 0
    while offset < len(data):
        payload, offset = record.decode(data, offset)
        found.append(payload)
    return found


def test_segment_names() -> None:
    assert segment_name(1) == "00000001.wal"
    assert parse_segment_name("00000042.wal") == 42
    assert parse_segment_name("00000042.txt") is None
    assert parse_segment_name("42.wal") is None
    assert parse_segment_name("0000000x.wal") is None


def test_list_segments_skips_other_files() -> None:
    fs = new_fs()
    for name in ["00000002.wal", "notes.txt", "00000001.wal"]:
        fs.open_append(f"{WAL}/{name}").close()

    assert list_segments(fs, WAL) == [1, 2]


def test_appended_records_end_up_in_the_segment() -> None:
    fs = new_fs()
    wal = WalWriter(fs, WAL)

    wal.append(b"one")
    wal.append(b"two")
    wal.sync()
    wal.close()

    assert read_records(fs.read_file(f"{WAL}/00000001.wal")) == [b"one", b"two"]


def test_full_segment_rolls_over_to_the_next() -> None:
    fs = new_fs()
    wal = WalWriter(fs, WAL, max_segment_size=50)

    for i in range(20):
        wal.append(f"record {i}".encode())
    wal.sync()
    wal.close()

    segments = list_segments(fs, WAL)
    assert len(segments) > 1
    assert wal.segment == segments[-1]

    found = []
    for number in segments:
        found += read_records(fs.read_file(f"{WAL}/{segment_name(number)}"))
    assert found == [f"record {i}".encode() for i in range(20)]


def test_synced_records_survive_a_crash() -> None:
    fs = new_fs()
    wal = WalWriter(fs, WAL)
    wal.append(b"keep me")
    wal.sync()

    fs.crash()

    assert read_records(fs.read_file(f"{WAL}/00000001.wal")) == [b"keep me"]


def test_old_segments_are_synced_before_rolling_over() -> None:
    fs = new_fs()
    wal = WalWriter(fs, WAL, max_segment_size=20)
    wal.append(b"a" * 20)  # fills segment 1
    wal.append(b"b")  # goes to segment 2, which syncs segment 1 first

    fs.crash()

    assert read_records(fs.read_file(f"{WAL}/00000001.wal")) == [b"a" * 20]
