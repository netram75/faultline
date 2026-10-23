import pytest

from faultline.fs.fake import FakeFileSystem, PowerCut
from faultline.lsm.manifest import CorruptManifest, Manifest, TableInfo, load, save

ROOT = "/db"


def sample() -> Manifest:
    return Manifest(
        next_file=9,
        wal_start=4,
        tables=(
            TableInfo(number=7, level=0, smallest=b"a", largest=b"m", size=4096),
            TableInfo(number=8, level=0, smallest=b"", largest=b"\xff" * 20, size=1),
            TableInfo(number=3, level=1, smallest=b"b", largest=b"c", size=123456789),
        ),
    )


def new_fs() -> FakeFileSystem:
    fs = FakeFileSystem()
    fs.mkdir(ROOT)
    return fs


def test_round_trip() -> None:
    assert Manifest.decode(sample().encode()) == sample()
    assert Manifest.decode(Manifest().encode()) == Manifest()


def test_load_with_nothing_saved() -> None:
    assert load(new_fs(), ROOT) is None


def test_save_then_load() -> None:
    fs = new_fs()
    save(fs, ROOT, Manifest())
    save(fs, ROOT, sample())

    assert load(fs, ROOT) == sample()
    assert fs.list_dir(ROOT) == ["MANIFEST"]


def test_saved_manifest_survives_a_crash() -> None:
    fs = new_fs()
    save(fs, ROOT, sample())

    fs.crash()

    assert load(fs, ROOT) == sample()


def test_leftover_temp_file_is_replaced() -> None:
    # What a crash in the middle of an earlier save leaves behind
    fs = new_fs()
    f = fs.open_append(f"{ROOT}/MANIFEST.tmp")
    f.write(b"half a manifest")
    f.close()

    save(fs, ROOT, sample())

    assert load(fs, ROOT) == sample()
    assert fs.list_dir(ROOT) == ["MANIFEST"]


def test_every_flipped_bit_is_caught() -> None:
    data = sample().encode()
    for i in range(len(data) * 8):
        damaged = bytearray(data)
        damaged[i // 8] ^= 1 << (i % 8)
        with pytest.raises(CorruptManifest):
            Manifest.decode(bytes(damaged))


def test_every_cut_is_caught() -> None:
    data = sample().encode()
    for cut in range(len(data)):
        with pytest.raises(CorruptManifest):
            Manifest.decode(data[:cut])


def test_power_cut_at_any_step_of_a_save_keeps_old_or_new() -> None:
    old = Manifest(next_file=2, wal_start=1)
    new = sample()
    saw_new = False
    for n in range(20):
        fs = new_fs()
        save(fs, ROOT, old)
        fs.cut_power_after(n)
        try:
            save(fs, ROOT, new)
        except PowerCut:
            assert load(fs, ROOT) in (old, new)
            continue
        # The save finished before the cut came up
        fs.crash()
        assert load(fs, ROOT) == new
        saw_new = True
        break
    assert saw_new
