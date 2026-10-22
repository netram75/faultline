import pytest

from faultline.entry import TOMBSTONE
from faultline.fs.fake import FakeFileSystem
from faultline.sstable.block import decode_block
from faultline.sstable.format import BLOCK_HANDLE, FOOTER_SIZE, MAGIC, Footer
from faultline.sstable.writer import TableWriter

PATH = "/data/00000001.sst"


def new_fs() -> FakeFileSystem:
    fs = FakeFileSystem()
    fs.mkdir("/data")
    return fs


def test_file_ends_with_a_valid_footer() -> None:
    fs = new_fs()
    writer = TableWriter(fs, PATH)
    writer.add(b"a", b"1")
    writer.add(b"b", TOMBSTONE)
    size = writer.finish()

    data = fs.read_file(PATH)
    assert len(data) == size
    assert data.endswith(MAGIC)
    footer = Footer.decode(data[-FOOTER_SIZE:])
    assert footer.entry_count == 2
    assert footer.index_offset + footer.index_size == size - FOOTER_SIZE


def test_big_input_is_split_into_many_blocks() -> None:
    fs = new_fs()
    writer = TableWriter(fs, PATH, block_size=256)
    for i in range(500):
        writer.add(f"key{i:05d}".encode(), b"v" * 20)
    writer.finish()

    data = fs.read_file(PATH)
    footer = Footer.decode(data[-FOOTER_SIZE:])
    index = decode_block(data[footer.index_offset : footer.index_offset + footer.index_size])
    assert len(index) > 10

    # Each index entry points at a block whose last key is the index key
    for last_key, handle in index:
        assert isinstance(handle, bytes)
        offset, length = BLOCK_HANDLE.unpack(handle)
        block = decode_block(data[offset : offset + length])
        assert block[-1][0] == last_key


def test_keys_out_of_order_are_refused() -> None:
    writer = TableWriter(new_fs(), PATH)
    writer.add(b"b", b"1")

    with pytest.raises(ValueError):
        writer.add(b"a", b"2")


def test_tables_are_never_overwritten() -> None:
    fs = new_fs()
    TableWriter(fs, PATH).finish()

    with pytest.raises(FileExistsError):
        TableWriter(fs, PATH)


def test_finished_table_survives_a_crash() -> None:
    fs = new_fs()
    writer = TableWriter(fs, PATH)
    writer.add(b"k", b"v")
    size = writer.finish()

    fs.crash()

    assert len(fs.read_file(PATH)) == size


def test_footer_round_trip() -> None:
    footer = Footer(10, 20, 30, 40, 50)

    assert Footer.decode(footer.encode()) == footer
