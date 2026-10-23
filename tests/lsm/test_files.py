from faultline.fs.fake import FakeFileSystem
from faultline.lsm.files import list_tables, parse_table_name, table_path


def test_table_names() -> None:
    assert table_path("/db", 7) == "/db/00000007.sst"
    assert parse_table_name("00000007.sst") == 7
    assert parse_table_name("00000007.wal") is None
    assert parse_table_name("7.sst") is None
    assert parse_table_name("0000000x.sst") is None
    assert parse_table_name("MANIFEST") is None


def test_list_tables_skips_other_files() -> None:
    fs = FakeFileSystem()
    fs.mkdir("/db/wal")
    for name in ["00000003.sst", "MANIFEST", "00000001.sst", "notes.txt"]:
        fs.open_append(f"/db/{name}").close()

    assert list_tables(fs, "/db") == [1, 3]
