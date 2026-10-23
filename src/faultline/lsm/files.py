"""Names of the table files in an engine's folder.

Tables are numbered from one counter that only goes up (next_file in the
manifest), so a number is never used twice, even after a crash:

    00000007.sst
    00000012.sst
"""

import os

from faultline.fs import FileSystem

_SUFFIX = ".sst"
_DIGITS = 8


def table_path(root: str, number: int) -> str:
    return os.path.join(root, f"{number:0{_DIGITS}d}{_SUFFIX}")


def parse_table_name(name: str) -> int | None:
    """The table number in a file name, or None if it is not a table."""
    stem = name.removesuffix(_SUFFIX)
    if stem == name or len(stem) != _DIGITS or not (stem.isascii() and stem.isdigit()):
        return None
    return int(stem)


def list_tables(fs: FileSystem, root: str) -> list[int]:
    """Numbers of every table file in the folder, live or not."""
    numbers = [parse_table_name(name) for name in fs.list_dir(root)]
    return sorted(n for n in numbers if n is not None)
