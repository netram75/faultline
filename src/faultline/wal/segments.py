"""WAL segment files and their names.

The log is split into numbered files called segments:

    wal/00000001.wal
    wal/00000002.wal
    wal/00000003.wal   <- new records go here

When the newest one gets too big, we start the next. Later, once the data in
an old segment is safely in the main storage files, the whole segment can be
deleted in one go instead of rewriting a single huge log.
"""

import os

from faultline.fs import FileSystem

_SUFFIX = ".wal"
_DIGITS = 8


def segment_name(number: int) -> str:
    return f"{number:0{_DIGITS}d}{_SUFFIX}"


def segment_path(directory: str, number: int) -> str:
    return os.path.join(directory, segment_name(number))


def parse_segment_name(name: str) -> int | None:
    """The segment number in a file name, or None if it is not a segment."""
    stem = name.removesuffix(_SUFFIX)
    if stem == name or len(stem) != _DIGITS:
        return None
    # isascii() stops things like "0000000²" from passing isdigit()
    if not (stem.isascii() and stem.isdigit()):
        return None
    return int(stem)


def list_segments(fs: FileSystem, directory: str) -> list[int]:
    """Segment numbers in a folder, oldest first. Other files are ignored."""
    numbers = []
    for name in fs.list_dir(directory):
        number = parse_segment_name(name)
        if number is not None:
            numbers.append(number)
    return sorted(numbers)
