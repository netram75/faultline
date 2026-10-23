"""Replacing a whole file so that a crash leaves the old copy or the new one.

Writing over a file in place is not safe: a crash halfway through leaves a
mix of old and new bytes. Instead we write the new copy next to it, sync it,
and rename it over the old one. A rename is atomic, so whoever reads the file
later sees one copy or the other, never half of each. Then we sync the
folder, because the rename itself only survives a crash once the folder is
synced.

The storage engine's manifest and Raft's saved term and vote both work this
way. They are small files that always change as a whole.
"""

import os

from faultline.fs.base import FileSystem


def replace_file(fs: FileSystem, path: str, data: bytes) -> None:
    """Make `data` the content of `path`. When this returns, it is durable."""
    temp = path + ".tmp"
    # Left over from a crash in the middle of an earlier replace. Safe to
    # drop, since it never got renamed into place.
    if fs.exists(temp):
        fs.remove(temp)
    f = fs.open_append(temp)
    f.write(data)
    f.sync()
    f.close()
    fs.rename(temp, path)
    fs.sync_dir(os.path.dirname(path) or ".")
