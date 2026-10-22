"""Reading the WAL back after a restart.

The rule recovery is built on comes from writer.py: an old segment is always
synced before the next one is started. So after a crash:

* The newest segment may end in a half-written record. That is expected. We
  cut it off and carry on.
* Any other segment must be perfect. A bad record there is real damage, not a
  crash, so we stop and raise instead of guessing.
"""

import os
from dataclasses import dataclass

from faultline.fs import FileSystem
from faultline.wal import record
from faultline.wal.segments import list_segments, segment_path
from faultline.wal.writer import DEFAULT_SEGMENT_SIZE, WalWriter


class CorruptWal(Exception):
    """A segment that is not the newest has a bad record."""


@dataclass
class SegmentScan:
    records: list[bytes]
    good_size: int  # bytes up to the end of the last good record
    error: record.RecordError | None  # why we stopped early, or None if we read it all


def scan_segment(data: bytes) -> SegmentScan:
    """Read records until the data ends or a record is bad."""
    records: list[bytes] = []
    offset = 0
    while offset < len(data):
        try:
            payload, next_offset = record.decode(data, offset)
        except record.RecordError as err:
            return SegmentScan(records, offset, err)
        records.append(payload)
        offset = next_offset
    return SegmentScan(records, offset, None)


def open_wal(
    fs: FileSystem,
    directory: str,
    max_segment_size: int = DEFAULT_SEGMENT_SIZE,
) -> tuple[list[bytes], WalWriter]:
    """Open the WAL in `directory`, creating it if needed.

    Returns every record that survived, oldest first, and a writer that
    appends after them.
    """
    if not fs.exists(directory):
        fs.mkdir(directory)
        fs.sync_dir(os.path.dirname(os.path.normpath(directory)) or ".")

    segments = list_segments(fs, directory)
    if not segments:
        return [], WalWriter(fs, directory, 1, max_segment_size)

    records: list[bytes] = []
    for number in segments:
        path = segment_path(directory, number)
        scan = scan_segment(fs.read_file(path))
        records += scan.records
        if scan.error is None:
            continue
        if number != segments[-1]:
            raise CorruptWal(f"{path}: {scan.error}") from scan.error
        # A torn tail in the newest segment. Cut it off, so the next record we
        # write starts on a clean boundary instead of after the garbage.
        fs.truncate(path, scan.good_size)

    return records, WalWriter(fs, directory, segments[-1], max_segment_size)
