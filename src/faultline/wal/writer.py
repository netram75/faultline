"""Appending records to the WAL."""

from faultline.fs import FileSystem, WritableFile
from faultline.wal import record
from faultline.wal.segments import segment_path

DEFAULT_SEGMENT_SIZE = 16 * 1024 * 1024


class WalWriter:
    """Adds records to the end of the log.

    append() only hands the bytes to the file. They are not safe until sync().
    Keeping the two apart lets a caller write a batch of records and pay for
    one sync instead of one per record. That trick is called group commit,
    and it matters a lot because a sync costs milliseconds.
    """

    def __init__(
        self,
        fs: FileSystem,
        directory: str,
        segment: int = 1,
        max_segment_size: int = DEFAULT_SEGMENT_SIZE,
    ) -> None:
        self._fs = fs
        self._directory = directory
        self._max_segment_size = max_segment_size
        self._segment = segment
        self._file = self._open_segment(segment)

    @property
    def segment(self) -> int:
        """Number of the segment new records go to."""
        return self._segment

    def append(self, payload: bytes) -> None:
        if self._file.size >= self._max_segment_size:
            self.start_next_segment()
        self._file.write(record.encode(payload))

    def sync(self) -> None:
        """Wait until every appended record is safe on disk."""
        self._file.sync()

    def close(self) -> None:
        self._file.close()

    def start_next_segment(self) -> None:
        """Send new records to a fresh segment, even if this one is not full.

        The storage engine calls this when it flushes the memtable, so the old
        segments hold exactly the writes in that memtable and can be deleted
        once it is safe on disk.
        """
        # Sync the old segment before the new one exists. That way only the
        # newest segment can ever end in a half-written record, and recovery
        # can treat a bad record anywhere else as real damage.
        self._file.sync()
        self._file.close()
        self._segment += 1
        self._file = self._open_segment(self._segment)

    def _open_segment(self, number: int) -> WritableFile:
        path = segment_path(self._directory, number)
        is_new = not self._fs.exists(path)
        f = self._fs.open_append(path)
        if is_new:
            # Without this a crash can wipe the file's name, and every record
            # in it with it, even if the records themselves were synced.
            self._fs.sync_dir(self._directory)
        return f
