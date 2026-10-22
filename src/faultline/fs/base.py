"""The file operations the rest of faultline is allowed to use.

Storage code never calls os or open() directly. It goes through a FileSystem.
On a real server that is the real disk. In tests it can be a fake disk that
lets us pull the plug at any moment and look at what survived.

Two rules every caller has to follow, because real disks work this way:

1. Data you write is not safe until you call sync() on the file.
2. A new, renamed or deleted file is not safe until you call sync_dir() on the
   folder it lives in. Syncing the file alone does not save its name.
"""

from typing import Protocol


class WritableFile(Protocol):
    """A file opened for appending."""

    @property
    def size(self) -> int:
        """Current length of the file in bytes, including unsynced writes."""
        ...

    def write(self, data: bytes) -> None:
        """Add data to the end of the file."""

    def sync(self) -> None:
        """Wait until everything written so far is on stable storage."""

    def close(self) -> None:
        """Close the file. This does not sync it."""


class ReadableFile(Protocol):
    """A file opened for reading any part of it, like an SSTable."""

    @property
    def size(self) -> int:
        """Length of the file in bytes."""
        ...

    def read_at(self, offset: int, length: int) -> bytes:
        """Read `length` bytes from `offset`. Returns fewer only at the end of the file."""
        ...

    def close(self) -> None: ...


class FileSystem(Protocol):
    def open_append(self, path: str) -> WritableFile:
        """Open a file for appending, creating it if it does not exist.

        The folder must already exist.
        """
        ...

    def open_read(self, path: str) -> ReadableFile:
        """Open an existing file for reading. Raises FileNotFoundError if it is missing."""
        ...

    def read_file(self, path: str) -> bytes:
        """Read a whole file. Raises FileNotFoundError if it is missing."""
        ...

    def truncate(self, path: str, size: int) -> None:
        """Cut a file down to `size` bytes and sync it."""

    def rename(self, src: str, dst: str) -> None:
        """Move a file, replacing `dst` if it exists. Atomic on a real disk."""

    def remove(self, path: str) -> None:
        """Delete a file."""

    def list_dir(self, path: str) -> list[str]:
        """Names in a folder, sorted."""
        ...

    def exists(self, path: str) -> bool:
        """True if a file or folder exists at `path`."""
        ...

    def mkdir(self, path: str) -> None:
        """Create a folder and any missing parents. Fine if it already exists."""

    def sync_dir(self, path: str) -> None:
        """Make the folder's list of names safe on disk (creates, renames, deletes)."""
