"""The real disk, using plain os calls."""

import fcntl
import os

# Only exists on macOS. Looked up with getattr so type checks pass on Linux too.
_F_FULLFSYNC: int | None = getattr(fcntl, "F_FULLFSYNC", None)


def _sync_fd(fd: int) -> None:
    # On macOS a plain fsync() only hands the data to the drive, and the drive
    # can still lose it from its cache on power loss. F_FULLFSYNC makes the
    # drive flush that cache too. It is slower, but it is what "synced" means.
    if _F_FULLFSYNC is not None:
        try:
            fcntl.fcntl(fd, _F_FULLFSYNC)
            return
        except OSError:
            pass  # some file systems don't support it, plain fsync is the best we can do
    os.fsync(fd)


class OSFile:
    def __init__(self, path: str) -> None:
        self._fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        self._size = os.fstat(self._fd).st_size

    @property
    def size(self) -> int:
        return self._size

    def write(self, data: bytes) -> None:
        # os.write can write less than we asked for, so keep going until it's all out
        view = memoryview(data)
        while view:
            written = os.write(self._fd, view)
            view = view[written:]
        self._size += len(data)

    def sync(self) -> None:
        _sync_fd(self._fd)

    def close(self) -> None:
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1


class OSReader:
    def __init__(self, path: str) -> None:
        self._fd = os.open(path, os.O_RDONLY)
        self._size = os.fstat(self._fd).st_size

    @property
    def size(self) -> int:
        return self._size

    def read_at(self, offset: int, length: int) -> bytes:
        # pread reads at an offset without moving a shared file position, so
        # two readers on the same file can't get in each other's way. Like
        # write, it can return less than asked for, so loop.
        chunks = []
        while length > 0:
            chunk = os.pread(self._fd, length, offset)
            if not chunk:
                break  # end of file
            chunks.append(chunk)
            offset += len(chunk)
            length -= len(chunk)
        return b"".join(chunks)

    def close(self) -> None:
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1


class OSFileSystem:
    def open_append(self, path: str) -> OSFile:
        return OSFile(path)

    def open_read(self, path: str) -> OSReader:
        return OSReader(path)

    def read_file(self, path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    def truncate(self, path: str, size: int) -> None:
        fd = os.open(path, os.O_WRONLY)
        try:
            os.ftruncate(fd, size)
            _sync_fd(fd)
        finally:
            os.close(fd)

    def rename(self, src: str, dst: str) -> None:
        os.replace(src, dst)

    def remove(self, path: str) -> None:
        os.remove(path)

    def list_dir(self, path: str) -> list[str]:
        return sorted(os.listdir(path))

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def mkdir(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)

    def sync_dir(self, path: str) -> None:
        fd = os.open(path, os.O_RDONLY)
        try:
            _sync_fd(fd)
        finally:
            os.close(fd)
