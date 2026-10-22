"""A disk that lives in memory and can crash.

It keeps two views of every file and folder:

* what the program sees right now, with every write, rename and delete so far
* what is actually safe on disk, which is only what was synced

crash() throws the first view away and keeps the second, like a power cut.
Tests use this to answer the one question that matters for storage code:
after a crash, is everything we promised to keep still there?

What a crash does to things that were not synced:

* Unsynced bytes at the end of a file can be lost, kept, or kept in part.
  "In part" means a random prefix survives. That is a torn write.
* A name that was created, renamed or deleted without sync_dir() goes back to
  how it was at the last sync_dir() of its folder.

Things kept simple on purpose, for now:

* Folders never disappear in a crash.
* A torn write always keeps a prefix. Real disks can also write sectors out of
  order or leave zeros behind.

Paths must be absolute, like /data/wal/00000001.wal.
"""

import posixpath
import random


class _Inode:
    """The bytes of one file, no matter which name points at it."""

    def __init__(self) -> None:
        self.data = bytearray()  # what reads see right now
        self.synced = b""  # what is safe on disk

    def sync(self) -> None:
        self.synced = bytes(self.data)

    def crash(self, rng: random.Random) -> None:
        # data always starts with synced: the only way to shrink a file is
        # truncate(), and that syncs.
        unsynced = bytes(self.data[len(self.synced) :])
        keep = rng.randint(0, len(unsynced))
        self.data = bytearray(self.synced + unsynced[:keep])
        self.synced = bytes(self.data)


class FakeFileSystem:
    def __init__(self, seed: int = 0) -> None:
        self.crash_count = 0
        self._rng = random.Random(seed)
        self._dirs = {"/"}
        self._names: dict[str, _Inode] = {}  # what the program sees
        self._durable_names: dict[str, _Inode] = {}  # what survives a crash

    def crash(self) -> None:
        """Pull the plug. Anything not synced may be lost or torn."""
        self.crash_count += 1
        self._names = dict(self._durable_names)

        # Go through files in sorted order so the same seed always tears the
        # same files the same way. Two names can share one inode after a
        # half-synced rename, so only crash each inode once.
        seen: set[int] = set()
        for name in sorted(self._names):
            inode = self._names[name]
            if id(inode) not in seen:
                seen.add(id(inode))
                inode.crash(self._rng)

    def open_append(self, path: str) -> "FakeFile":
        path = _clean(path)
        self._need_dir(posixpath.dirname(path))
        if path in self._dirs:
            raise IsADirectoryError(path)
        inode = self._names.get(path)
        if inode is None:
            inode = _Inode()
            self._names[path] = inode
        return FakeFile(self, inode)

    def open_read(self, path: str) -> "FakeReader":
        return FakeReader(self, self._inode(path))

    def read_file(self, path: str) -> bytes:
        return bytes(self._inode(path).data)

    def truncate(self, path: str, size: int) -> None:
        inode = self._inode(path)
        del inode.data[size:]
        inode.sync()

    def rename(self, src: str, dst: str) -> None:
        src, dst = _clean(src), _clean(dst)
        inode = self._inode(src)
        self._need_dir(posixpath.dirname(dst))
        del self._names[src]
        self._names[dst] = inode

    def remove(self, path: str) -> None:
        path = _clean(path)
        self._inode(path)
        del self._names[path]

    def list_dir(self, path: str) -> list[str]:
        path = _clean(path)
        self._need_dir(path)
        children = {
            posixpath.basename(p)
            for p in [*self._names, *self._dirs]
            if p != "/" and posixpath.dirname(p) == path
        }
        return sorted(children)

    def exists(self, path: str) -> bool:
        path = _clean(path)
        return path in self._names or path in self._dirs

    def mkdir(self, path: str) -> None:
        path = _clean(path)
        while path not in self._dirs:
            if path in self._names:
                raise FileExistsError(path)
            self._dirs.add(path)
            path = posixpath.dirname(path)

    def sync_dir(self, path: str) -> None:
        path = _clean(path)
        self._need_dir(path)
        for name in [*self._names, *self._durable_names]:
            if posixpath.dirname(name) != path:
                continue
            if name in self._names:
                self._durable_names[name] = self._names[name]
            else:
                self._durable_names.pop(name, None)

    def _inode(self, path: str) -> _Inode:
        inode = self._names.get(_clean(path))
        if inode is None:
            raise FileNotFoundError(path)
        return inode

    def _need_dir(self, path: str) -> None:
        if path not in self._dirs:
            raise FileNotFoundError(path)


class _Handle:
    """What open files for writing and for reading have in common."""

    def __init__(self, fs: FakeFileSystem, inode: _Inode) -> None:
        self._fs = fs
        self._inode = inode
        self._opened_at_crash = fs.crash_count
        self._closed = False

    @property
    def size(self) -> int:
        return len(self._inode.data)

    def close(self) -> None:
        self._closed = True

    def _check_usable(self) -> None:
        if self._closed:
            raise OSError("file is closed")
        # After a crash the process is "dead", so its old handles must be too.
        # This catches tests that forget to reopen files after crash().
        if self._opened_at_crash != self._fs.crash_count:
            raise OSError("file was opened before a crash")


class FakeFile(_Handle):
    def write(self, data: bytes) -> None:
        self._check_usable()
        self._inode.data += data

    def sync(self) -> None:
        self._check_usable()
        self._inode.sync()


class FakeReader(_Handle):
    def read_at(self, offset: int, length: int) -> bytes:
        self._check_usable()
        return bytes(self._inode.data[offset : offset + length])


def _clean(path: str) -> str:
    if not path.startswith("/"):
        raise ValueError(f"fake disk paths must start with /, got {path!r}")
    return posixpath.normpath(path)
