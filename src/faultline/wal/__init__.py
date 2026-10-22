"""The write-ahead log (WAL).

Every change is written here first, before anything else happens to it. If
the process dies, the log is read back on restart and nothing that was
synced is lost. See docs/wal.md for the full story.
"""

from faultline.wal.recovery import CorruptWal, open_wal
from faultline.wal.writer import WalWriter

__all__ = ["CorruptWal", "WalWriter", "open_wal"]
