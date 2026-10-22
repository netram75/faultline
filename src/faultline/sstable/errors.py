class CorruptTable(Exception):
    """An SSTable has bytes that are not what we wrote.

    SSTables are only ever written whole and then synced, so a crash can't
    leave one half written. Any damage found here is real damage.
    """
