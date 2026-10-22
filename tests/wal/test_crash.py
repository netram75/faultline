"""Crash the WAL over and over and check that the promise holds.

The promise: every record that was synced comes back after a crash, in
order, and nothing comes back that was never written.

Each seed is one run with its own random records, syncs and crashes. If a
seed fails, rerun just that one, for example:

    uv run pytest "tests/wal/test_crash.py::test_wal_keeps_its_promise[seed=17]"

and it fails the exact same way every time.
"""

import random

import pytest

from faultline.fs.fake import FakeFileSystem
from faultline.wal import open_wal

WAL = "/data/wal"


@pytest.mark.parametrize("seed", range(300), ids=lambda s: f"seed={s}")
def test_wal_keeps_its_promise(seed: int) -> None:
    rng = random.Random(seed)
    fs = FakeFileSystem(seed)
    fs.mkdir("/data")
    # Small segments, so plenty of runs cross segment boundaries
    segment_size = rng.choice([32, 128, 1024])

    log: list[bytes] = []  # every record the WAL should hold, in order
    synced = 0  # how many records at the front of `log` must survive

    for _ in range(rng.randint(1, 6)):
        recovered, wal = open_wal(fs, WAL, max_segment_size=segment_size)

        assert recovered[:synced] == log[:synced], "lost a synced record"
        assert recovered == log[: len(recovered)], "got back a record that was never written"

        # Whatever survived is on disk now, so it must survive the next crash too
        log = recovered
        synced = len(log)

        for _ in range(rng.randint(0, 40)):
            payload = rng.randbytes(rng.randint(0, 60))
            wal.append(payload)
            log.append(payload)
            if rng.random() < 0.2:
                wal.sync()
                synced = len(log)

        fs.crash()
