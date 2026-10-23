import pytest

from faultline.raft.log import RaftLog
from faultline.raft.messages import Entry


def log_with_terms(*terms: int) -> RaftLog:
    log = RaftLog()
    log.append([Entry(term, f"cmd{i}".encode()) for i, term in enumerate(terms, start=1)])
    return log


def test_empty_log() -> None:
    log = RaftLog()

    assert log.last_index == 0
    assert log.last_term == 0
    assert log.term_at(0) == 0
    assert log.entries_from(1) == ()


def test_append_and_read() -> None:
    log = log_with_terms(1, 1, 2)

    assert log.last_index == 3
    assert log.last_term == 2
    assert log.entry(1) == Entry(1, b"cmd1")
    assert log.term_at(3) == 2
    assert log.entries_from(2) == (Entry(1, b"cmd2"), Entry(2, b"cmd3"))
    assert log.entries_from(1, max_count=2) == (Entry(1, b"cmd1"), Entry(1, b"cmd2"))
    assert log.entries_from(4) == ()


def test_out_of_range() -> None:
    log = log_with_terms(1)

    with pytest.raises(IndexError):
        log.entry(0)
    with pytest.raises(IndexError):
        log.entry(2)
    with pytest.raises(IndexError):
        log.entries_from(3)
    with pytest.raises(IndexError):
        log.truncate_from(0)


def test_matches() -> None:
    log = log_with_terms(1, 1, 2)

    assert log.matches(0, 0)
    assert log.matches(3, 2)
    assert not log.matches(3, 1)
    assert not log.matches(4, 2)


def test_truncate_from() -> None:
    log = log_with_terms(1, 1, 2, 3)

    log.truncate_from(3)

    assert log.last_index == 2
    assert log.last_term == 1
    log.truncate_from(3)  # cutting at the end is fine and does nothing
    assert log.last_index == 2


@pytest.mark.parametrize(
    ("other_index", "other_term", "expected"),
    [
        (3, 2, True),  # same log end
        (4, 2, True),  # same last term, longer
        (2, 2, False),  # same last term, shorter
        (1, 3, True),  # later last term wins even when much shorter
        (9, 1, False),  # earlier last term loses even when much longer
    ],
)
def test_is_up_to_date(other_index: int, other_term: int, expected: bool) -> None:
    log = log_with_terms(1, 1, 2)

    assert log.is_up_to_date(other_index, other_term) == expected
