"""What a leader knows about one follower's log.

The Raft paper (figure 2) calls these nextIndex and matchIndex:

    match  the follower's log is known to match ours up to here
    next   the first entry to send it next time

A new leader knows nothing about anyone. match starts at 0 and next just
past the leader's own last entry, the hopeful guess that the follower is up
to date. A successful append moves both forward. A failed one means the
follower's log does not have our entry at next - 1, so next moves back.
"""

from dataclasses import dataclass


@dataclass
class Progress:
    match: int = 0
    next: int = 1

    def on_success(self, match_index: int) -> None:
        # max(), because replies can arrive out of order and an old one must
        # not move us backwards
        self.match = max(self.match, match_index)
        self.next = max(self.next, self.match + 1)

    def on_failure(self, hint: int) -> None:
        """`hint` is where the follower guesses our logs may match. Jump next
        back to just after it, instead of one entry per round trip.

        Always at least one step back, so the search ends. Never back past
        match: a late reply from before the logs matched must not undo what
        we already know.
        """
        self.next = max(self.match + 1, min(self.next - 1, hint + 1))
