"""What a leader knows about one follower's log, and how it sends to it.

The Raft paper (figure 2) calls the first two nextIndex and matchIndex:

    match    the follower's log is known to match ours up to here
    next     the first entry to send it next time
    probing  whether we still have to find out where its log matches ours

A new leader knows nothing about anyone. match starts at 0 and next just
past the leader's own last entry, the hopeful guess that the follower is up
to date. A successful append moves both forward. A failed one means the
follower's log does not have our entry at next - 1, so next moves back.

How much to send depends on what we know:

* **Probing.** We don't know where the follower's log matches ours. Each
  append may fail, so we send one at a time: when a reply comes back, or
  once per heartbeat if it doesn't. Sending more would only send more
  entries that get thrown away.
* **Streaming.** The last append worked. Now every new entry goes out as
  soon as it is proposed, and next moves past it straight away, without
  waiting for the reply. If a message gets lost, the next append fails its
  check and we are back to probing.
"""

from dataclasses import dataclass


@dataclass
class Progress:
    match: int = 0
    next: int = 1
    probing: bool = True

    def sent(self, count: int) -> None:
        """We just sent `count` entries starting at next."""
        if not self.probing:
            self.next += count

    def on_success(self, match_index: int) -> None:
        # max(), because replies can arrive out of order and an old one must
        # not move us backwards
        self.match = max(self.match, match_index)
        self.next = max(self.next, self.match + 1)
        self.probing = False

    def on_failure(self, hint: int) -> None:
        """`hint` is where the follower guesses our logs may match. Jump next
        back to just after it, instead of one entry per round trip.

        Always at least one step back, so the search ends. Never back past
        match: a late reply from before the logs matched must not undo what
        we already know.
        """
        self.next = max(self.match + 1, min(self.next - 1, hint + 1))
        self.probing = True
