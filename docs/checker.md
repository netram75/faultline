# The linearizability checker

The simulator ([simulator.md](simulator.md)) runs a cluster through crashes,
power cuts and network splits, and records every client operation: when it
started, when it ended, and what answer it got. The checker reads that
history and asks one question:

> Could a single copy of the data, doing one operation at a time, have given
> exactly these answers?

If yes, the history is **linearizable**, and the clients could not tell the
cluster apart from one machine that never fails. If no, the database gave
some client a wrong answer, and the checker shows where.

```python
from faultline.checker import check, explain

report = check(world.history)  # or History.from_jsonl(open("out.jsonl").read())
report.verdict  # "ok", "bad" or "unknown"
for result in report.bad_keys:
    print("\n".join(explain(result).lines()))
```

From the command line, every simulator run is checked:

```
python -m faultline.sim --seed 7                     # linearizable: ok
python -m faultline.sim --seed 0 --bug early-ack     # switch on a known bug and watch it get caught
```

It looks only at what clients saw. It never looks inside Raft or the
storage engine. That is the point: it would catch a bug in any of them, or
in parts that don't exist yet.

## What linearizable means

Each operation takes effect at one moment, somewhere between its start and
its end. The history is fine if we can pick those moments so that every
answer is what a single copy would give.

```
client 1:  [--- put a ---]
client 2:                     [-- get -> missing --]
```

Not linearizable. The put ended before the get started, so the get had to
see it. This is a **stale read**, and it is the bug most databases ship at
some point.

```
client 1:  [------------- put a -------------]
client 2:        [-- get -> missing --]
```

Fine. The two overlap, so the get may have taken effect before the put.

## From events to operations (ops.py)

The history has two events per operation: the invoke, and how it ended.

| Ended | Meaning | What the checker does |
|---|---|---|
| ok | it happened, with this answer | must be placed, inside its window |
| fail | it surely did not happen | dropped |
| info | nobody knows: no answer came | a write may be placed any time after its start, or never; a read is dropped |

A write with no answer is the hard part. The client gave up waiting, but
the write may still be in some node's log and take effect a second later.
So it has a start and no end.

Windows are **positions in the history**, not milliseconds. The simulator
can do several things in the same millisecond. With times, a client that
starts its next operation in the same millisecond its last one ended would
look like it ran both at once, and the checker would let them swap.

## The model (model.py)

The value of one key, or missing. Each operation either fits the value or
it doesn't:

- **put:** always fits. The value becomes the new one.
- **get:** fits if it read the current value.
- **cas that matched:** fits if the value was the expected one. The value
  becomes the new one.
- **cas that did not match:** fits if the value was anything else.
- **cas with no answer:** always fits. It writes only if the value was the
  expected one.

## The search (linear.py)

The idea is from Wing and Gong (1993), with a memo from Lowe (2017). It is
the same search the Knossos (Jepsen) and Porcupine checkers use.

Build the order from the front, one operation at a time. An operation may go
next only if no other waiting operation ended before it started. Try it on
the model. If it fits, place it and carry on. If nothing fits, take the last
one back out and try another. Done when every operation with an answer is
placed.

The **memo**: two paths often reach the same place, the same operations
placed and the same value. What can happen from there depends only on those
two, so the second time we skip it.

### Making it fast

The first version was correct and fast on ordinary runs. Then the bug hunt
ran it on harsh settings (8 clients on one key, 15 ms client timeouts) and
19 of 400 runs came back **unknown**: the search ran out of steps. Every one
of them turned out to be fine. The search was just slow. It took five
changes, each one safe for the reason given:

1. **A linked list of what is waiting.** The ops that may go next are the
   starts before the first end, so only the front of the list is read.
   Taking an op out and putting it back are a few pointer changes. Before,
   each step scanned every op.
2. **Ops with an answer first.** An op with no answer has no end, so it can
   always wait.
3. **An op with no answer goes only where it is needed.** It must change the
   value, and the next op must be one that could not go without it. If the
   next op could go anyway, the same place is reached with it first, or
   without the write at all.
4. **Values nobody reads are all one value.** If no get reads it and no cas
   expects it, nothing can tell it apart from another such value. And two
   writes with no answer that do the same thing count as one.
5. **Never strand a read.** The key is never moved off a value that a
   waiting read still needs, if nothing left can write that value again.
   That read could never go, so we stop right there instead of finding out
   thousands of ops later.

The memo also got smarter: a place reached again, with the same answered ops
and value, but with *more* of the no-answer writes used up, is nothing new.

Afterwards, all 19 were ok in under 0.2 seconds each.

## One key at a time (check.py)

Linearizability is **local** (Herlihy and Wing 1990): a history is
linearizable exactly when each key's part is. So each key is searched on its
own, which is much smaller than searching everything at once.

Each key gets a **budget** of 2,000,000 model steps, about a second. The
search is exponential in the worst case: the problem is NP-complete. If the
budget runs out, that key's verdict is **unknown**. Not ok, not bad. One
bad key makes the whole history bad. Otherwise, one unknown key makes it
unknown.

## Explaining a failure (explain.py)

A bad key from a long run has hundreds of operations. The explanation finds
the few that matter:

```
key k1: no order of its operations gives these answers.
The first answer that can't be explained:
  #605   client 2   at   7221 ms  get -> 3-127  (ok)
When it started, k1 could only hold: 2-123
Operations around it:
  #600   client 3   at   7171 ms  get -> 3-127  (ok)
  #602   client 1   at   7199 ms  cas 3-126 -> 1-112  (did not match)
  #604   client 2   at   7213 ms  put 2-123  (ok)
```

1. **Cut the history** at a position: keep what happened before it, and
   treat ops still running there as if no answer came. Cutting can only
   remove problems, never add one. Binary search finds the first position
   where the cut history is bad. The answer that arrived there is the first
   one that no order can explain.
2. **What the key could hold** when that op started: every value, over all
   orders that work up to there.
3. **The ops around it:** the ones that overlap it, and the last few that
   ended before it.

Why not shrink the history by dropping operations one at a time, while it
stays bad? It is the obvious idea, and it is wrong. Dropping a write can make
a good history look bad. In "put x, put y, then a cas from x that did not
match", dropping put y makes the cas look wrong. The explanation would point
at a problem that isn't there.

## How we know the checker is right

A checker that is wrong in the "ok" direction hides bugs, and one that is
wrong in the "bad" direction sends you chasing ghosts. So:

- **Brute force.** `tests/checker/test_brute_force.py` builds small random
  histories and checks each one with a slow checker that tries every order
  of every set of operations, sharing no code with the real one. They must
  agree. 6,000 histories in the test suite. A one-off run of 40,000 more,
  many of them heavy with writes that got no answer, found no disagreement
  either.
- **Hand-made histories** (`tests/checker/test_histories.py`), one rule each:
  stale read, lost write, two cas that both won, a read that sees the new
  value and then the old one, writes with no answer that show up late.
- **Broken on purpose.** Each of these was put into the checker, one at a
  time, and the tests caught it:

| Bug put in the checker | Caught by |
|---|---|
| An op may go next even after another ended before it started | hand-made, brute force |
| The window edge is off by one | brute force |
| The memo ignores the value | a test where only the second order works, brute force |
| Going back does not restore the value | many tests |
| The linked list puts ops back in the wrong order | many tests |
| A cas that did not match fits anything | hand-made, brute force |
| A cas that matched fits anything | hand-made |
| A cas with no answer always writes | hand-made, brute force |
| All keys searched as one | a write to one key seen on another |
| Out of budget counted as bad; budget ignored; unknown hides bad (3 bugs) | budget tests |
| Speed changes 2 to 5, or the smarter memo, taken out (7 bugs) | speed tests with a step budget |
| Speed changes done wrong: 3 looks at the value after the write, 4 merges values that are read, 5 miscounts in three ways (5 bugs) | brute force, hand-made, speed tests |
| The smarter memo's subset test turned around | only a direct test of the memo |
| Explanation: running writes keep their answer, the wrong cut is found, "could hold" is taken at the end, final values stop at the first (4 bugs) | explanation tests |

29 bugs, all caught. The second to last needed a test of its own: the search
order (answered ops first) almost never reaches the case where it matters,
so no history-level test noticed.

One more was tried and turned out not to be a bug at all: making every write
with no answer *required*. A write with no answer has no end, so it can
always go last, after everything else. Requiring it only makes the search
slower.

## Known limits

- **Registers only:** put, get and compare-and-set on single keys. Range
  scans and deletes are not in the simulator's workload, so the model
  doesn't have them.
- **Unknown is possible.** Some histories are too hard for any checker of
  this kind. Jepsen's answer is to keep each key's history short and move on
  to a fresh key often. The simulator uses a few keys for the whole run,
  which is fine at its default settings: the bug hunt never got an unknown
  there.
- **It trusts the history.** If the simulator recorded something wrong, the
  checker would check the wrong thing. The history code is small and has
  its own tests.
