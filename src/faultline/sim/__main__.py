"""Run the fault simulator from the command line.

    python -m faultline.sim --seed 7 --seconds 60 --faults heavy
    python -m faultline.sim --seed 1 --runs 100            # seeds 1 to 100, one line each
    python -m faultline.sim --seed 7 --history out.jsonl   # keep the history for the checker
    python -m faultline.sim --seed 7 --log                 # print what the nemesis did
    python -m faultline.sim --seed 1 --runs 20 --bug early-ack   # see the checker catch a bug

The exit code is 1 if any run broke a safety rule, got stuck, or gave
answers that are not linearizable, so a script or CI job can run many seeds
and stop at the first bad one. Every failure prints the exact command that
replays it. A history that is not linearizable is also explained, in the
few operations where it first goes wrong.
"""

import argparse
import sys

from faultline.checker.check import check
from faultline.checker.explain import explain
from faultline.sim.bugs import BUGS
from faultline.sim.checks import SafetyViolation
from faultline.sim.nemesis import PROFILES
from faultline.sim.run import simulate


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m faultline.sim", description=__doc__)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--runs", type=int, default=1, help="run this many seeds, from --seed on")
    parser.add_argument("--seconds", type=int, default=30, help="simulated seconds of trouble")
    parser.add_argument("--faults", choices=sorted(PROFILES), default="heavy")
    parser.add_argument("--nodes", type=int, default=5)
    parser.add_argument("--clients", type=int, default=3)
    parser.add_argument("--history", help="write the history to this file (single run only)")
    parser.add_argument("--log", action="store_true", help="print the nemesis log")
    parser.add_argument("--bug", choices=sorted(BUGS), help="switch on a known bug (bugs.py)")
    args = parser.parse_args(argv)

    bad = 0
    for seed in range(args.seed, args.seed + args.runs):
        replay = (
            f"python -m faultline.sim --seed {seed} --seconds {args.seconds} --faults {args.faults}"
            f" --nodes {args.nodes} --clients {args.clients}"
            + (f" --bug {args.bug}" if args.bug else "")
        )
        try:
            summary, world, nemesis = simulate(
                seed, args.seconds, args.faults, args.nodes, args.clients, bug=args.bug
            )
        except SafetyViolation as violation:
            print(f"SAFETY VIOLATION: {violation}")
            print(f"replay with: {replay}")
            bad += 1
            continue
        if args.runs == 1:
            print("\n".join(summary.lines()))
        else:
            status = ", ".join(summary.problems) or "ok"
            print(f"seed {seed}: {status}, {summary.ok} ops ok, digest {summary.history_digest}")
        if summary.stuck:
            print(f"replay with: {replay} --log")
        if summary.linearizable == "bad":
            for result in check(world.history).bad_keys:
                explanation = explain(result)
                if explanation is not None:
                    print("\n".join(explanation.lines()))
            print(f"replay with: {replay} --history history.jsonl")
        if summary.linearizable == "unknown":
            print("the checker ran out of steps on some key, try a shorter run")
        if summary.problems:
            bad += 1
        if args.log:
            for time, what in nemesis.log:
                print(f"{time:>8} ms  {what}")
        if args.history and args.runs == 1:
            world.history.save(args.history)
            print(f"history written to {args.history}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
