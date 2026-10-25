"""python -m faultline.chaos: see run.py."""

import argparse
import asyncio
import sys

from faultline.chaos.run import FAULTS, run_chaos
from faultline.checker import explain


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m faultline.chaos",
        description="Break a real local cluster and check what the clients saw.",
    )
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--clients", type=int, default=3)
    parser.add_argument("--keys", type=int, default=3)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--faults", default=",".join(FAULTS), help="any of: kill,pause,leader")
    parser.add_argument("--history", help="write the history to this file")
    parser.add_argument("--data", help="keep the nodes' data and logs here (default: a temp dir)")
    args = parser.parse_args(argv)
    faults = tuple(f for f in args.faults.split(",") if f)
    if not set(faults) <= set(FAULTS):
        parser.error(f"faults must be among {', '.join(FAULTS)}")

    result = asyncio.run(
        run_chaos(args.seconds, args.nodes, args.clients, args.keys, args.seed, args.data, faults)
    )
    for line in result.faults:
        print(line)
    counts = result.counts
    print(f"operations: {counts['ok']} ok, {counts['fail']} failed, {counts['info']} unknown")
    print(f"linearizable: {result.report.verdict}")
    for bad in result.report.bad_keys:
        explanation = explain(bad)
        if explanation is not None:
            print("\n".join(explanation.lines()))
    if args.history:
        result.history.save(args.history)
        print(f"history written to {args.history}")
    return 0 if result.report.verdict == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
