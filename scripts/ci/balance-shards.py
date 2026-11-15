#!/usr/bin/env python3
"""Assign CI work items to shards by measured duration.

Items are read one per line from standard input. The weights file holds
"<item><TAB><seconds>" lines measured from CI (blank lines and lines starting
with "#" are ignored); the timing files that the fuzz and race shards write
use the same format, so they can be concatenated into a new weights file.

The assignment is the longest-processing-time-first heuristic: the heaviest
remaining item goes to the currently lightest shard, ties broken by item name
and shard index. It is deterministic, so every shard of a run computes the
same partition, and every input item lands in exactly one shard.

Usage:
  balance-shards.py [--default SECONDS] WEIGHTS SHARD COUNT  < items
  balance-shards.py [--default SECONDS] --plan WEIGHTS COUNT < items

The first form prints SHARD's items in input order. --plan prints every item
as "<shard><TAB><seconds><TAB><item>" followed by one "# shard <n> <seconds>"
total per shard. Without --default every item must have a weight; a weight for
an item that is not in the input always fails, so renamed or removed items
cannot silently leave a stale assignment behind.
"""
import argparse
import sys


def read_weights(path):
    weights = {}
    with open(path, "r", encoding="utf-8") as handle:
        for number, raw in enumerate(handle, 1):
            line = raw.rstrip("\n")
            if not line.strip() or line.startswith("#"):
                continue
            item, separator, seconds = line.rpartition("\t")
            try:
                value = float(seconds)
            except ValueError:
                value = -1.0
            if not separator or not item or not value > 0:
                raise SystemExit(f"{path}:{number}: want <item><TAB><positive seconds>")
            if item in weights:
                raise SystemExit(f"{path}:{number}: duplicate weight for {item}")
            weights[item] = value
    return weights


def read_items(stream):
    items = []
    seen = set()
    for raw in stream:
        item = raw.strip()
        if not item:
            continue
        if item in seen:
            raise SystemExit(f"duplicate item: {item}")
        seen.add(item)
        items.append(item)
    if not items:
        raise SystemExit("no items to shard")
    return items


def assign(items, weights, count, default):
    stale = sorted(set(weights) - set(items))
    if stale:
        raise SystemExit("weights name items that no longer exist: " + ", ".join(stale))
    missing = [item for item in items if item not in weights]
    if missing and default is None:
        raise SystemExit("items have no measured weight: " + ", ".join(missing))
    if count > len(items):
        raise SystemExit(f"{count} shards for {len(items)} items would leave a shard empty")
    cost = {item: weights.get(item, default) for item in items}
    loads = [0.0] * count
    shard_of = {}
    for item in sorted(items, key=lambda name: (-cost[name], name)):
        shard = min(range(count), key=lambda index: (loads[index], index))
        shard_of[item] = shard
        loads[shard] += cost[item]
    return shard_of, cost, loads


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--default", type=float, help="weight for items absent from WEIGHTS")
    parser.add_argument("--plan", action="store_true", help="print the whole assignment")
    parser.add_argument("weights")
    parser.add_argument("positions", nargs="+", metavar="SHARD COUNT")
    args = parser.parse_args()

    expected = 1 if args.plan else 2
    if len(args.positions) != expected:
        parser.error("want WEIGHTS COUNT with --plan, otherwise WEIGHTS SHARD COUNT")
    try:
        numbers = [int(value) for value in args.positions]
    except ValueError:
        parser.error("SHARD and COUNT must be integers")
    count = numbers[-1]
    if count < 1:
        parser.error("COUNT must be at least 1")
    if not args.plan and not 0 <= numbers[0] < count:
        parser.error(f"SHARD must be in [0, {count})")
    if args.default is not None and not args.default > 0:
        parser.error("--default must be positive")

    items = read_items(sys.stdin)
    shard_of, cost, loads = assign(items, read_weights(args.weights), count, args.default)
    if args.plan:
        for item in items:
            print(f"{shard_of[item]}\t{cost[item]:g}\t{item}")
        for index, load in enumerate(loads):
            print(f"# shard {index} {load:g}")
        return
    for item in items:
        if shard_of[item] == numbers[0]:
            print(item)


if __name__ == "__main__":
    main()
