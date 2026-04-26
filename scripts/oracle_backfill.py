#!/usr/bin/env python3
"""Backfill Polymarket on-chain oracle resolutions for historical paper trades.

For any list of (cid, our_actual) pairs, queries the CTF contract via
eth_call and reports per-trade agreement plus aggregate stats. Useful
for:

  * Reconciling trades the bot fired but never resolved (e.g. orphaned
    by circuit breaker before the resolution loop saw them)
  * Auditing the bot's "X wins / Y losses" headline against on-chain
    truth — yesterday's run had 2 false wins of 36 (~5.6%) caught by
    this exact technique

Inputs (any combination):
  --cids cid1,cid2,...                    explicit cids
  --our-side u,d,d,u,...                  our prediction per cid (optional)
  --from-journal --since "2026-04-25 11:08 UTC"
                                         pull all candle.trade.paper +
                                         candle.resolved events from
                                         journalctl, then compare each
                                         to CTF (requires running on
                                         the VPS or an SSH tunnel)

Output: per-trade table + summary (agreed / disagreed / not-resolved).
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys

sys.path.insert(0, "src")

from polymomentum.data.ctf import get_resolution


def query_cid(cid: str) -> tuple[str | None, list[int]]:
    """Wrapper that returns (winner, [num0, num1]) and never raises."""
    try:
        return get_resolution(cid)
    except Exception as e:
        print(f"  ERROR querying {cid[:18]}...: {e}", file=sys.stderr)
        return None, [0, 0]


def parse_journal(since: str) -> tuple[list[dict], list[dict]]:
    """Pull candle.trade.paper + candle.resolved events from journalctl.

    Must run on the VPS (or in a context where journalctl can read the
    polymomentum-candle journal).
    """
    out = subprocess.run(
        ["journalctl", "-u", "polymomentum-candle", "--since", since,
         "--no-pager", "-o", "cat"],
        capture_output=True, text=True,
    )
    trades, resolutions = [], []
    for line in out.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if ev.get("event") == "candle.trade.paper":
            trades.append(ev)
        elif ev.get("event") == "candle.resolved":
            resolutions.append(ev)
    return trades, resolutions


def report(checks: list[dict]) -> int:
    """Print per-trade table + summary. Returns 0 if all-agreed, 1 if any disagreement."""
    agreed = sum(1 for c in checks if c["agreement"] == "agreed")
    disagreed = sum(1 for c in checks if c["agreement"] == "disagreed")
    pending = sum(1 for c in checks if c["agreement"] == "pending")
    no_ours = sum(1 for c in checks if c["agreement"] == "no_ours")

    print(f"=== Oracle backfill: {len(checks)} cids ===")
    print(f"{'cid':22}  {'our':>6}  {'pm':>6}  {'agreement':<12}  numerators")
    print("-" * 80)
    for c in checks:
        cid_short = c["cid"][:20] + ".."
        ours = c["our_actual"] or "-"
        pm = c["pm_actual"] or "-"
        nums = f"[{c['num0']},{c['num1']}]"
        print(f"{cid_short}  {ours:>6}  {pm:>6}  {c['agreement']:<12}  {nums}")
    print()
    print(f"Agreed:        {agreed}")
    print(f"Disagreed:     {disagreed}")
    print(f"Pending:       {pending}  (not yet resolved on-chain)")
    print(f"No our_actual: {no_ours}  (e.g. orphaned by circuit breaker)")

    if disagreed:
        print()
        print("DISAGREEMENTS (our resolution differs from Polymarket):")
        for c in checks:
            if c["agreement"] == "disagreed":
                print(f"  {c['cid'][:24]}... we={c['our_actual']} pm={c['pm_actual']}")

    return 1 if disagreed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--cids", help="Comma-separated condition_ids")
    parser.add_argument("--our-side", help="Comma-separated u/d for each cid (matches --cids order)")
    parser.add_argument("--from-journal", action="store_true",
                        help="Read trades from journalctl (must run where journal is accessible)")
    parser.add_argument("--since", default="1 day ago",
                        help="Journal time range (default: '1 day ago')")
    args = parser.parse_args()

    cids: list[str] = []
    our_sides: dict[str, str | None] = {}

    if args.from_journal:
        trades, _resolutions = parse_journal(args.since)
        # Note: journal events don't include cid directly. The mapping
        # cid → trade direction needs SQLite or the session JSONL. For
        # now, only support direct --cids when correlating to our_actual.
        if not args.cids:
            print("--from-journal currently only enumerates trades; combine with --cids "
                  "to cross-check against our_actual.", file=sys.stderr)
            print(f"Journal saw {len(trades)} candle.trade.paper events in the window.")
            return 0

    if args.cids:
        cids = [c.strip() for c in args.cids.split(",") if c.strip()]
        if args.our_side:
            sides = [s.strip() for s in args.our_side.split(",") if s.strip()]
            sides = ["up" if s.startswith("u") else "down" if s.startswith("d") else None
                     for s in sides]
            for cid, side in zip(cids, sides):
                our_sides[cid] = side

    if not cids:
        parser.print_help()
        return 2

    print(f"Querying {len(cids)} condition_ids against Polymarket CTF...")
    print()

    checks = []
    for cid in cids:
        winner, nums = query_cid(cid)
        ours = our_sides.get(cid)

        if winner is None:
            agreement = "pending"
        elif ours is None:
            agreement = "no_ours"
        elif ours == winner:
            agreement = "agreed"
        else:
            agreement = "disagreed"

        checks.append({
            "cid": cid,
            "our_actual": ours,
            "pm_actual": winner,
            "num0": nums[0],
            "num1": nums[1],
            "agreement": agreement,
        })

    return report(checks)


if __name__ == "__main__":
    sys.exit(main())
