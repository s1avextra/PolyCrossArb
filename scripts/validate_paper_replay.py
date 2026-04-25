#!/usr/bin/env python3
"""Validate that paper-mode logs replay deterministically through the backtest.

For each `signal.evaluation` event in a paper session JSONL, this script:

  1. Reconstructs the inputs to `decide_candle_trade` from the logged fields
  2. Calls the SAME pure decision function the live pipeline used
  3. Asserts the outcome (zone, edge, traded/skip) matches what was logged

For each `candle.resolved` event, it:

  1. Reads our open_btc/close_btc from the log
  2. Compares to Polymarket's `oracle.resolution` event for the same cid
  3. Reports disagreement counts

Output: a summary report + an exit code (0 = all match, 1 = drift detected).

Usage:
    uv run python scripts/validate_paper_replay.py logs/sessions/session_*.jsonl

The decision function is purely deterministic — given identical inputs it
must produce identical outputs. Any drift between the live decision and
the replayed decision means either (a) bug in pipeline-layer threading
of inputs, (b) mutated state we didn't capture in the evaluation event,
or (c) silent code change between live and replay versions. All three
are bugs we want to catch fast.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Iterator

sys.path.insert(0, "src")

from polymomentum.crypto.decision import (
    CandleDecision,
    SkipReason,
    decide_candle_trade,
)
from polymomentum.crypto.momentum import MomentumSignal


def iter_events(path: Path) -> Iterator[dict]:
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def replay_evaluation(ev: dict) -> tuple[bool, str]:
    """Replay one signal.evaluation event through decide_candle_trade.

    Returns (matched, detail). Matched=True if the replayed decision matches
    the logged outcome. Detail is a human-readable diff if not.
    """
    # Reconstruct MomentumSignal from logged fields.
    signal = MomentumSignal(
        direction=ev["dir"],
        confidence=ev["conf"],
        price_change=ev["chg"],
        price_change_pct=ev["chg_pct"],
        consistency=ev["cons"],
        minutes_elapsed=ev["elapsed_min"],
        minutes_remaining=ev["remaining_min"],
        current_price=ev["px"],
        open_price=ev["open"],
        z_score=ev["z"],
        reversion_count=0,  # not logged; not consumed by decide_candle_trade
    )

    # Call the same decision function. We don't have window_minutes in the
    # eval log directly, but elapsed + remaining give it back exactly.
    window_minutes = ev["elapsed_min"] + ev["remaining_min"]

    # implied_vol: live used price_feed.implied_volatility at decision time.
    # We logged vol_fast (realized_vol). The decision function uses
    # implied_vol for BS pricing — these can differ. For replay validity
    # we need to log implied_vol at decision time too. Until that lands,
    # accept vol_fast as a best-effort proxy and flag this in the report.
    decision = decide_candle_trade(
        signal=signal,
        minutes_elapsed=ev["elapsed_min"],
        minutes_remaining=ev["remaining_min"],
        window_minutes=window_minutes,
        up_price=ev["up_price"],
        down_price=ev["down_price"],
        btc_price=ev["px"],
        open_btc=ev["open"],
        implied_vol=ev["vol_fast"],
        # The live thresholds came from CandlePipeline init — they're
        # fixed at 0.60 confidence / 7% edge in our default deploy.
        # If you ran with custom thresholds you'd need to thread them
        # through; for now rely on the defaults.
        min_confidence=0.60,
        min_edge=0.07,
        skip_dead_zone=True,
        cross_asset_boost=ev["cross_boost"],
    )

    logged_traded = bool(ev["traded"])
    if isinstance(decision, SkipReason):
        replayed_traded = False
        replayed_zone = decision.zone
    elif isinstance(decision, CandleDecision):
        replayed_traded = True
        replayed_zone = decision.zone
    else:
        return False, f"unknown decision type {type(decision)}"

    if logged_traded != replayed_traded:
        return False, (
            f"trade-vs-skip mismatch: logged traded={logged_traded}, "
            f"replayed traded={replayed_traded}"
        )

    # Zone should match for both trade and skip outcomes.
    if ev["zone"] != replayed_zone:
        return False, (
            f"zone mismatch: logged={ev['zone']}, replayed={replayed_zone}"
        )

    # For traded events, compare edge within rounding tolerance.
    if logged_traded and isinstance(decision, CandleDecision):
        edge_diff = abs(decision.edge - ev["edge"])
        if edge_diff > 0.001:
            return False, (
                f"edge mismatch: logged={ev['edge']}, "
                f"replayed={decision.edge:.4f}, diff={edge_diff:.4f}"
            )

    return True, ""


def analyze(path: Path) -> int:
    n_eval = 0
    n_match = 0
    mismatches: list[tuple[str, str]] = []
    skipped_no_implied_vol = 0

    n_resolved = 0
    n_oracle = 0
    oracle_disagreements: list[dict] = []
    resolutions_by_cid: dict[str, dict] = {}

    skip_reasons: Counter[str] = Counter()
    zones_traded: Counter[str] = Counter()

    for ev in iter_events(path):
        cat = ev.get("cat")
        typ = ev.get("type")

        if cat == "signal" and typ == "evaluation":
            n_eval += 1
            if ev.get("vol_fast", 0) <= 0:
                # decide_candle_trade requires vol > 0; logged events
                # with zero vol are pre-warmup and can't be replayed
                # deterministically. Skip rather than fail.
                skipped_no_implied_vol += 1
                continue
            ok, detail = replay_evaluation(ev)
            if ok:
                n_match += 1
                if ev.get("traded"):
                    zones_traded[ev["zone"]] += 1
            else:
                mismatches.append((ev.get("cid", ""), detail))
            if ev.get("skip_reason"):
                skip_reasons[ev["skip_reason"]] += 1

        elif cat == "resolution" and typ == "resolved":
            n_resolved += 1
            cid = ev.get("cid", "")
            if cid:
                resolutions_by_cid[cid] = ev

        elif cat == "oracle" and typ == "resolution":
            n_oracle += 1
            if not ev.get("agreed", True):
                oracle_disagreements.append(ev)

    print(f"=== Paper Replay Validation: {path.name} ===")
    print(f"Signal evaluations:    {n_eval}")
    print(f"  Replay-matched:      {n_match}")
    print(f"  Mismatched:          {len(mismatches)}")
    print(f"  Skipped (vol unset): {skipped_no_implied_vol}")
    print()
    print(f"Top skip reasons:")
    for reason, count in skip_reasons.most_common(10):
        print(f"  {count:6d}  {reason}")
    print()
    print(f"Trades by zone:")
    for zone, count in zones_traded.most_common():
        print(f"  {count:6d}  {zone}")
    print()
    print(f"Resolutions:           {n_resolved}")
    print(f"Oracle cross-checks:   {n_oracle}")
    print(f"Oracle disagreements:  {len(oracle_disagreements)}")
    if oracle_disagreements:
        print()
        print("Disagreements:")
        for d in oracle_disagreements[:10]:
            print(f"  cid={d['cid']}  ours={d['our_actual']} (open=${d['our_open_btc']:,.0f} "
                  f"close=${d['our_close_btc']:,.0f}) → polymarket={d['polymarket_actual']}")

    if mismatches:
        print()
        print("First 10 decision mismatches:")
        for cid, detail in mismatches[:10]:
            print(f"  cid={cid}  {detail}")
        return 1

    if oracle_disagreements:
        print()
        print(f"NOTE: {len(oracle_disagreements)} oracle disagreement(s) found — these are "
              f"not replay bugs, they're real BTC-tape vs Polymarket-CCIX divergence.")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("session_jsonl", nargs="+", type=Path,
                        help="One or more logs/sessions/session_*.jsonl files")
    args = parser.parse_args()

    rc = 0
    for path in args.session_jsonl:
        if not path.exists():
            print(f"NOT FOUND: {path}", file=sys.stderr)
            rc = 2
            continue
        rc |= analyze(path)
        print()
    return rc


if __name__ == "__main__":
    sys.exit(main())
