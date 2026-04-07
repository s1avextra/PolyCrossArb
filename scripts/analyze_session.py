#!/usr/bin/env python3
"""Analyze a live session's monitoring data.

Reads the JSONL session file and produces a diagnostic report
focused on identifying obstacles to production reliability.

Usage:
  python scripts/analyze_session.py                     # latest session
  python scripts/analyze_session.py logs/sessions/session_20260404_120000.jsonl
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "src")


def load_events(path: Path) -> list[dict]:
    events = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def analyze(events: list[dict]):
    orders = [e for e in events if e["cat"] == "order"]
    signals = [e for e in events if e["cat"] == "signal"]
    prices = [e for e in events if e["cat"] == "price"]
    risks = [e for e in events if e["cat"] == "risk"]
    resolutions = [e for e in events if e["cat"] == "resolution"]
    system = [e for e in events if e["cat"] == "system"]
    markets = [e for e in events if e["cat"] == "market"]

    print("=" * 65)
    print("  SESSION DIAGNOSTIC ANALYSIS")
    print("=" * 65)

    # ── Timeline ──
    if events:
        t0 = events[0]["ts"]
        t1 = events[-1]["ts"]
        duration = (t1 - t0) / 60
        print(f"\n  Duration: {duration:.1f} minutes ({len(events)} events)")

    # ── Order Analysis ──
    placed = [e for e in orders if e["type"] == "placed"]
    filled = [e for e in orders if e["type"] == "filled"]
    rejected = [e for e in orders if e["type"] == "rejected"]
    timeouts = [e for e in orders if e["type"] == "timeout"]
    cancelled = [e for e in orders if e["type"] == "cancelled"]

    print(f"\n  ORDERS")
    print(f"    Placed:    {len(placed)}")
    print(f"    Filled:    {len(filled)}")
    print(f"    Rejected:  {len(rejected)}")
    print(f"    Timeouts:  {len(timeouts)}")
    print(f"    Cancelled: {len(cancelled)}")

    if rejected:
        print(f"\n    Rejection reasons:")
        reasons = Counter(e.get("reason", "unknown") for e in rejected)
        for reason, count in reasons.most_common(10):
            print(f"      {reason}: {count}")

    if filled:
        slippages = [e.get("slippage", 0) for e in filled]
        slippage_bps = [e.get("slippage_bps", 0) for e in filled]
        fill_times = [e.get("fill_time_s", 0) for e in filled]
        fees = [e.get("fee", 0) for e in filled]
        fill_pcts = [e.get("fill_pct", 1) for e in filled]

        print(f"\n    Fill quality:")
        print(f"      Avg slippage:   ${sum(slippages)/len(slippages):.4f} ({sum(slippage_bps)/len(slippage_bps):.1f} bps)")
        print(f"      Max slippage:   ${max(slippages):.4f}")
        print(f"      Avg fill time:  {sum(fill_times)/len(fill_times):.1f}s")
        print(f"      Max fill time:  {max(fill_times):.1f}s")
        print(f"      Total fees:     ${sum(fees):.4f}")
        print(f"      Avg fill %:     {sum(fill_pcts)/len(fill_pcts):.1%}")
        partial = [e for e in filled if e.get("fill_pct", 1) < 0.95]
        if partial:
            print(f"      Partial fills:  {len(partial)} ({len(partial)/len(filled):.0%})")

    # ── Signal Analysis ──
    momentum = [e for e in signals if e["type"] == "momentum"]
    skips = [e for e in signals if e["type"] == "skip"]

    print(f"\n  SIGNALS")
    print(f"    Detected:  {len(momentum)}")
    print(f"    Skipped:   {len(skips)}")

    if skips:
        skip_reasons = Counter(e.get("reason", "?") for e in skips)
        print(f"    Skip breakdown:")
        for reason, count in skip_reasons.most_common(10):
            print(f"      {reason}: {count}")

    if momentum:
        traded = [e for e in momentum if e.get("traded")]
        confs = [e["conf"] for e in traded] if traded else []
        edges = [e["edge"] for e in traded] if traded else []
        mins_left = [e["mins_left"] for e in traded] if traded else []

        if confs:
            print(f"\n    Traded signals:")
            print(f"      Avg confidence: {sum(confs)/len(confs):.1%}")
            print(f"      Avg edge:       {sum(edges)/len(edges):.2%}")
            print(f"      Avg mins left:  {sum(mins_left)/len(mins_left):.1f}")

    # ── Resolution Analysis ──
    resolved = [e for e in resolutions if e["type"] == "resolved"]
    if resolved:
        wins = [e for e in resolved if e["won"]]
        losses = [e for e in resolved if not e["won"]]
        pnls = [e["pnl"] for e in resolved]

        print(f"\n  RESOLUTIONS")
        print(f"    Total:     {len(resolved)}")
        print(f"    Wins:      {len(wins)} ({len(wins)/len(resolved):.0%})")
        print(f"    Losses:    {len(losses)}")
        print(f"    Total P&L: ${sum(pnls):+.2f}")
        print(f"    Avg P&L:   ${sum(pnls)/len(pnls):+.2f}")

        if losses:
            print(f"\n    Loss analysis:")
            for e in losses:
                btc_move = e.get("btc_move", 0)
                print(f"      predicted={e['predicted']} actual={e['actual']} "
                      f"BTC move=${btc_move:+.0f} entry=${e['entry_price']:.3f}")

    # ─��� Price Feed Health ──
    snapshots = [e for e in prices if e["type"] == "snapshot"]
    dropouts = [e for e in prices if e["type"] == "source_dropout"]

    if snapshots:
        source_counts = [e.get("sources", 0) for e in snapshots]
        spreads = [e.get("spread", 0) for e in snapshots]
        staleness = [e.get("staleness_ms", 0) for e in snapshots]

        print(f"\n  PRICE FEED")
        print(f"    Snapshots:    {len(snapshots)}")
        print(f"    Avg sources:  {sum(source_counts)/len(source_counts):.1f}")
        print(f"    Min sources:  {min(source_counts)}")
        print(f"    Avg spread:   ${sum(spreads)/len(spreads):.2f}")
        print(f"    Max spread:   ${max(spreads):.2f}")
        print(f"    Avg stale:    {sum(staleness)/len(staleness):.0f}ms")
        print(f"    Max stale:    {max(staleness):.0f}ms")

    if dropouts:
        by_source = Counter(e["source"] for e in dropouts)
        print(f"    Dropouts:")
        for src, cnt in by_source.most_common():
            print(f"      {src}: {cnt}")

    # ── Market Availability ──
    scans = [e for e in markets if e["type"] == "scan"]
    if scans:
        candle_counts = [e.get("candle_contracts", 0) for e in scans]
        print(f"\n  MARKET AVAILABILITY")
        print(f"    Scans:        {len(scans)}")
        print(f"    Avg contracts: {sum(candle_counts)/len(candle_counts):.1f}")
        print(f"    Min contracts: {min(candle_counts)}")
        print(f"    Max contracts: {max(candle_counts)}")

    # ── Risk State Over Time ──
    risk_states = [e for e in risks if e["type"] == "state"]
    if risk_states:
        bankrolls = [e["bankroll"] for e in risk_states]
        exposures = [e["exposure"] for e in risk_states]

        print(f"\n  RISK TRAJECTORY")
        print(f"    Start bankroll: ${bankrolls[0]:.2f}")
        print(f"    End bankroll:   ${bankrolls[-1]:.2f}")
        print(f"    Peak bankroll:  ${max(bankrolls):.2f}")
        print(f"    Min bankroll:   ${min(bankrolls):.2f}")
        print(f"    Max exposure:   ${max(exposures):.2f}")

    # ── System Errors ──
    errors = [e for e in system if e["type"] == "error"]
    reconnects = [e for e in system if e["type"] == "reconnect"]
    api_calls = [e for e in system if e["type"] == "api_call"]

    if errors or reconnects:
        print(f"\n  SYSTEM HEALTH")
        print(f"    Errors:      {len(errors)}")
        print(f"    Reconnects:  {len(reconnects)}")
        if api_calls:
            latencies = [e.get("latency_ms", 0) for e in api_calls]
            print(f"    API calls:   {len(api_calls)}")
            print(f"    Avg latency: {sum(latencies)/len(latencies):.0f}ms")

        if errors:
            print(f"\n    Error breakdown:")
            err_types = Counter(e.get("component", "?") for e in errors)
            for comp, cnt in err_types.most_common(5):
                print(f"      {comp}: {cnt}")

    # ── Obstacles Summary ──
    print(f"\n{'='*65}")
    print(f"  IDENTIFIED OBSTACLES")
    print(f"{'='*65}")
    obstacles = []

    if len(rejected) > len(filled) * 0.5 and len(rejected) > 2:
        obstacles.append(f"High rejection rate: {len(rejected)}/{len(placed)} orders rejected")

    if filled:
        avg_slip = sum(abs(e.get("slippage", 0)) for e in filled) / len(filled)
        if avg_slip > 0.01:
            obstacles.append(f"High slippage: avg ${avg_slip:.4f} per trade")

    if snapshots:
        low_source = sum(1 for s in source_counts if s < 2)
        if low_source > len(snapshots) * 0.1:
            obstacles.append(f"Price feed instability: {low_source}/{len(snapshots)} snapshots with <2 sources")

    if dropouts:
        obstacles.append(f"Exchange dropouts: {len(dropouts)} total across {len(set(e['source'] for e in dropouts))} sources")

    if timeouts:
        obstacles.append(f"Fill timeouts: {len(timeouts)} orders timed out")

    if scans and min(candle_counts) == 0:
        zero_scans = sum(1 for c in candle_counts if c == 0)
        obstacles.append(f"No contracts available: {zero_scans}/{len(scans)} scans found 0 contracts")

    if errors:
        obstacles.append(f"System errors: {len(errors)} errors during session")

    if resolved:
        wr = len(wins) / len(resolved)
        if wr < 0.75:
            obstacles.append(f"Win rate below target: {wr:.0%} (target 85%+)")

    if not obstacles:
        print("  None detected — session looks clean!")
    else:
        for i, obs in enumerate(obstacles, 1):
            print(f"  {i}. {obs}")

    print(f"{'='*65}")


def main():
    if len(sys.argv) > 1:
        path = Path(sys.argv[1])
    else:
        # Find latest session file
        session_dir = Path("logs/sessions")
        if not session_dir.exists():
            print("No sessions found in logs/sessions/")
            return
        files = sorted(session_dir.glob("session_*.jsonl"))
        if not files:
            print("No session files found")
            return
        path = files[-1]
        print(f"Analyzing latest session: {path.name}")

    events = load_events(path)
    analyze(events)


if __name__ == "__main__":
    main()
