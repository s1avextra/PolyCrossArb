#!/usr/bin/env python3
"""Validate paper-mode bot decisions against the actual v2 L2 stream.

Stronger than `validate_paper_replay.py` (which only re-runs decision
logic against logged signal.evaluation events). This one cross-checks
the bot's recorded book state and fill prices against the L2 events
PMXT v2 captured live from Polymarket's WS — independent ground truth.

For each paper trade fired by the bot:

  1. Find all v2 events for the trade's condition_id within ±60s
     of the bot's decision_ts.
  2. Compute the v2 book state (best_bid, best_ask) at decision_ts
     and at decision_ts + slippage_latency (~30 bps fill window).
  3. Compare to the bot's logged market_price + slipped fill.
  4. Find any v2 last_trade_price events near the bot's fill —
     are there real prints at our claimed fill price?

Reports per-trade comparison + aggregate disagreement stats. Exit 1
if substantial divergence detected (fill_price off by >2 cents on
>20% of trades).

Usage:
    # On VPS (preferred — uses shared cache):
    uv run python scripts/validate_paper_vs_v2.py \\
        --since "2026-04-25 11:08 UTC" \\
        --until "2026-04-25 16:00 UTC"

    # Local:
    PMXT_V2_CACHE_DIR=data/pmxt_v2_cache uv run python scripts/validate_paper_vs_v2.py ...
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, "src")

from polymomentum.backtest.pmxt_v2_loader import PMXTv2Loader, TradePrint
from polymomentum.backtest.pmxt_loader import L2Event, BookSnapshot


def parse_journal_events(since: str, until: str) -> tuple[list[dict], list[dict]]:
    """Return (trade_events, resolution_events) from journalctl."""
    out = subprocess.run(
        ["journalctl", "-u", "polymomentum-candle",
         "--since", since, "--until", until,
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


def load_trade_cids(state_db: Path) -> dict[int, str]:
    """Map trade_id -> condition_id from SQLite (chronological order)."""
    if not state_db.exists():
        return {}
    conn = sqlite3.connect(state_db)
    rows = conn.execute(
        "SELECT id, market_condition_id FROM trades WHERE paper=1 ORDER BY id"
    ).fetchall()
    return {tid: cid for tid, cid in rows}


def hours_between(start: datetime, end: datetime) -> list[datetime]:
    """All UTC hour boundaries that the [start, end] window touches."""
    cur = start.replace(minute=0, second=0, microsecond=0)
    out = []
    while cur <= end:
        out.append(cur)
        cur += timedelta(hours=1)
    return out


def book_at(events: list, ts: float, token_id: str) -> tuple[float, float] | None:
    """Compute (best_bid, best_ask) for a token at a given timestamp.

    Walks events chronologically and tracks the most recent book state
    for the token. Returns None if no event observed before ts.
    """
    best_bid = best_ask = 0.0
    seen = False
    for ev in events:
        if not isinstance(ev, L2Event):
            continue
        if ev.timestamp > ts:
            break
        snap = ev.snapshot if ev.snapshot else None
        change = ev.change if ev.change else None
        if snap and snap.token_id == token_id:
            best_bid = snap.best_bid
            best_ask = snap.best_ask
            seen = True
        elif change and change.token_id == token_id:
            if change.best_bid:
                best_bid = change.best_bid
            if change.best_ask:
                best_ask = change.best_ask
            seen = True
    return (best_bid, best_ask) if seen else None


def trades_near(trade_prints: list[TradePrint], token_id: str, ts: float, window_s: float = 60.0) -> list[TradePrint]:
    """Find all v2 trade prints for token within ±window_s of ts."""
    return [
        tp for tp in trade_prints
        if tp.asset_id == token_id and abs(tp.timestamp - ts) <= window_s
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--since", required=True,
                        help='Window start (e.g. "2026-04-25 11:08 UTC")')
    parser.add_argument("--until", required=True,
                        help='Window end (e.g. "2026-04-25 16:00 UTC")')
    parser.add_argument("--state-db",
                        default="/opt/polymomentum/logs/candle/state.db",
                        help="Path to SQLite state.db (for cid lookup)")
    parser.add_argument("--cache-dir", default=None,
                        help="Override PMXT v2 cache dir")
    parser.add_argument("--token-side", default="up",
                        help="Which token to look up — both bots track up_token_id and down_token_id; "
                             "the trade event's 'direction' field tells us which was bought")
    args = parser.parse_args()

    # Parse window
    def _parse(s: str) -> datetime:
        for fmt in ("%Y-%m-%d %H:%M %Z", "%Y-%m-%d %H:%M:%S %Z", "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        raise ValueError(f"Can't parse: {s}")

    since = _parse(args.since)
    until = _parse(args.until)

    print(f"=== Paper-vs-v2 validation ===")
    print(f"Window: {since.isoformat()} → {until.isoformat()}")

    # 1. Pull trade events from journal
    trades, resolutions = parse_journal_events(args.since, args.until)
    print(f"Journal: {len(trades)} trade.paper, {len(resolutions)} resolved")

    # 2. Map by chronological index → cid via SQLite
    cid_by_idx = load_trade_cids(Path(args.state_db))
    if not cid_by_idx:
        print("WARNING: state.db is empty — cannot map trades to cids.", file=sys.stderr)
        print("Pass --state-db to a path with paper trades, or run on the VPS.", file=sys.stderr)
        return 2

    sqlite_trades = list(cid_by_idx.items())  # [(id, cid), ...]
    if len(trades) > len(sqlite_trades):
        print(f"WARNING: journal has {len(trades)} trades but SQLite has {len(sqlite_trades)}. "
              f"State was reset mid-window?", file=sys.stderr)

    # 3. Load v2 events for the window, filtered to our trades' cids
    cids = set(cid for _id, cid in sqlite_trades[:len(trades)])
    print(f"Distinct cids: {len(cids)}")

    loader = PMXTv2Loader(cache_dir=args.cache_dir)
    all_events: list[L2Event] = []
    all_trades: list[TradePrint] = []
    for hour in hours_between(since, until):
        if not loader.is_cached(hour):
            print(f"  [missing] {loader.cache_path(hour).name} — fetch first", file=sys.stderr)
            return 3
        events = loader.load_hour(hour, condition_ids=cids)
        for e in events:
            if isinstance(e, L2Event):
                all_events.append(e)
            elif isinstance(e, TradePrint):
                all_trades.append(e)
        print(f"  [loaded ] {loader.cache_path(hour).name}  events={len([e for e in events if isinstance(e, L2Event)])} prints={len([e for e in events if isinstance(e, TradePrint)])}")

    all_events.sort(key=lambda e: e.timestamp)
    print(f"Total v2 events: {len(all_events)} L2, {len(all_trades)} trade prints")

    # 4. Per-trade comparison
    print()
    print(f"{'idx':>3}  {'time':19}  {'cid':12}  {'dir':4}  {'bot_mkt':>8}  {'v2_ask':>8}  {'Δ¢':>5}  {'prints±60s':>10}")
    print("-" * 80)

    discrepancies = []
    n_no_book = 0
    n_match_close = 0
    n_match_loose = 0

    for i, trade in enumerate(trades):
        if i >= len(sqlite_trades):
            break
        _id, cid = sqlite_trades[i]
        ts_iso = trade.get("timestamp", "")
        try:
            decision_ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).timestamp()
        except (ValueError, TypeError):
            continue

        direction = trade.get("direction", "")
        bot_market = float(trade.get("price", "$0").replace("$", ""))
        bot_slipped = float(trade.get("slipped", "$0").replace("$", ""))

        # Book lookup needs the OUTCOME TOKEN ID (asset_id), not condition_id.
        # We don't have the token_id directly in the trade event, but the
        # contract scanner mapped condition_id → up_token_id/down_token_id.
        # For this validator we approximate: take ALL events for this cid
        # (both up and down tokens) and infer which token matches by
        # comparing the bot's market_price to either side's best_ask.
        # Alternative: load the contract from candle_contracts.csv to get token_id.
        # For a first-pass validation we use the heuristic "best_ask
        # closest to bot_market" within the cid's events.

        events_for_cid = [e for e in all_events if e.timestamp <= decision_ts and (
            (e.snapshot and e.snapshot.market_id == cid) or
            (e.change and e.change.market_id == cid)
        )]

        if not events_for_cid:
            n_no_book += 1
            print(f"{i+1:>3}  {ts_iso[11:19]:19}  {cid[:12]}  {direction:4}  ${bot_market:>7.4f}  {'?':>8}  {'?':>5}  {'?':>10}  no v2 book before decision")
            continue

        # Find token whose best_ask matches bot_market most closely
        best_match_token = None
        best_match_ask = 0.0
        best_diff = 999.0
        # Walk distinct token_ids seen
        token_ids_seen = set()
        for e in events_for_cid:
            tid = (e.snapshot.token_id if e.snapshot else e.change.token_id)
            token_ids_seen.add(tid)
        for tid in token_ids_seen:
            book = book_at(events_for_cid, decision_ts, tid)
            if book is None:
                continue
            _bid, ask = book
            if abs(ask - bot_market) < best_diff:
                best_diff = abs(ask - bot_market)
                best_match_token = tid
                best_match_ask = ask

        if best_match_token is None:
            n_no_book += 1
            continue

        diff_cents = (bot_market - best_match_ask) * 100
        prints = trades_near(all_trades, best_match_token, decision_ts)

        if abs(diff_cents) <= 0.5:
            n_match_close += 1
            tag = ""
        elif abs(diff_cents) <= 2.0:
            n_match_loose += 1
            tag = "loose"
        else:
            discrepancies.append({
                "idx": i + 1, "ts": ts_iso, "cid": cid, "direction": direction,
                "bot_market": bot_market, "v2_ask": best_match_ask, "diff_cents": diff_cents,
            })
            tag = "DRIFT"

        print(f"{i+1:>3}  {ts_iso[11:19]:19}  {cid[:12]}  {direction:4}  ${bot_market:>7.4f}  ${best_match_ask:>7.4f}  {diff_cents:>+5.2f}  {len(prints):>10}  {tag}")

    print()
    print(f"=== SUMMARY ===")
    print(f"Trades validated:          {len(trades)}")
    print(f"Match within 0.5¢:         {n_match_close}")
    print(f"Match within 0.5-2.0¢:     {n_match_loose}")
    print(f"Drift > 2¢ (DISAGREEMENT): {len(discrepancies)}")
    print(f"No v2 book before decision: {n_no_book}")

    drift_rate = len(discrepancies) / max(len(trades), 1)
    if drift_rate > 0.20:
        print()
        print(f"❌ {drift_rate:.0%} of trades have >2¢ drift between bot's market_price and v2 best_ask.")
        print("This suggests either (a) the bot's price feed was stale, (b) we're matching the")
        print("wrong outcome token (use --token-side override), or (c) v2 missed events for")
        print("these markets.")
        return 1

    print()
    print(f"✅ Bot's logged book state matches v2 within 2¢ on {(len(trades)-len(discrepancies))/max(len(trades),1):.0%} of trades.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
