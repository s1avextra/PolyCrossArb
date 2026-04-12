#!/usr/bin/env python3
"""Latency-exploiting candle trading pipeline.

Orchestrator that:
1. Feeds Polymarket candle contracts to the Rust latency engine (stdin)
2. Reads trade signals from Rust engine (stdout)
3. Executes trades on Polymarket CLOB (paper or live)
4. Tracks P&L and manages risk

The Rust engine handles:
- 4-exchange BTC price aggregation (<1ms)
- Edge detection (stale MM price vs our fair value)
- Edge accumulation (scale-in logic)
- Full latency instrumentation

Usage:
  python scripts/run_latency.py [--mode paper|live] [--bankroll N]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "src")

import structlog
from polycrossarb.config import settings
from polycrossarb.data.client import PolymarketClient
from polycrossarb.crypto.candle_scanner import scan_candle_markets
from polycrossarb.crypto.price_feed import CryptoPriceFeed
from polycrossarb.execution.executor import ExecutionMode, SingleLegExecutor
from polycrossarb.ipc.bridge import EngineBridge
from polycrossarb.risk.manager import RiskManager
from polycrossarb.monitoring.session_monitor import SessionMonitor

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)
logging.basicConfig(level=logging.WARNING)
logging.getLogger("polycrossarb").setLevel(logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)

log = structlog.get_logger(__name__)

RUST_ENGINE = Path("rust_engine/target/release/polycrossarb-engine")
LOG_DIR = Path("logs")


class LatencyPipeline:
    """Orchestrates the Rust latency engine + Python CLOB execution."""

    def __init__(self, mode: ExecutionMode, bankroll: float):
        self.mode = mode
        self._client = PolymarketClient()
        self._risk = RiskManager(initial_bankroll=bankroll)
        self._monitor = SessionMonitor()
        self._price_feed = CryptoPriceFeed()
        self._running = False
        self._rust_proc: subprocess.Popen | None = None
        self._bridge: EngineBridge | None = None
        self._use_ipc = os.environ.get("IPC_MODE") == "uds"
        self._initial_bankroll = bankroll
        self._bankroll = bankroll

        # Stats
        self._signals_received = 0
        self._trades_executed = 0
        self._trades_skipped = 0
        self._total_pnl = 0.0
        self._total_fees = 0.0
        self._wins = 0
        self._losses = 0

        # Paper position tracking
        self._paper_positions: dict[str, dict] = {}
        # BTC price snapshots near window close for accurate resolution
        self._close_snapshots: dict[str, float] = {}

        # Live executor
        if mode == ExecutionMode.LIVE:
            self._executor = SingleLegExecutor(self._risk)
        else:
            self._executor = None

    async def run(self):
        self._running = True
        log.info("latency.start", mode=self.mode)

        if not RUST_ENGINE.exists():
            log.error("Rust engine not found. Run: cd rust_engine && cargo build --release")
            return

        # Start Rust engine as subprocess
        env = os.environ.copy()
        if os.environ.get("DEBUG") == "1":
            env["DEBUG"] = "1"
        self._rust_proc = subprocess.Popen(
            [str(RUST_ENGINE)],
            stdin=subprocess.PIPE if not self._use_ipc else subprocess.DEVNULL,
            stdout=subprocess.PIPE if not self._use_ipc else subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        log.info("latency.rust_started", pid=self._rust_proc.pid,
                 ipc="uds" if self._use_ipc else "stdio")

        # Read Rust stderr in background (diagnostics)
        asyncio.create_task(self._read_rust_stderr())

        # Wait for Rust to be ready, then connect via IPC if enabled
        await asyncio.sleep(3)
        if self._use_ipc:
            self._bridge = EngineBridge()
            if not await self._bridge.connect(timeout=10.0):
                log.error("latency.ipc_connect_failed")
                self.stop()
                return

        try:
            tasks = [
                self._price_feed.start(),
                self._contract_feed_loop(),
                self._resolution_loop(),
            ]
            if self._use_ipc:
                tasks.append(self._ipc_reader_loop())
            else:
                tasks.append(self._signal_reader_loop())
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            self._price_feed.stop()
            self.stop()

    def stop(self):
        self._running = False
        if self._bridge and self._bridge.connected:
            asyncio.ensure_future(self._bridge.disconnect())
        if self._rust_proc:
            self._rust_proc.terminate()
            self._rust_proc.wait(timeout=5)
            self._rust_proc = None

    async def _contract_feed_loop(self):
        """Fetch candle contracts every 30s and feed to Rust engine."""
        while self._running:
            try:
                markets = await self._client.fetch_all_active_markets(min_liquidity=0)
                contracts = scan_candle_markets(markets, max_hours=1.0, min_liquidity=50)

                # Convert to Rust format
                now = datetime.now(timezone.utc)
                contract_data = []
                for c in contracts:
                    try:
                        end = datetime.fromisoformat(c.end_date.replace("Z", "+00:00"))
                        end_time_s = end.timestamp()
                    except (ValueError, TypeError):
                        continue

                    # Estimate window minutes
                    desc = c.window_description.lower()
                    if "am-" in desc or "pm-" in desc:
                        window_minutes = 5.0  # default for range windows
                    else:
                        window_minutes = 60.0  # hourly

                    contract_data.append({
                        "contract_id": c.market.condition_id,
                        "token_id": c.up_token_id,  # we'll determine up/down in Rust
                        "up_price": c.up_price,
                        "down_price": c.down_price,
                        "end_time_s": end_time_s,
                        "window_minutes": window_minutes,
                    })

                # Send to Rust via IPC or stdin
                if self._use_ipc and self._bridge and self._bridge.connected:
                    await self._bridge.send_contracts(contract_data)
                elif self._rust_proc and self._rust_proc.stdin:
                    msg = json.dumps({"type": "contracts", "data": contract_data})
                    self._rust_proc.stdin.write(msg + "\n")
                    self._rust_proc.stdin.flush()

                log.info("latency.contracts_fed", count=len(contract_data))

            except Exception as e:
                log.warning("latency.contract_feed_error", error=str(e)[:100])

            await asyncio.sleep(30)

    async def _signal_reader_loop(self):
        """Read trade signals from Rust engine stdout and execute."""
        loop = asyncio.get_event_loop()
        while self._running and self._rust_proc:
            try:
                # Read line from Rust stdout (non-blocking via executor)
                line = await loop.run_in_executor(
                    None, self._rust_proc.stdout.readline
                )
                if not line:
                    if not self._running:
                        break
                    await asyncio.sleep(0.1)
                    continue

                signal_data = json.loads(line.strip())
                self._signals_received += 1
                await self._handle_signal(signal_data)

            except json.JSONDecodeError:
                continue
            except Exception as e:
                log.warning("latency.signal_error", error=str(e)[:100])
                await asyncio.sleep(0.1)

    async def _ipc_reader_loop(self):
        """Read trade signals from Rust engine via UDS IPC."""
        while self._running and self._bridge and self._bridge.connected:
            try:
                async for msg in self._bridge.read_messages():
                    if not self._running:
                        break
                    msg_type = msg.get("type", "")
                    if msg_type == "trade_signal":
                        self._signals_received += 1
                        await self._handle_signal(msg)
                    elif msg_type == "fill_report":
                        log.info("latency.fill_report",
                                 order_id=msg.get("order_id", "")[:16],
                                 latency_us=msg.get("latency_us", 0))
                    elif msg_type == "latency_report":
                        log.info("latency.report",
                                 avg_us=msg.get("avg_us", 0),
                                 p99_us=msg.get("p99_us", 0))
            except Exception as e:
                log.warning("latency.ipc_reader_error", error=str(e)[:100])
                if self._running:
                    await asyncio.sleep(1)
                    if self._bridge:
                        await self._bridge.reconnect_loop(max_retries=5)

    async def _handle_signal(self, sig: dict):
        """Process a trade signal from Rust engine."""
        action = sig.get("action", "")
        direction = sig.get("direction", "")
        edge = sig.get("edge", 0)
        mm_price = sig.get("mm_price", 0)
        btc_price = sig.get("btc_price", 0)
        btc_move = sig.get("btc_move", 0)
        entry_number = sig.get("entry_number", 1)
        size_usd = sig.get("size_usd", 1.0)
        contract_id = sig.get("contract_id", "")
        token_id = sig.get("token_id", "")
        mm_staleness = sig.get("mm_staleness_s", 0)
        tick_us = sig.get("tick_to_signal_us", 0)

        log.info("latency.signal",
                 action=action,
                 direction=direction,
                 edge=f"{edge:.1%}",
                 mm_price=f"${mm_price:.3f}",
                 btc_move=f"${btc_move:+.0f}",
                 stale=f"{mm_staleness:.0f}s",
                 entry=entry_number,
                 size=f"${size_usd:.2f}",
                 tick_us=tick_us)

        if self.mode == ExecutionMode.PAPER:
            # Paper trade: simulate fill at MM price + slippage
            slippage_bps = 30
            slipped = mm_price * (1 + slippage_bps / 10000)
            fee = size_usd * 0.002

            self._trades_executed += 1

            # Track for resolution
            if contract_id not in self._paper_positions:
                self._paper_positions[contract_id] = {
                    "direction": direction,
                    "entries": [],
                    "total_size": 0.0,
                    "end_time_s": sig.get("timestamp_s", 0) + sig.get("minutes_remaining", 5) * 60,
                    "open_btc": btc_price - btc_move,
                }

            pos = self._paper_positions[contract_id]
            pos["entries"].append({
                "price": slipped,
                "size": size_usd,
                "fee": fee,
                "btc": btc_price,
            })
            pos["total_size"] += size_usd

            # Log trade
            entry = {
                "timestamp": time.time(),
                "action": action,
                "direction": direction,
                "contract_id": contract_id[:20],
                "mm_price": mm_price,
                "slipped": round(slipped, 4),
                "edge": round(edge, 4),
                "btc_price": btc_price,
                "btc_move": round(btc_move, 1),
                "mm_staleness_s": round(mm_staleness, 1),
                "entry_number": entry_number,
                "size_usd": size_usd,
                "tick_to_signal_us": tick_us,
                "mode": "paper",
            }
            LOG_DIR.mkdir(exist_ok=True)
            with open(LOG_DIR / "latency_trades.jsonl", "a") as f:
                f.write(json.dumps(entry) + "\n")

        elif self._executor:
            # Live execution
            shares = size_usd / mm_price
            result = await self._executor.execute_single(
                token_id=token_id,
                side="buy",
                price=mm_price,
                size=round(shares, 1),
                neg_risk=False,
                event_id=contract_id,
            )
            if result.success:
                self._trades_executed += 1
                log.info("latency.filled",
                         fill=f"${result.fill_price:.3f}",
                         cost=f"${result.cost:.2f}")
            else:
                self._trades_skipped += 1
                log.warning("latency.rejected", error=result.error)

    async def _resolution_loop(self):
        """Resolve paper positions when windows expire using real BTC price.

        Full closed loop:
        1. Snapshot BTC price near window close (within 2s)
        2. Determine win/loss at resolution
        3. Calculate exact P&L including fees
        4. Update bankroll
        5. Send new bankroll to Rust engine
        """
        while self._running:
            await asyncio.sleep(1)

            now_s = time.time()
            btc = self._price_feed.btc_price
            if btc <= 0:
                continue

            # Snapshot BTC for positions within 2s of close
            for cid, pos in self._paper_positions.items():
                if cid not in self._close_snapshots and abs(now_s - pos["end_time_s"]) < 2:
                    self._close_snapshots[cid] = btc

            resolved = []
            for cid, pos in list(self._paper_positions.items()):
                if now_s < pos["end_time_s"]:
                    continue

                # Use snapshotted close price, fallback to current
                close_btc = self._close_snapshots.pop(cid, btc)
                open_btc = pos["open_btc"]

                # Determine actual outcome
                actual_direction = "up" if close_btc >= open_btc else "down"
                won = actual_direction == pos["direction"]

                # Calculate P&L
                total_cost = 0.0
                total_shares = 0.0
                total_fees = 0.0
                for e in pos["entries"]:
                    total_cost += e["size"]
                    total_shares += e["size"] / e["price"]
                    total_fees += e["fee"]

                if won:
                    payout = total_shares * 1.0  # token resolves to $1.00
                    pnl = payout - total_cost - total_fees
                    self._wins += 1
                else:
                    payout = 0.0  # token resolves to $0.00
                    pnl = -total_cost - total_fees
                    self._losses += 1

                self._total_pnl += pnl
                self._total_fees += total_fees
                self._bankroll += pnl

                avg_price = total_cost / max(total_shares, 0.001)
                btc_move = close_btc - open_btc

                log.info("latency.resolved",
                         contract=cid[:20],
                         direction=pos["direction"],
                         actual=actual_direction,
                         won="WIN" if won else "LOSS",
                         entries=len(pos["entries"]),
                         cost=f"${total_cost:.2f}",
                         payout=f"${payout:.2f}",
                         pnl=f"${pnl:+.2f}",
                         bankroll=f"${self._bankroll:.2f}",
                         btc_move=f"${btc_move:+.0f}",
                         open_btc=f"${open_btc:,.0f}",
                         close_btc=f"${close_btc:,.0f}")

                # Log resolution to file
                res_entry = {
                    "timestamp": time.time(),
                    "type": "resolution",
                    "contract_id": cid[:20],
                    "direction": pos["direction"],
                    "actual": actual_direction,
                    "won": won,
                    "entries": len(pos["entries"]),
                    "total_cost": round(total_cost, 4),
                    "total_shares": round(total_shares, 4),
                    "avg_entry_price": round(avg_price, 4),
                    "payout": round(payout, 4),
                    "pnl": round(pnl, 4),
                    "fees": round(total_fees, 4),
                    "bankroll_after": round(self._bankroll, 4),
                    "open_btc": round(open_btc, 2),
                    "close_btc": round(close_btc, 2),
                    "btc_move": round(btc_move, 2),
                }
                with open(LOG_DIR / "latency_trades.jsonl", "a") as f:
                    f.write(json.dumps(res_entry) + "\n")

                resolved.append(cid)

            if resolved:
                # Remove resolved positions
                for cid in resolved:
                    del self._paper_positions[cid]

                # Send updated bankroll to Rust engine
                self._send_balance_update()

    def _send_balance_update(self):
        """Send current bankroll to Rust engine so it updates capital limits."""
        if self._use_ipc and self._bridge and self._bridge.connected:
            asyncio.ensure_future(
                self._bridge.send_risk_update(round(self._bankroll, 2))
            )
            log.info("latency.balance_update", bankroll=f"${self._bankroll:.2f}", via="ipc")
            return
        if not self._rust_proc or not self._rust_proc.stdin:
            return
        try:
            msg = json.dumps({
                "type": "config",
                "data": {"bankroll_usd": round(self._bankroll, 2)},
            })
            self._rust_proc.stdin.write(msg + "\n")
            self._rust_proc.stdin.flush()
            log.info("latency.balance_update", bankroll=f"${self._bankroll:.2f}", via="stdin")
        except Exception as e:
            log.warning("latency.balance_update_failed", error=str(e)[:50])

    async def _read_rust_stderr(self):
        """Read Rust engine diagnostics from stderr."""
        loop = asyncio.get_event_loop()
        while self._running and self._rust_proc:
            try:
                line = await loop.run_in_executor(
                    None, self._rust_proc.stderr.readline
                )
                if line:
                    # Print Rust diagnostics to our stderr
                    print(f"[rust] {line.rstrip()}", file=sys.stderr)
            except Exception:
                break

    def status(self) -> dict:
        return {
            "signals": self._signals_received,
            "trades": self._trades_executed,
            "skipped": self._trades_skipped,
            "wins": self._wins,
            "losses": self._losses,
            "win_rate": f"{self._wins / max(self._wins + self._losses, 1):.0%}",
            "total_pnl": round(self._total_pnl, 2),
            "total_fees": round(self._total_fees, 4),
            "bankroll": round(self._bankroll, 2),
            "open_positions": len(self._paper_positions),
            "mode": str(self.mode),
        }


async def main(mode: str, bankroll: float):
    exec_mode = ExecutionMode.LIVE if mode == "live" else ExecutionMode.PAPER

    print(f"{'='*55}")
    print(f"  LATENCY PIPELINE — {'LIVE' if mode == 'live' else 'PAPER'} MODE")
    print(f"{'='*55}")
    print(f"  Bankroll:     ${bankroll:.2f}")
    print(f"  Strategy:     Exploit stale MM prices via 4-exchange feed")
    print(f"  Engine:       Rust (50ms ticks, <1us signal generation)")
    print(f"  Accumulation: Scale into positions as edge grows")
    print(f"{'='*55}")
    print()

    pipeline = LatencyPipeline(mode=exec_mode, bankroll=bankroll)

    loop = asyncio.get_event_loop()
    for sig_name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig_name, pipeline.stop)

    await pipeline.run()

    s = pipeline.status()
    print(f"\n{'='*55}")
    print(f"  SESSION RESULTS")
    print(f"{'='*55}")
    print(f"  Signals:    {s['signals']}")
    print(f"  Trades:     {s['trades']}")
    print(f"  Wins:       {s['wins']}")
    print(f"  Losses:     {s['losses']}")
    print(f"  Win Rate:   {s['win_rate']}")
    print(f"  P&L:        ${s['total_pnl']:+.2f}")
    print(f"  Fees:       ${s['total_fees']:.4f}")
    print(f"  Bankroll:   ${s['bankroll']:.2f} (started: ${bankroll:.2f})")
    print(f"  Open:       {s['open_positions']}")
    print(f"{'='*55}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--bankroll", type=float, default=6.0)
    args = parser.parse_args()
    asyncio.run(main(args.mode, args.bankroll))
