# PolyMomentum

Single-strategy bot trading **"Up or Down" 5/15-min crypto candle markets on Polymarket**. Multi-exchange momentum signal → BS-binary fair-value mispricing detection → CLOB execution.

> **Status (2026-04-25):** Paper mode running 24/7 on a shared VPS. Post-audit backtests show break-even — terminal-zone-only is the one sub-strategy with positive bias. **No live trades have been placed.** Wallet is funded ($6.03 USDC.e + ~5.37 POL) but only as a tip in the water.

## What it does

For each active candle window:

1. **Subscribe** to 8-exchange spot WS feeds (Binance, Bybit, OKX, Coinbase, Bitget, HTX, Gate.io, Kraken) for the underlying asset (BTC primary; ETH/SOL alts).
2. **Detect momentum** via `MomentumDetector`: z-score of the move from window-open against EWMA fast/slow realized vol, plus consistency.
3. **Compute BS fair value** of the binary "above strike" using observed implied vol from Deribit + window time-to-expiry.
4. **Compare to market** (top of book on the candle's YES/NO Polymarket tokens). Mispricing > threshold + zone-conditional confidence/edge gates → trade.
5. **Hold to resolution** (window close), then mark won/lost vs our BTC tape AND cross-check against Polymarket's CCIX oracle.

Zone gates split the window into **early / primary / late / terminal** bands with independent thresholds. The post-audit reality is that **only terminal-zone (last 5%) entries showed profit** in our backtests; the other zones are break-even or losing.

## What's running

```
VPS (193.24.234.202, alias `vps`):
  /opt/polymomentum/current → releases/2026-04-25T110627Z
  
  systemd:
    polymomentum-rust.service         (Rust latency engine, WS feeds, CLOB signing)
    polymomentum-candle.service       (Python pipeline, --mode paper)
    polymomentum-healthcheck.timer    (every 60s)

  Coexistence caps (multibot host shared with adgts + polyarbitrage):
    CPUQuota=80%  MemoryMax=512M  TasksMax=256  per unit
```

See [memory/project_state.md](https://github.com/s1avextra/PolyMomentum/blob/main/.claude/projects/-Users-ttoomm-Documents-PolyMomentum/memory/project_state.md) — wait, that's gitignored. The handoff doc lives at `~/.claude/projects/-Users-ttoomm-Documents-PolyMomentum/memory/project_state.md` on the operator's machine.

## Operational commands

```bash
# Health
ssh vps 'systemctl is-active polymomentum-rust polymomentum-candle adgts polyarbitrage'

# Live cycle log (cycle_ms, price_staleness_ms, top_skips, trade count)
ssh vps 'journalctl -u polymomentum-candle -f -n 5 | grep candle.cycle'

# Trade tape
ssh vps 'journalctl -u polymomentum-candle | grep -E "candle.trade.paper|candle.resolved"'

# Kill switch (halts trading within ~100ms)
ssh vps 'touch /tmp/polymomentum/KILL'
ssh vps 'rm /tmp/polymomentum/KILL && systemctl restart polymomentum-candle'

# Deploy
bash deploy/deploy.sh vps           # Python only (~10s)
bash deploy/deploy.sh vps --rust    # also rebuild Rust binary (~2.5min)

# Rollback
ssh vps '/opt/polymomentum/current/deploy/rollback.sh'
```

## Replay-grade data collection

Paper-mode design goal: paper run logs replayable through the backtest harness producing **identical PnL**, AND paper fills representative of live fills. As of `19b82b8`:

- **Per-evaluation JSONL** (`logs/sessions/session_*.jsonl`) — `cat=signal type=evaluation` events fire on every contract evaluation (trade or skip), with full state: open price, current price, z-score, confidence, EWMA fast/slow vol, cross-asset boost, top of book, decision zone, fair value, edge, traded flag, skip reason + detail.
- **Cycle latency** — `cycle_ms` + `price_staleness_ms` per cycle so we can calibrate the backtest's static-latency assumption.
- **Polymarket oracle cross-check** — `cat=oracle type=resolution` events compare our BTC-tape resolution to Polymarket's actual settlement (their CCIX index). Disagreement = real strategy risk we now quantify.
- **Persistent state** (`f61ff0a`): `logs/` and `data/` are symlinked to `/opt/polymomentum/{logs,data}` shared dirs, so SQLite + JSONL + CSV survive deploys.

Validate a paper session against the backtest:
```bash
uv run python scripts/validate_paper_replay.py logs/sessions/session_*.jsonl
```
Exit 0 = clean, 1 = decision drift detected.

## Critical files

| | |
|---|---|
| Live entry | `scripts/run_production.py --mode paper\|live` |
| Backtest harness | `scripts/run_strategy_harness.py` |
| Replay validator | `scripts/validate_paper_replay.py` |
| Live runtime | `src/polymomentum/crypto/candle_pipeline.py` |
| Pure decision (live + backtest) | `src/polymomentum/crypto/decision.py` |
| Momentum + EWMA vol | `src/polymomentum/crypto/momentum.py` |
| BS pricer | `src/polymomentum/crypto/fair_value.py` |
| Multi-exchange WS aggregator | `src/polymomentum/crypto/price_feed.py` |
| Polymarket fee formula | `src/polymomentum/execution/fees.py` |
| L2 backtest engine | `src/polymomentum/backtest/l2_replay.py` |
| Rust latency engine | `rust_engine/src/main.rs`, `rust_engine/src/exchange.rs` |

## Tests

```bash
uv sync --all-extras              # one-time install of dev + execution extras
uv run pytest -q                  # 117 passed, 2 skipped
cd rust_engine && cargo test      # 14 passed
```

## Multibot etiquette

PolyMomentum shares the VPS with two other bots: **adgts** (XRP/USDT futures grid, port 9092) and **polyarbitrage** (port 127.0.0.1:9090). Don't touch their `/opt/<name>`, `/etc/<name>`, or systemd units. Coexistence caps applied via systemd drop-ins. Use `nice -n 10 cargo build --release` for Rust builds. See [docs/cross_bot_note_mexc_hardening.md](docs/cross_bot_note_mexc_hardening.md) for the cross-Claude coordination protocol.

## Strategy reality check

Post-audit (after fixing 4 lookahead/precision bugs): backtests show **break-even to losing** across baseline, ewma_15min, regime variants. The terminal-zone-only sub-strategy had +$7.66 on 13 trades in one window — promising but tiny sample. **Going live with capital today would be premature.** The current play is to collect 24h+ of replay-grade paper data, then iterate (entry-price filter, terminal-only deployment, more backtest windows) before flipping any live switch.

---

*Repository renamed from PolyCrossArb → PolyMomentum on 2026-04-13 when the cross-arb and weather strategies were deleted (-10,225 LOC).*
