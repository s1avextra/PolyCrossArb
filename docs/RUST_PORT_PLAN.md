# Rust Port — Status & Forward Plan

**Decision date:** 2026-04-26
**Status as of 2026-04-26 (late session):** Python implementation removed. Rust binary `polymomentum-engine` is the only source of truth.

## Why the port happened

A 2026-04-26 validation against PMXT v2 archive (real-time L2 capture) revealed the Python pipeline was firing trades against book state **30-50¢ stale on the terminal-zone snap**: the contract scanner refreshed every 30-120 s while the matcher snaps prices in seconds. Bug was wiring, not perf — but the operator chose to use it as the trigger for a single-language Rust stack, eliminating the Rust-Python boundary the legacy IPC engine straddled.

Python paper bot stopped 2026-04-26 ~10:09 UTC after 14 h of running. Wallet untouched throughout.

## What landed (commits `3860163` + this session)

A single Rust binary, **`polymomentum-engine`**, with these subcommands:

| Subcommand | What it does |
|---|---|
| `live --mode paper\|live` | The 10 Hz cycle loop. Paper by default; `live` requires `--i-understand-live` AND a populated `PRIVATE_KEY` + `POLY_API_KEY`. |
| `scan` | Smoke test: pull markets from Gamma, scan for candles, print summary. |
| `wallet` | Print USDC.e / native USDC / POL balances for the configured private key. |
| `ctf <condition_id>` | Read the on-chain CTF resolution (eth_call) for a market. |
| `validate-replay <session.jsonl>` | Replay every `signal.evaluation` event through `decide_candle_trade` and assert the trade/skip outcome matches what the live process logged. Exit 0 = clean, 1 = drift. |

Module layout (all under `rust_engine/src/`):

```
main.rs                         clap dispatch
lib.rs                          module re-exports
config.rs                       env-driven Settings
data/{gamma,scanner,ctf,wallet,models}
strategy/{momentum,decision}    pure logic
fair_value.rs                   Black-Scholes binary pricer
execution/fees.rs               polymarket_fee formula
risk/manager.rs                 SQLite RiskManager (state.db schema unchanged from Python)
monitoring/{session,alerter}    JSONL writer + Slack webhook
live/{pipeline,window,breaker,paper_fill}    cycle loop + sub-loops
polymarket_ws.rs                full L2 book WS feed (dynamic resub)
exchange.rs                     Binance/Bybit/OKX BTC + Binance/Bybit ETH+SOL alts + Deribit IV
price_state.rs                  multi-source aggregation
clob.rs                         CLOB direct order placement (live mode)
signing.rs                      EIP-712 order signing
```

Phase rollups (reference for what was actually delivered):

- **Phase 1 — Foundation** ✔ Cargo deps, module reshuffle, env-driven config, Gamma client, candle scanner (11 supported assets), CTF reader, wallet reader, polymarket_ws extended for full L2 + dynamic resubscription.
- **Phase 2 — Strategy core** ✔ MomentumDetector with EWMA fast/slow vol + z-score, `decide_candle_trade` with 4-zone gates / dead-zone filter / EV buffer / `edge_cap`. Tests cover skip + trade paths.
- **Phase 3 — Risk + monitoring** ✔ `RiskManager` over rusqlite, schema-compatible with the Python `state.db` so cutover preserves history. `SessionMonitor` writes JSONL with the schema the validator expects. Slack alerter, drawdown + win-rate breaker (eager + post-resolution).
- **Phase 4 — Live runtime** ✔ Cycle loop + paper resolution + CTF oracle verification + monitoring + contract refresh. Verified end-to-end: 60 s soak, 378 events, 186 evaluations replay clean (0 mismatches), 0.4-0.9 ms per cycle. State persists across kill+restart (paper positions, total_pnl, breaker meta).
- **Phase 5 — Live execution** ✔ Wired through `clob.rs` (EIP-712-signed maker → taker fallback). Gated behind `--i-understand-live` AND a populated `PRIVATE_KEY` + `POLY_API_KEY` — no live trade has been executed yet.

Phases 6 and 7 of the original plan were superseded:

- **Phase 6 — Backtest harness (PMXT v2 + L2 replay)** is **deferred**. The Rust pipeline writes the same replay-grade JSONL the Python backtest consumed. `validate-replay` covers paper-vs-decision parity. A full Rust backtest harness (parquet loaders + L2 replay engine + fill models + strategies + harness CLI) would land in a separate effort, sized roughly:

  | Component | Python LOC | Estimated Rust |
  |---|---:|---:|
  | PMXT v1+v2 loader | 690 | ~400 |
  | BTC history (causality-tested) | 351 | ~200 |
  | L2 replay engine | 836 | ~600 |
  | Fill models (OneTick/BookWalk/Maker/Perfect) | 283 | ~200 |
  | Candle resolver | 285 | ~150 |
  | Strategies + harness | 720 | ~500 |
  | Tests | ~600 | ~300 |
  | **Total** | **~3,800** | **~2,400** + deps `parquet`, `arrow` |

  Until then, strategy iteration uses live paper data with `validate-replay` for sanity.

- **Phase 7 — Cutover** ✔ This session removed the Python tree entirely (`src/polymomentum/`, `tests/`, `scripts/`, `pyproject.toml`, `uv.lock`, `Dockerfile`), retired the legacy Rust IPC binary (`legacy_main.rs`, `ipc.rs`, `latency.rs`, `edge.rs`, `debug.rs`), and switched deploy / systemd to the new single-binary path. The repo is Rust-only.

## Tests

```bash
cd rust_engine
cargo test       # 51 unit tests: config, gamma, scanner, momentum, decision, risk,
                 # session, fees, fair_value, signing, paper_fill, breaker, window
cargo build --release
```

Smoke tests run in this session:
- `scan` returns 231 candle contracts across 7 assets in <2 s.
- `live --mode paper` boots, connects 5 exchange feeds + Polymarket WS, runs ~3 cycles/sec, persists state, shuts down on SIGINT.
- `validate-replay` over a 60 s paper run: 186 evaluations, 0 mismatches.
- State persistence: kill + restart restores `total_pnl`, paper positions, breaker meta.

## Operations

```bash
# Bootstrap a fresh VPS (one-shot)
ssh root@vps "$(cat deploy/setup.sh)"

# Deploy + restart (build, scp, systemctl restart)
bash deploy/deploy.sh user@vps --enable-service --mode paper

# Logs
ssh vps 'journalctl -u polymomentum-engine -f'

# Kill switch
ssh vps 'touch /tmp/polymomentum/KILL'

# Resume after kill switch / breaker trip
ssh vps 'rm -f /tmp/polymomentum/KILL && \
         sqlite3 /opt/polymomentum/logs/candle/state.db \
                 "DELETE FROM meta WHERE key=\"candle_breaker_tripped\"" && \
         sudo systemctl restart polymomentum-engine'
```

## Risks remaining

| Risk | Status |
|---|---|
| Live execution untested | Code-complete; needs operator-driven $1 trade with `--i-understand-live` once paper is validated. |
| No backtest harness in Rust | Strategy tuning blocked on Phase 6 follow-up OR live paper iteration only. |
| Cooldown enforcement | Not wired in the Rust pipeline (Python had per-event_id cooldowns). Add when needed. |
| Paper fill is top-of-book + flat slippage | Not full BookWalk against captured L2. Known gap, low priority for the strategy bias we have. |

## What stays out

- Anything that depended on Python (backtest harness, `pmxt_v2_fetch.py`, `validate_paper_vs_v2.py`, `oracle_backfill.py`). Resurrect from git history if/when porting.
- The legacy IPC bridge — Python is gone, single-process is sufficient.
- MEXC WS (silent stalls + reconnect spam). Three BTC sources + Deribit IV is enough.

## Tracking

This document is the authoritative status. Memory at `~/.claude/projects/.../memory/project_state.md` mirrors it.
