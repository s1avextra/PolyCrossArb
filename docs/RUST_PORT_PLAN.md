# Rust Port Plan

**Decision date:** 2026-04-26
**Direction:** Port the entire codebase from Python to Rust. Live runtime, backtest harness, validators, scripts. Single binary `polymomentum-engine`.

## Why now

Operator decision after the 2026-04-26 paper-vs-v2 validation surfaced ~30-50¢ stale-price bugs in the Python pipeline (scanner refresh interval was 30-120s while the matcher snaps prices in seconds). The bug itself is wiring, not perf — but the operator wants a single-language, predictable-latency stack going forward and accepts the multi-week port cost.

Python paper bot was stopped 2026-04-26 ~10:09 UTC. Healthcheck timer disabled. Rust engine (`polymomentum-rust`, 11 MB) keeps running for its existing role (multi-exchange WS, edge calc, CLOB direct path); we'll extend it to subsume everything Python was doing.

## Scope (audit)

| Layer | Python LOC | Already in Rust | Port work |
|---|---:|---|---:|
| BS fair value | 80 | ✅ `fair_value.rs` (109) | 0 |
| EIP-712 signing | — | ✅ `signing.rs` (427) | 0 |
| Multi-exchange spot WS | 598 | ✅ `exchange.rs` (351, hardened today) | 200 (port aggregator) |
| Polymarket book WS | 405 | ✅ `polymarket_ws.rs` (153) | 100 (extend for resub on contract refresh) |
| CLOB direct order placement | 386 | ✅ `clob.rs` (318) | 100 (paper fill simulation) |
| IPC bridge | — | ✅ `ipc.rs` (199) | 0 (or remove — single-process now) |
| Latency monitor | — | ✅ `latency.rs` (176) | 0 |
| Decision function | 301 | — | 250 (pure logic, easy port) |
| Momentum detector + EWMA vol | 356 | partially in `edge.rs` | 250 |
| Candle scanner / Gamma client | ~500 | — | 400 |
| Risk manager + SQLite | 653 | — | 500 |
| Session monitor / JSONL | 633 | — | 400 |
| Cycle loop + orchestration | 1236 | partial in `main.rs` (622) | 600 |
| BTC history / parquet readers | 690 | — | 400 |
| L2 backtest engine | 836 (l2_replay + replay_engine) | — | 700 |
| Strategy harness | 379 | — | 300 |
| Candle resolver | 285 | — | 200 |
| PMXT v2 loader | 304 | — | 200 |
| CTF on-chain reader | 90 | — | 80 |
| Oracle backfill / validator scripts | ~600 | — | 400 |
| Test suite (120 tests) | 1931 | 14 tests | 1500 |
| **Total** | **~17,500 LOC** | **3,051 LOC (17%)** | **~6,500 LOC port + 1,500 tests** |

LOC ratio Python:Rust is roughly 1.5:1 for equivalent logic (Rust's static types eliminate validation boilerplate but add type signatures). Estimated final Rust LOC: ~12,000.

## Cargo dependencies to add

| Crate | Purpose |
|---|---|
| `rusqlite` | SQLite for state.db (rust replacement for sqlite3 stdlib) |
| `arrow` + `parquet` | Read PMXT parquet files (v1 + v2 schemas) |
| `tracing` + `tracing-subscriber` | Structured logging (replaces structlog) |
| `tracing-appender` | JSONL log file rotation |
| `clap` | CLI parsing for harness/scripts |
| `chrono-tz` | Timezone-aware candle window parsing ("8:30AM ET") |
| `axum` (optional) | HTTP healthcheck endpoint if we want one |
| `dashmap` | Concurrent maps (book state per token) |
| `prometheus` (optional) | Metrics endpoint if we expose one |

Already have: `tokio`, `tokio-tungstenite`, `serde`, `serde_json`, `reqwest`, `futures-util`, `chrono`, `k256`, `sha3`, `hmac`, `sha2`, `base64`, `rmp-serde`, `rand`, `hex`.

## Phase plan (target: 8-12 weeks at 4-6 focused hours/day)

### Phase 1 — Foundation (1.5 weeks)

Goal: Single Rust binary boots cleanly, connects to all data sources, no trading yet.

- [ ] Cargo.toml: add new deps (rusqlite, arrow, parquet, tracing, etc.)
- [ ] Module layout reshuffle: `src/{config,data,strategy,execution,risk,monitoring,backtest}/`
- [ ] Configuration loader (env vars → typed Config struct, replaces pydantic-settings)
- [ ] Tracing subscriber + JSONL session log writer
- [ ] **Polymarket Gamma client** (REST market discovery) — port `data/client.py`
- [ ] **Candle scanner** — port `crypto/candle_scanner.py` to filter Up/Down crypto candle markets
- [ ] **CTF on-chain reader** — port `data/ctf.py` (raw eth_call, no web3 dep)
- [ ] **Wallet balance reader** — port `execution/wallet.py` (raw eth_call, similar to CTF)
- [ ] Extend `polymarket_ws.rs` to expose `subscribe_tokens` + `get_book` for the cycle loop
- [ ] Smoke test: binary starts, subscribes to ~120 tokens, prints first book updates

### Phase 2 — Strategy core (2 weeks)

Goal: pure decision logic ported and tested. Same outputs as Python for golden-test inputs.

- [ ] Port `crypto/momentum.py` → `src/strategy/momentum.rs` (MomentumDetector, EWMA vol, z-score)
- [ ] Port `crypto/decision.py` → `src/strategy/decision.rs` (decide_candle_trade, ZoneConfig, all 6 skip reasons + EV filter)
- [ ] Port `crypto/fair_value.py` extras (already have BS, just need any wrappers)
- [ ] Port 56 unit tests from `tests/test_candle_decision.py` to Rust
- [ ] Parity validator: feed identical inputs to Python and Rust, assert identical outputs across 1000 random scenarios

### Phase 3 — Risk + monitoring (1.5 weeks)

Goal: state persistence, observability.

- [ ] Port `risk/manager.py` → `src/risk/manager.rs` with `rusqlite`
  - All tables: trades, positions, paper_positions, oracle_pending, meta, state, cooldowns
  - Migration script for existing SQLite (or fresh start — operator's call)
- [ ] Port `monitoring/session_monitor.py` → `src/monitoring/session.rs` with JSONL writer
- [ ] Port `monitoring/alerter.py` (Slack webhook for circuit breaker)
- [ ] Port circuit breaker logic from candle_pipeline (drawdown, WR thresholds)
- [ ] Tests for SQLite round-trips + JSONL schema

### Phase 4 — Live runtime (2 weeks)

Goal: bot runs in paper mode, makes decisions on real-time WS data, persists state, no trades to live CLOB yet.

- [ ] **The cycle loop** — port `crypto/candle_pipeline.py::_scan_loop` (~600 LOC)
  - 10 Hz cycle
  - Per-contract evaluation
  - Paper fill simulation (using polymarket_book real-time best_ask, NOT scanner price)
  - Position sizing (10% of bankroll, $20 cap, vol regime mult)
  - Eager circuit breaker
- [ ] Port `_paper_resolution_loop` (BTC tape vs window close)
- [ ] Port `_oracle_verification_loop` (CTF cross-check)
- [ ] Port `_contract_refresh_loop` (Gamma every 2 min)
- [ ] Port `_monitoring_loop` (15s state snapshots)
- [ ] Wire all loops into `main.rs` `tokio::join!`
- [ ] systemd unit for new binary (replaces both `polymomentum-rust` and `polymomentum-candle`)
- [ ] Deploy + verify on VPS in paper mode

### Phase 5 — Live execution (1 week)

Goal: live mode reuses existing `clob.rs` for order placement.

- [ ] Wire decision output → `clob.rs::place_order` for live mode
- [ ] Maker/taker fallback logic (already in Python executor, port)
- [ ] Fill recording → risk manager
- [ ] Live mode safety: kill switch, balance check, alerter required
- [ ] Live mode test: $1 trade end-to-end on real Polymarket

### Phase 6 — Backtest harness (2 weeks)

Goal: strategy tuning workflow restored. PMXT v2 + L2 replay + harness all in Rust.

- [ ] Port `backtest/pmxt_loader.py` + `pmxt_v2_loader.py` using `parquet` crate
- [ ] Port `backtest/btc_history.py` (BTCHistory with causality guarantee + 6 regression tests)
- [ ] Port `backtest/l2_replay.py` (L2BacktestEngine with flush-before-apply order)
- [ ] Port `backtest/fill_model.py` (OneTick, BookWalk, Maker, Perfect fill models)
- [ ] Port `backtest/candle_resolver.py` (won/lost determination)
- [ ] Port `backtest/strategies.py` (Strategy trait + Baseline/Ewma/Regime variants)
- [ ] Port `backtest/candle_strategy.py` (L2 backtest adapter)
- [ ] Port `scripts/run_strategy_harness.py` → `polymomentum-engine harness ...` subcommand
- [ ] Port `scripts/validate_paper_replay.py` → `polymomentum-engine validate-replay ...`
- [ ] Port `scripts/validate_paper_vs_v2.py` → `polymomentum-engine validate-vs-v2 ...`
- [ ] Port `scripts/oracle_backfill.py` → `polymomentum-engine oracle-backfill ...`

### Phase 7 — Test parity + cutover (1 week)

Goal: Rust binary fully replaces Python in production.

- [ ] Port remaining Python tests (test_btc_history_causality, test_fair_value_parity, test_executor_and_risk, test_signing already in Rust, test_candle_adapter_parity, test_ipc_bridge)
- [ ] **Parity test suite**: side-by-side run Python and Rust against the same PMXT v2 hour, assert identical trade decisions
- [ ] Update deploy.sh to build Rust binary only
- [ ] Update systemd: single `polymomentum.service` replaces `polymomentum-candle` + `polymomentum-rust`
- [ ] Delete `src/polymomentum/` directory (Python source)
- [ ] Delete `tests/` Python tests
- [ ] Delete `pyproject.toml`, `uv.lock`, `.venv/`
- [ ] Update `README.md` to reflect Rust-only project
- [ ] Memory update: Python deprecation complete

## Architecture sketch

```
polymomentum-engine (single binary)
├── subcommands (clap):
│   ├── live --mode paper|live          # main runtime
│   ├── harness --strategies ...        # backtest A/B
│   ├── validate-replay <session.jsonl> # replay validator
│   ├── validate-vs-v2 --since ...      # PMXT v2 cross-check
│   ├── oracle-backfill --cids ...      # CTF batch query
│   └── pmxt-v2-fetch --start ... --end ... # downloader
└── modules:
    ├── config        — env-driven Config struct
    ├── data
    │   ├── gamma     — Polymarket REST market discovery
    │   ├── ctf       — On-chain ConditionalTokens reader
    │   ├── pmxt      — Parquet historical loaders (v1, v2)
    │   └── exchanges — Spot WS aggregator (already in `exchange.rs`)
    ├── strategy
    │   ├── momentum  — MomentumDetector + EWMA vol
    │   ├── decision  — decide_candle_trade pure function
    │   └── fair_value — BS binary pricer (already in `fair_value.rs`)
    ├── execution
    │   ├── clob      — Already in `clob.rs`
    │   ├── signing   — Already in `signing.rs`
    │   ├── fees      — polymarket_fee formula
    │   └── wallet    — On-chain balance reader
    ├── risk
    │   ├── manager   — Bankroll, positions, SQLite
    │   └── circuit_breaker — drawdown / WR halt
    ├── monitoring
    │   ├── session   — JSONL writer
    │   └── alerter   — Slack webhook
    ├── backtest
    │   ├── btc_history     — Causality-guaranteed BTC tape
    │   ├── l2_replay       — L2 event simulator
    │   ├── fill_model      — OneTick/BookWalk/Maker/Perfect
    │   ├── strategies      — Strategy trait
    │   └── harness         — A/B runner with JSON dump
    └── main          — orchestration + subcommand dispatch
```

## Risks + mitigations

| Risk | Mitigation |
|---|---|
| Strategy iteration paused for 8-12 weeks | Operator accepted (per AskUserQuestion 2026-04-26). Paper bot stopped — no PnL accumulating to chase, just code. |
| Subtle decision-logic differences cause silent live losses | Phase 2 parity test runs Python and Rust on identical inputs and asserts equality across 1000+ random scenarios. Cutover in phase 7 only after parity passes. |
| Rust async ergonomics slow down dev | Use `tokio` throughout, follow patterns in existing `exchange.rs`/`polymarket_ws.rs`. No new async runtime. |
| Backtest data parsing bugs (parquet schemas) | Phase 6 includes side-by-side replay against Python to catch divergence. |
| SQLite concurrency in multi-task Rust | Use `tokio::sync::Mutex` around `rusqlite::Connection`. Or `r2d2` connection pool if contention shows. |
| Long Phase = motivation drift | Each phase has concrete deliverables. Ship phase 1 binary first, validate, then iterate. |

## Operational notes

- **Existing `polymomentum-rust` service stays running** during the port. It does the multi-exchange WS feed for the OLD edge strategy. We'll subsume its job in Phase 4.
- **State.db format will likely change** — Rust's rusqlite gives stricter typing. Migration script TBD in Phase 3.
- **VPS deploy gets simpler** — no more `uv pip install`, just `cargo build --release` + `cp` binary. Faster deploys.
- **Memory should drop from ~270 MB (Python) to ~30-50 MB (Rust)**, freeing the VPS for the peer bots.

## What gets deleted at end

- `src/polymomentum/` (entire Python source tree)
- `tests/` (Python test suite)
- `scripts/*.py` (replaced by `polymomentum-engine` subcommands)
- `pyproject.toml`, `uv.lock`, `.venv/`
- `data/local_book.py` (untracked dead code)
- The PMXT v2 Python loader I just wrote (replaced by Rust equivalent)
- The new Polymarket WS book client (`polymomentum/data/polymarket_book.py`) — discarded, port to Rust directly

## What stays

- `rust_engine/` (extended; renamed `crates/` if we want a workspace)
- `deploy/` (modified for Rust-only)
- `docs/` (updated)
- `data/` (PMXT cache, BTC history files — these are data, not code)
- `.github/`, `.gitignore`, `.git/`
- `README.md`, `CLAUDE.md`

## Tracking

Each phase will get a separate session/conversation. Memory will be updated at end of each phase. Estimate maintained as we learn the actual pace.

**Next session:** Phase 1, starting with module reshuffle + Cargo.toml + Gamma client port.
