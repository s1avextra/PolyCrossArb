# Plan: Latency Optimization + PnL Monetization + Testing Framework

## Context

Current signal-to-order latency on the Dublin VPS: **62ms** (11ms WS staleness + 0.02ms decision + **50ms CLOB POST**). The 48h L2 backtest proved that **latency alone does NOT improve PnL** for the current strategy — both 62ms (VPS) and 340ms (MacBook) produce identical trade sets because the MM lag is 5-30 seconds, dwarfing the order placement time.

The real PnL multipliers are **strategic** (new trade zones, cross-asset signals, dynamic sizing), not raw latency. The Rust engine should be pursued for maker fee savings (~$5/day) and future-proofing, but the immediate PnL focus is on strategies that don't require Rust changes.

**Estimated combined uplift from all phases: +$66-86/day** on top of the current ~$55/day (at lz=0.5, $5 position).

## Phase 1: Terminal Zone + Volatility Sizing (Days 1-2) — +$41/day

Pure Python changes to existing code paths. Testable immediately with L2 backtest.

### 1A. Terminal zone trading (+$30/day estimated)

In the final 15-30 seconds of a 5-min candle, BTC outcome is ~90%+ determined but the MM may still show stale prices. Add a "terminal" zone with relaxed thresholds.

**Files:**
- `src/polymomentum/crypto/decision.py` — add `terminal` to `zone_for()` (line 95-101) for `elapsed_pct >= 0.95`; add terminal thresholds to `ZoneConfig`: `terminal_min_confidence=0.55`, `terminal_min_z=0.3`, `terminal_min_edge=0.03`; add terminal branch to `zone_thresholds()` (line 104-116)
- `src/polymomentum/config.py` — add `candle_zone_terminal_*` settings fields
- `src/polymomentum/crypto/candle_pipeline.py` — lower `minutes_left > 0.5` floor to `0.25` for terminal zone only (line 549)
- `src/polymomentum/backtest/candle_strategy.py` — same floor adjustment
- `tests/test_candle_decision.py` — add terminal zone golden tests

### 1B. Volatility regime dynamic sizing (+$11/day estimated)

During high-vol events (FOMC, liquidation cascades), MM lag widens and edge per trade increases. Scale position size with realized vol.

**Files:**
- `src/polymomentum/crypto/momentum.py` — add `VolatilityRegime` enum (`LOW`, `NORMAL`, `HIGH`, `EXTREME`) based on trailing 15-min realized vol vs 24h average
- `src/polymomentum/crypto/candle_pipeline.py` — in `_execute_candle_trade()` (line 697), multiply position by vol-regime multiplier: HIGH=1.5x, EXTREME=2.0x
- `src/polymomentum/config.py` — add `candle_vol_high_multiplier`, `candle_vol_extreme_multiplier`
- `src/polymomentum/risk/manager.py` — circuit breaker must use % drawdown (already does), not absolute dollars

### 1-Validation
- Run L2 backtest on 48h dataset with terminal zone enabled: `scripts/run_l2_sweep.py` with new terminal params
- Compare trade count, WR, PnL, Sharpe vs baseline
- **Gate:** terminal zone WR ≥ 85% on ≥20 trades before deploying to VPS paper mode

## Phase 2: Cross-Asset Lead-Lag Signal (Days 3-5) — +$20-40/day

Use BTC momentum as a LEADING indicator for ETH/SOL candle contracts. ETH/SOL MMs lag even more than BTC MM (lower volume).

**Files:**
- `src/polymomentum/crypto/momentum.py` — add `reference_asset_signal` parameter to `detect()`: when evaluating an ETH/SOL contract, accept a BTC `MomentumSignal` as a cross-asset boost to confidence
- `src/polymomentum/crypto/candle_pipeline.py` — in `_scan_loop()`, when evaluating non-BTC contracts, pass the current BTC momentum signal as the reference; add lead-lag correlation filter (disable when 30s rolling correlation < 0.70)
- `src/polymomentum/crypto/price_feed.py` — already tracks ETH/SOL from 9 exchanges via `_update_asset_price()` (line 86-95)
- `src/polymomentum/crypto/decision.py` — add optional `cross_asset_boost` parameter to `decide_candle_trade()` that lowers zone thresholds by a configurable amount when the reference asset strongly agrees
- `src/polymomentum/backtest/candle_strategy.py` — wire BTC history as reference signal for ETH/SOL adapters
- `rust_engine/src/exchange.rs` — add ETH/USDT and SOL/USDT feeds on Binance and Bybit

### 2-Validation
- Run L2 backtest with `--asset ETH` and `--asset SOL` separately, using BTC as reference signal
- Compare WR to BTC-only baseline
- **Gate:** cross-asset WR ≥ 85% on ≥30 trades; BTC-ETH rolling correlation must be ≥ 0.70 in the test window

## Phase 3: Rust Engine Hot Path (Days 5-10) — latency 62ms → <10ms

Move order placement from Python (py_clob_client, 50ms) to Rust (direct CLOB POST, 3-5ms). This is the infrastructure change that enables Phase 4 (maker fees) and future-proofs against MM speedups.

### 3A. EIP-712 Order Signing in Rust

The Rust `clob.rs` already has pre-warmed HTTP connection pool with TCP nodelay, but it sends orders WITHOUT proper EIP-712 signatures (just API key headers). Polymarket requires signed orders.

**Files:**
- `rust_engine/Cargo.toml` — add `ethers = "2"` (for k256 ECDSA + EIP-712), `hmac`, `sha2`
- `rust_engine/src/signing.rs` (NEW) — implement Polymarket's EIP-712 order signing: domain separator, order struct hash, ECDSA signature. Port from py_clob_client's order_builder. Target: <500µs per signature.
- `rust_engine/src/clob.rs` — replace `OrderRequest` with `SignedOrderRequest` including EIP-712 signature, salt, maker address, nonce; add HMAC-SHA256 request authentication headers (`POLY-TIMESTAMP` + HMAC); add `place_taker_order()` with FOK support alongside existing `place_maker_order()`

### 3B. Unix Domain Socket IPC (replace stdin/stdout)

**Files:**
- `rust_engine/src/ipc.rs` (NEW) — UDS server with length-prefixed msgpack framing. Messages Python→Rust: `ContractUpdate`, `ConfigUpdate`, `RiskUpdate{available_capital}`, `Shutdown`. Messages Rust→Python: `TradeSignal`, `FillReport{order_id, latency_us, fill_price}`, `LatencyReport`.
- `src/polymomentum/ipc/bridge.py` (NEW) — async UDS client mirroring Rust protocol. `connect()`, `send_contracts()`, `send_risk_update()`, `read_fills()`. Heartbeat every 5s; auto-reconnect.
- `rust_engine/src/main.rs` — replace stdin/stdout with UDS; replace 50ms polling loop with event-driven `tokio::select!` across exchange WS + Polymarket WS + UDS + timer fallback (200ms)
- `scripts/run_latency.py` — replace subprocess Popen + stdin with UDS client
- `src/polymomentum/crypto/candle_pipeline.py` — add `use_rust_engine` flag; when True, delegate scan loop to Rust via bridge, keep only resolution/monitoring/contract discovery in Python

### 3C. Polymarket WebSocket L2 Feed in Rust

**Files:**
- `rust_engine/src/polymarket_ws.rs` (NEW) — subscribe to Polymarket book updates for active candle token_ids; on each update, extract MM best bid/ask, compute staleness, feed into edge evaluator
- `rust_engine/src/main.rs` — add Polymarket WS as a new tokio task; trigger edge evaluation on each book update (event-driven, not polled)

### 3-Validation
- `tests/test_signing.py` (NEW): sign 100 random orders in both Python and Rust, compare byte-for-byte
- Submit a $1 test order signed by Rust to real CLOB, verify acceptance
- `scripts/benchmark_latency.py` (NEW): inject synthetic price, measure signal-to-POST against mock CLOB server, target <5ms
- Run paper mode with Rust executor for 24h, compare with Python executor on same data
- **Gate:** Rust-signed order accepted by CLOB; signal-to-order p99 < 10ms; zero trade discrepancies between Rust and Python paths over 24h

## Phase 4: Maker Fee Optimization (Days 10-12) — +$5/day

With sub-10ms order placement, post GTC maker orders (0% fee) instead of crossing as taker (7.2% fee). Saves ~$0.09 per $5 trade.

**Files:**
- `src/polymomentum/execution/executor.py` — add maker mode to `SingleLegExecutor`: post at `best_ask - 1 tick`, set 3-5s fill timeout, fallback to taker if unfilled
- `rust_engine/src/edge.rs` — emit signals with `order_type` field ("maker" for normal, "taker_fok" for terminal-zone high-urgency)
- `rust_engine/src/clob.rs` — existing `place_maker_order()` already uses GTC; add `place_taker_order()` with FOK
- `src/polymomentum/config.py` — add `candle_prefer_maker: bool = True`
- `src/polymomentum/backtest/fill_model.py` — add `MakerFillModel`: fills at limit price with 65% probability, taker fallback with 35%

### 4-Validation
- Run L2 backtest comparing MakerFillModel vs OneTickTakerFillModel
- Live A/B test: 50% maker, 50% taker for 48h paper mode
- **Gate:** maker fill rate ≥ 60%; net fee savings positive after accounting for missed fills

## Phase 5: Testing & Monitoring Framework (Days 12-15)

### 5A. Paper-live parity verification

**Files:**
- `src/polymomentum/crypto/candle_pipeline.py` — add `_compare_predictions()`: for each Rust trade signal, also run Python `decide_candle_trade()`, log agreement
- `src/polymomentum/monitoring/session_monitor.py` — add `record_latency_breakdown()` (signal_detect_us, sign_us, post_us, server_us, total_us); add `record_comparison()` (paper prediction vs actual outcome)
- `tests/test_paper_live_parity.py` (NEW) — replay recorded session through both paths, verify identical decisions

### 5B. Latency measurement + alerting

**Files:**
- `rust_engine/src/latency.rs` — add per-stage timestamps: price_read → edge_calc → signing → http_post → server_ack; add per-strategy buckets
- `src/polymomentum/monitoring/alerter.py` — add `alert_latency_degradation()` triggered when p99 > 20ms for 5 consecutive 30s windows
- `scripts/benchmark_latency.py` (NEW) — end-to-end latency benchmark against mock CLOB

### 5C. Stress testing

- `tests/test_ipc_bridge.py` (NEW) — 1000 messages/s UDS throughput, reconnection, backpressure
- Concurrent signal test: 10 simultaneous contracts fire signals, verify all 10 orders placed within 50ms total

## Implementation order rationale

**Phases 1-2 FIRST** (strategic PnL, +$41-81/day) because they:
- Don't require Rust changes
- Can be validated immediately with the existing L2 backtest
- Deliver the largest PnL uplift per engineering hour
- Work at ANY latency (62ms or 5ms)

**Phase 3 SECOND** (Rust engine) because it:
- Is the most complex engineering work
- Has the hardest testing requirement (signing correctness)
- Enables Phase 4 but doesn't directly generate PnL

**Phases 4-5 LAST** because they:
- Depend on Phase 3
- Are smaller PnL increments
- Are production-hardening, not revenue-generating

## Risk summary

| Risk | Impact | Mitigation |
|---|---|---|
| Terminal zone admits losers at relaxed thresholds | Drawdown | Tighter circuit breaker for terminal trades (max 5% of bankroll) |
| Cross-asset correlation breaks during idiosyncratic events | Losses on ETH/SOL trades | Dynamic correlation filter: disable when 30s correlation < 0.70 |
| Rust EIP-712 signing produces invalid signatures | Can't trade | Keep Python executor as fallback; test with $1 orders first |
| UDS bridge drops messages under load | Missed trades or phantom trades | Heartbeat + audit trail; Rust logs all trades to local file as backup |
| Volatility sizing amplifies losses in whipsaws | Larger drawdown | Circuit breaker uses % drawdown (already implemented) |
| Maker orders don't fill in time | Missed trades | 3-5s timeout with automatic taker fallback |

## Verification plan

After each phase:
1. Run `pytest tests/ -q` — all tests pass (currently 131)
2. Run L2 backtest on 48h dataset with new params — compare PnL/WR/Sharpe to baseline
3. Deploy to VPS paper mode for 24h — verify live behavior matches backtest predictions
4. Check Telegram alerts fire correctly on circuit breaker / latency events
5. Review session JSONL for any anomalies

## New environment variables

```env
# Phase 1
CANDLE_ZONE_TERMINAL_MIN_CONFIDENCE=0.55
CANDLE_ZONE_TERMINAL_MIN_Z=0.3
CANDLE_ZONE_TERMINAL_MIN_EDGE=0.03
CANDLE_VOL_HIGH_MULTIPLIER=1.5
CANDLE_VOL_EXTREME_MULTIPLIER=2.0

# Phase 2
CANDLE_CROSS_ASSET_ENABLED=0
CANDLE_CROSS_ASSET_MIN_CORRELATION=0.70
CANDLE_CROSS_ASSET_CONFIDENCE_BOOST=0.10

# Phase 3
IPC_MODE=uds
IPC_SOCKET_PATH=/tmp/polymomentum/engine.sock
SCAN_MODE=hybrid
CLOB_DIRECT=1

# Phase 4
CANDLE_PREFER_MAKER=0
CANDLE_MAKER_TIMEOUT_S=3

# Phase 5
LATENCY_ALERT_P99_MS=20
```
