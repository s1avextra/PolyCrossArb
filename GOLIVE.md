# Go-Live Roadmap

Current state: $6.03 USDC.e + 5.38 POL in wallet.

---

## Phase 1: Validate Paper (Days 1-3)

### 1a. Start data collector (Day 1)
```bash
nohup uv run python scripts/run_collector.py > logs/collector.log 2>&1 &
```
This records BTC ticks, candle contracts, AND Polymarket order book bid/ask every 5 seconds. Runs 24/7.

### 1b. Run candle pipeline in paper mode (Days 1-3)
```bash
uv run python scripts/run_production.py --mode paper --bankroll 6
```
Validates the new vol-normalized momentum + 3-zone entry timing with real Polymarket prices. Watch for:
- Trades per hour (expect 1-4 on 5-min windows)
- Win rate (target: >65%)
- Realized P&L vs expected P&L (gap = slippage model error)
- Zone distribution (should see early/primary trades, not just late)

### 1c. Run arb scanner in background (Days 1-3)
```bash
uv run python scripts/run_ws_pipeline.py --mode paper
```
Paper-tracks cross-market arb opportunities. Note how many underpriced vs overpriced appear. Log the execution_strategy field.

**Go/no-go**: After 3 days, check `logs/candle_trades.jsonl` and session logs. Need >50 resolved paper trades with realized WR >60% and positive P&L.

---

## Phase 2: First Live Trades — Candle Strategy (Days 4-7)

### 2a. Small live candle trading
```bash
uv run python scripts/run_production.py --mode live --bankroll 5
```
- Max $1/trade (bankroll $5 → position sizing auto-caps at $1)
- Circuit breaker: auto-stops if WR <65% after 20 trades or drawdown >30%
- Run for 4-8 hours at a time, review results

### 2b. Validate execution quality
Check `logs/sessions/` for:
- Fill rate (target: >90%)
- Avg slippage vs paper (should be within 2x)
- Fill time (target: <10s)
- Any rejected orders (log reasons)

**Go/no-go**: After 50+ live trades, realized P&L should be positive and within 50% of paper P&L.

---

## Phase 3: Enable On-Chain Split/Merge (Days 7-10)

### 3a. One-time setup
```bash
uv run python -c "
from polymomentum.execution.onchain import OnChainExecutor
oc = OnChainExecutor()
# Approve USDC.e for ConditionalTokens contract (unlimited, one-time)
oc.approve_usdc('0x4D97DCd97eC945f40cF65F87097ACe5EA0476045')
print('Approved')
"
```

### 3b. Manual test: split + sell
Find an overpriced event via scan_once.py. Manually:
1. Call `split_position(condition_id, 1.0, num_outcomes)` — costs $1.00
2. Verify token balances via `get_token_balance(token_id)`
3. Sell tokens manually on polymarket.com UI
4. Verify USDC.e recovered

### 3c. Manual test: buy + merge
Find an underpriced event. Manually:
1. Buy $0.30-0.50 of each YES token on polymarket.com
2. Call `merge_positions(condition_id, amount, num_outcomes)`
3. Verify USDC.e returned

### 3d. Enable in config
```
ENABLE_ONCHAIN_EXECUTION=true
```

### 3e. Run arb pipeline with hybrid execution
```bash
uv run python scripts/run_ws_pipeline.py --mode live
```
Start with $2-3 max per arb trade. Monitor the first 5-10 hybrid trades carefully.

**Go/no-go**: 5+ successful hybrid trades (both split+sell and buy+merge). No stuck tokens, no failed merges.

---

## Phase 4: Scale Up (Days 10-21)

### 4a. Increase position sizes
- Candle: raise bankroll to full wallet balance
- Arb: raise max_position_per_market_usd gradually ($5 → $10 → $20)

### 4b. Run both strategies concurrently
```bash
uv run python scripts/run_combined.py --mode live
```
Shared risk manager prevents overcommitting across strategies.

### 4c. Deposit more capital
Once both strategies show consistent profit over 1 week:
- Deposit additional USDC.e to wallet via polymarket.com bridge
- Target: $50-100 working capital
- At $50 bankroll: candle trades $5/trade, arb trades $10-20/trade

### 4d. Collect enough data for proper backtest
After 2 weeks of collector data:
- Run backtest with REAL Polymarket bid/ask prices (from `candle_books_*.csv`)
- Validate the BS market price model assumptions
- Re-calibrate mm_lag_seconds if needed

---

## Phase 5: Optimization (Days 21-45)

### 5a. Fit logistic regression calibration
Using 2+ weeks of live trade data:
- Features: z_score, time_factor, consistency, reversion_count
- Target: actual win/loss outcome
- Replace dampened confidence with calibrated probability
- Validate with walk-forward reliability diagrams

### 5b. Implement cross-exchange lead-lag signal
- Compute Binance vs {Bybit, OKX, MEXC} divergence
- Add as independent feature to the logistic model
- Backtest on collected data

### 5c. Kelly-based position sizing
Replace step-function sizing with:
```
f = (p * b - q) / b  where b = (1-price)/price
```
Ties position size to calibrated probability, not bankroll buckets.

### 5d. Oracle resolution monitoring
- Subscribe to UMA oracle events
- Detect resolution 1-5 min before CLOB
- Auto-trade correlated markets that haven't adjusted

---

## Key Metrics to Track

| Metric | Target | Alert If |
|---|---|---|
| Candle win rate | >65% | <55% over 30 trades |
| Candle Sharpe | >0.3 | <0.1 |
| Arb fill rate | >90% | <70% |
| Merge success rate | 100% | Any failure |
| Paper vs live gap | <2x | >3x |
| Price sources | ≥3 | <2 (auto-halts) |
| Circuit breaker | not tripped | tripped |

## Risk Limits

| Parameter | Value | Why |
|---|---|---|
| Max per candle trade | min($10, 10% bankroll) | Single trade can't kill bankroll |
| Max per arb trade | min($20, 20% bankroll) | Arb legs need capital headroom |
| Max total exposure | 80% bankroll | Reserve for gas + emergencies |
| Circuit breaker WR | 65% min (20+ trades) | Auto-stop before drawdown spirals |
| Circuit breaker DD | 30% max | Hard cap on losses |
| Min price sources | 2 | Don't trade on single exchange data |
| Confidence dead zone | 80-90% | Known money-losing bucket |
