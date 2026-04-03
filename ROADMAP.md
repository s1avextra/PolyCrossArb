# PolyCrossArb — Production Roadmap

## Where We Are (v1.0)

Built from scratch in one session. Three parallel strategies:

| Strategy | Status | Edge | Realistic P&L |
|----------|--------|------|---------------|
| Neg_risk cross-market arb | Working, live-tested | $0.07/trade after spreads | ~$1/day |
| Weather information-edge | Working paper mode | $5-15/trade at 90%+ confidence | ~$50/day estimated |
| Crypto cross-exchange | Working paper mode | Needs volatility model fix | Unknown |

**Current bankroll**: ~$57 in existing positions + $0.69 USDC.e cash.

**Lessons learned the hard way**:
- Paper mode lied — didn't simulate balance, fees, or execution constraints
- Neg_risk positions can't be sold via simple CLOB orders
- Multi-leg arbs fail because legs execute sequentially, not atomically
- Stale contracts at 0.1¢ with empty books are traps, not opportunities
- The Polymarket UI shows positions differently than the CLOB API reports them

---

## Phase 1: Fix What's Broken (Week 1)

### 1.1 Volatility Model for Crypto
The Black-Scholes model uses 50% flat vol — wrong for crypto. Fix:
- Pull implied volatility from Deribit options API (free)
- Use volatility smile/skew for different strikes
- Add jump-diffusion model (Merton) for fat tails
- **Test**: compare model fair values to actual resolution outcomes on past contracts

### 1.2 Weather Confidence Calibration
The predictor says 70-97% confidence but we haven't validated against actual outcomes.
- Collect 1 week of predictions vs actual resolutions
- Calculate Brier score (calibration metric)
- Adjust confidence model to match actual hit rate
- **Target**: Brier score < 0.10 (well-calibrated)

### 1.3 Execution Reliability
- Wire weather pipeline to LiveExecutor (currently paper-only)
- Wire crypto pipeline to LiveExecutor
- Add order status reconciliation (check wallet balance after each trade)
- Add transaction receipt verification (confirm on-chain)

### 1.4 Position Resolution Tracking
- Auto-detect when positions resolve (WebSocket market_resolved events)
- Calculate actual P&L per resolved position
- Compare actual vs predicted P&L
- **Deliverable**: daily P&L report showing predicted vs actual

---

## Phase 2: Data Infrastructure (Week 2-3)

### 2.1 Historical Data Collection
- Run snapshot collector 24/7: `backtest.py collect --interval 60`
- Store: market prices, order books, weather temps, BTC prices
- Target: 2 weeks of tick-level data for backtesting
- Use SQLite or DuckDB instead of JSON files (currently JSONL)

### 2.2 Resolution Source Database
- For weather: identify exact resolution source per city (NOAA station, weather.gov URL)
- For crypto: track which oracle Polymarket uses for BTC price
- Parse market descriptions to extract resolution criteria
- Eliminate ambiguity — know EXACTLY what we're betting on

### 2.3 Order Book Depth History
- Record full order book snapshots (not just best bid/ask)
- Track how quickly books refill after our orders
- Measure our actual market impact
- **Use**: calibrate execution simulator for realistic paper trading

---

## Phase 3: ML Mispricing Predictor (Week 3-5)

### 3.1 Feature Engineering
From order flow data, compute:
- Order book imbalance (bid depth / ask depth)
- Trade flow direction (net buy vs net sell in last N seconds)
- Price momentum (rate of price change)
- Cross-market correlation (how fast Polymarket follows exchange moves)
- Time-of-day patterns (when are mispricings most common?)
- Volume spikes (unusual activity = someone knows something)

### 3.2 Model Training
- **Target**: predict when YES price sum will deviate from 1.0 in the next 30 seconds
- **Model**: LightGBM / XGBoost (fast inference, handles tabular data well)
- **Training data**: 2 weeks of historical snapshots
- **Features**: order book state + external prices + time features
- **Label**: did a profitable edge appear in the next 30s? (binary classification)
- **Metric**: precision@recall=0.5 (we want high precision — every trade should be right)

### 3.3 Deployment
- Run model inference every 2 seconds
- When model predicts edge > 0.7 probability, trigger trade evaluation
- Monitor prediction accuracy in real-time
- A/B test: model-triggered trades vs rule-based trades

---

## Phase 4: Production Infrastructure (Week 4-6)

### 4.1 Always-On Deployment
- Deploy on a VPS (Hetzner $5/month, or AWS Lightsail)
- Supervisor/systemd for auto-restart on crash
- Health check endpoint (simple HTTP server showing status)
- Watchdog: if no trades in 1 hour during active market hours, alert

### 4.2 Monitoring & Alerting
- Prometheus metrics: P&L, trade count, latency, error rate
- Grafana dashboard (or simple web dashboard)
- Telegram/Discord alerts for: trades executed, errors, daily P&L summary
- Log aggregation (structured JSON logs → centralized viewer)

### 4.3 Multi-Wallet Support
- Separate wallets for each strategy (weather, crypto, arb)
- Independent bankroll tracking per strategy
- Auto-rebalance between strategies based on performance

### 4.4 Rate Limiting & API Management
- Track API call budgets (Polymarket, weather APIs, Binance)
- Automatic backoff when rate-limited
- Connection pool management for WebSockets
- Graceful degradation (if one source fails, use others)

---

## Phase 5: Strategy Expansion (Week 6-10)

### 5.1 Cross-Platform Arbitrage (Kalshi)
- Add Kalshi API integration (public market data, no auth needed)
- Match events between Polymarket and Kalshi (same question, different platforms)
- Buy cheap side on one, expensive side on other
- Only 2 legs on independent order books — no price impact problem
- **This is what ArbPoly does and it works**

### 5.2 Hybrid Market-Making
- Provide liquidity on overpriced events (maker quotes)
- Earn maker rebates (20-50% of taker fees returned)
- Capture spread + structural edge simultaneously
- Dynamic quote adjustment based on inventory
- Split/merge operations for token inventory management

### 5.3 Event-Driven Trading
- Monitor news feeds (Twitter/X, RSS, official sources)
- When breaking news affects a prediction market, trade before prices adjust
- Example: election result announced → immediately trade all related markets
- Requires NLP for news classification (can use Claude API)

### 5.4 Sports Live Betting
- In-play sports markets on Polymarket
- Odds update slowly compared to sportsbooks
- Connect to live sports data feeds (API-Football, ESPN)
- Trade when live score changes but Polymarket prices lag
- Same architecture as weather (external data → information edge)

---

## Phase 6: Scale & Optimize (Week 10+)

### 6.1 Capital Scaling
- $100 → $1,000: validate strategies at 10x capital
- $1,000 → $10,000: add position limits, diversification requirements
- $10,000+: need deeper liquidity analysis, market impact modeling
- **Key constraint**: liquidity on Polymarket limits position size

### 6.2 Latency Optimization
- Co-locate near Polymarket's infrastructure (Polygon RPC nodes)
- Use dedicated RPC endpoint (Alchemy/QuickNode paid tier)
- Pre-sign transactions (reduce on-chain latency)
- Consider Rust rewrite for the hot path (WebSocket → decision → order)

### 6.3 Portfolio Optimization
- Global LP across all strategies (weather + crypto + arb + MM)
- Kelly-optimal allocation between strategies based on edge and variance
- Correlation-adjusted risk (weather in different cities = uncorrelated)
- Daily rebalancing based on opportunity set

### 6.4 Regulatory Compliance
- Track all trades for tax reporting (cost basis, gains/losses)
- KYC/AML compliance for Polymarket and Kalshi
- Legal review of prediction market trading in your jurisdiction
- Separate business entity if scaling above $10K

---

## Production Checklist (Before Going Live with Real Money)

### Strategy Validation
- [ ] Paper trade each strategy for 7+ days
- [ ] Actual win rate matches predicted win rate (within 5%)
- [ ] P&L matches expected P&L (within 20%)
- [ ] No position has been stuck (unable to exit) for > 24h
- [ ] Maximum drawdown < 15% during paper trading period

### Execution Safety
- [ ] All-or-nothing execution verified (no orphaned legs)
- [ ] Exit probe passes on every trade before committing capital
- [ ] Order book depth check prevents oversized trades
- [ ] $1 minimum order size enforced
- [ ] Volume filter prevents trading ghost markets

### Infrastructure
- [ ] State persists across restarts (state.json tested)
- [ ] Crash recovery works (kill process, restart, positions preserved)
- [ ] API keys rotated and securely stored
- [ ] .env not committed to git (verified)
- [ ] Wallet is dedicated trading wallet (not main wallet)

### Monitoring
- [ ] Log file rotation configured (prevent disk fill)
- [ ] Daily P&L email/alert configured
- [ ] Error alerting configured (Telegram/Discord)
- [ ] Watchdog detects if bot stops running

### Risk Management
- [ ] Per-strategy bankroll limits set
- [ ] Maximum drawdown stop-loss configured
- [ ] Position concentration limits per event/city/asset
- [ ] Cooldown timers working correctly
- [ ] Emergency stop mechanism (kill switch)

---

## Estimated Timeline

| Phase | Duration | Key Deliverable |
|-------|----------|----------------|
| Phase 1: Fix broken | 1 week | Reliable live execution |
| Phase 2: Data infra | 2 weeks | Historical data for backtesting |
| Phase 3: ML predictor | 2 weeks | Model-driven trade signals |
| Phase 4: Production | 2 weeks | Always-on deployment with monitoring |
| Phase 5: Expansion | 4 weeks | Kalshi cross-platform + sports |
| Phase 6: Scale | Ongoing | Capital growth + optimization |

**Total to production-grade**: 6-8 weeks
**Total to institutional-grade**: 3-6 months

---

## Revenue Projections (Conservative)

| Bankroll | Strategy Mix | Daily Trades | Avg Profit/Trade | Daily P&L | Monthly |
|----------|-------------|-------------|------------------|-----------|---------|
| $100 | Weather only | 5-10 | $3-8 | $15-80 | $450-2,400 |
| $500 | Weather + crypto | 10-20 | $5-15 | $50-300 | $1,500-9,000 |
| $5,000 | All strategies | 20-50 | $10-30 | $200-1,500 | $6,000-45,000 |
| $50,000 | All + Kalshi | 50-100 | $20-100 | $1,000-10,000 | $30,000-300,000 |

**Caveats**: These assume strategies maintain their edge. In practice:
- Competition compresses edges over time
- New market makers enter and reduce mispricings
- Platform rule changes can invalidate strategies
- Liquidity limits position sizes at higher capital levels
- Past paper performance does NOT guarantee live results
