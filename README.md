# PolyCrossArb

Quantitative cross-market arbitrage system for Polymarket prediction markets.

Detects logical pricing inconsistencies across interdependent markets (e.g. "Who will win X?" split into multiple binary YES/NO markets), computes optimal trade sizes via LP solver, and executes through paper or live trading.

## How it works

1. **Fetch** all active markets (~25,000+) from Polymarket's Gamma API
2. **Group** markets by event — Polymarket splits multi-outcome events into separate binary YES/NO markets sharing the same event
3. **Detect** arbitrage — for mutually exclusive events (`neg_risk=true`), the sum of YES prices should equal 1.0. Any deviation is guaranteed profit
4. **Solve** optimal positions using LP (PuLP/CBC) with executable order book prices, correct neg_risk collateral, and all fees deducted
5. **Execute** trades (paper or live) with dynamic bankroll management, exposure limits, and cooldowns

## Two pipeline modes

### REST Pipeline (polling)
Scans all markets every 30 seconds. Simpler, good for getting started.
```bash
.venv/bin/python scripts/run_pipeline.py --mode paper --cycles 10 --interval 30
```

### WebSocket Pipeline (event-driven) — recommended
Subscribes to real-time price changes. Detects arbs within <1 second of a price move.
```bash
.venv/bin/python scripts/run_ws_pipeline.py --mode paper --duration 600
```

| | REST Pipeline | WebSocket Pipeline |
|---|---|---|
| Latency | ~200s per cycle | <1s reaction time |
| Market coverage | 25,000+ per cycle | Top 100 arb candidates monitored |
| Data freshness | Stale between cycles | Real-time order book updates |
| Capital events | Polled | Instant resolution detection |
| Best for | Scanning, backtesting | Live trading |

## Quick start

```bash
# Setup
cd PolyCrossArb
cp .env.example .env
uv venv && uv pip install -e ".[dev]"

# Scan for arbs right now (no API keys needed)
.venv/bin/python scripts/scan_once.py --min-margin 0.02 --top 20

# Paper trade with WebSocket (recommended)
.venv/bin/python scripts/run_ws_pipeline.py --mode paper --duration 600

# Paper trade with REST polling
.venv/bin/python scripts/run_pipeline.py --mode paper --cycles 10

# Backtest
.venv/bin/python scripts/backtest.py collect --interval 300 --max 288
.venv/bin/python scripts/backtest.py replay --snapshot-dir snapshots/

# Run tests
.venv/bin/pytest tests/ -v
```

## API keys and .env

**Paper trading and scanning require NO API keys.** Market data is public.

For live trading, see **[docs/SETUP_API_KEYS.md](docs/SETUP_API_KEYS.md)** or run:
```bash
.venv/bin/python scripts/derive_api_keys.py
```

### Configuration

```env
# Capital (adjust to your bankroll)
BANKROLL_USD=100.0
MAX_TOTAL_EXPOSURE_USD=80.0       # 80% of bankroll
MAX_POSITION_PER_MARKET_USD=20.0  # 20% per event
KELLY_FRACTION=0.25               # Quarter Kelly (safest compounding)

# Detection thresholds
MIN_ARB_MARGIN=0.02               # 2%+ deviations only
MIN_PROFIT_USD=0.10               # Skip trades under $0.10
COOLDOWN_SECONDS=120
SCAN_INTERVAL_SECONDS=30          # REST pipeline only
```

## Project structure

```
src/polycrossarb/
├── config.py              # Settings from .env
├── pipeline.py            # REST polling orchestrator
├── pipeline_ws.py         # WebSocket event-driven orchestrator
├── data/
│   ├── client.py          # Polymarket Gamma + CLOB REST client
│   ├── models.py          # Market, Outcome, OrderBook dataclasses
│   └── websocket.py       # Real-time WebSocket client
├── graph/
│   ├── screener.py        # Rule-based dependency detection
│   └── dependency.py      # NetworkX dependency graph
├── arb/
│   ├── detector.py        # Single + cross-market arb detection
│   ├── polytope.py        # Marginal polytope constraints
│   └── projection.py      # KL/Bregman projection
├── solver/
│   ├── linear.py          # Tier 1: LP solver (PuLP/CBC)
│   └── frank_wolfe.py     # Tier 2: Frank-Wolfe + ILP oracle
├── execution/
│   ├── fees.py            # Live fee rates (API + fallback)
│   ├── sizing.py          # Fractional Kelly position sizing
│   ├── liquidity.py       # Order book depth & slippage
│   └── executor.py        # Paper + live trade execution
└── risk/
    └── manager.py         # Dynamic bankroll, exposure, P&L

scripts/
├── scan_once.py           # One-shot arb scanner
├── run_pipeline.py        # REST pipeline runner
├── run_ws_pipeline.py     # WebSocket pipeline runner
├── backtest.py            # Snapshot collector + replayer
└── derive_api_keys.py     # Polymarket API key generator
```

## Key concepts

### Cross-market arbitrage on neg_risk events

Polymarket's binary markets are efficiently priced (YES + NO = 1.0). But multi-outcome events are split into separate markets that can drift:

| Market | YES price |
|--------|-----------|
| "Will Candidate A win?" | 0.40 |
| "Will Candidate B win?" | 0.35 |
| "Will Candidate C win?" | 0.30 |
| **Sum** | **1.05** |

Since exactly one wins (confirmed by `neg_risk=true`), selling YES on all three pockets $0.05 per set.

### Cost-aware solver

Every trade accounts for:
- **Order book executable prices** (best bid for selling, best ask for buying)
- **Spread costs** — arbs that don't survive the bid-ask spread are rejected
- **Trading fees** — live from Polymarket API, auto-updates when rates change
- **Gas fees** — live from Polygon gas station + POL price
- **Correct neg_risk collateral** — selling YES requires `(1-price)` per share, not flat $1

### Dynamic bankroll

The system tracks `effective_bankroll = initial + realized_pnl`. Profits compound automatically — bigger bankroll means bigger positions and faster growth. Losses shrink exposure to protect capital.

### Capital turnover optimization

Trades are scored by **profit per dollar per day**, not just total profit. Faster-resolving events compound bankroll growth because capital is freed sooner.

### Fractional Kelly (0.25x)

Position sizing uses quarter-Kelly. Full Kelly maximises growth but has 50% chance of 50% drawdown. At 0.25x Kelly, growth rate is ~94% of full Kelly but max drawdown drops to ~12%.

### Early exit

When an arb corrects (prices converge back to sum=1.0), all positions in that event are closed automatically to free capital for new opportunities. Without this, capital can be locked for months waiting for event resolution.

### Portfolio-aware allocation

If a new arb shares market outcomes with existing positions, its allocation is halved to prevent correlated risk concentration.

### Crash recovery

All state (positions, P&L, fees, cooldowns) is persisted to `logs/state.json` after every trade. On restart, state is restored automatically — no trades lost.

### Live execution safety

- Leg-by-leg with abort: if any leg fails to fill (95%+ required), all remaining orders are cancelled
- Order ID validation: rejects trades if exchange doesn't return a valid order ID
- Fill polling with timeout: 30s per leg, cancels on timeout
- Input validation: config values, WebSocket data, API responses all validated

## Testing

```bash
.venv/bin/pytest tests/ -v   # 45 tests, ~2s
```

Covers: arb detection, polytope constraints, KL projection, LP solver, Kelly sizing, execution probability.

## Recommended workflow

1. **Scan** — `scan_once.py` to see current arb landscape
2. **Paper trade (WS)** — `run_ws_pipeline.py --mode paper` for 1+ week
3. **Backtest** — `backtest.py collect` then `replay` on historical data
4. **Go live** — set up API keys ([docs/SETUP_API_KEYS.md](docs/SETUP_API_KEYS.md)), switch to `--mode live`
5. **Scale** — increase `BANKROLL_USD` after consistent positive P&L
