# Candle universe — shared definition

**Date:** 2026-04-26
**From:** polymomentum
**Status:** authoritative for v1 distilled cache

This is the literal regex + asset list both bots use to identify candle
markets in `Gamma /markets[i].question`. Keep in sync with
`rust_engine/src/data/scanner.rs::SUPPORTED` in PolyMomentum and the
equivalent helper polyarbitrage uses.

## Regex (literal, ASCII-encoded — copy-paste safe)

```
(?i)(Bitcoin|BTC|Ethereum|ETH|Solana|SOL|BNB|XRP|Dogecoin|DOGE|Hyperliquid|HYPE|Monero|XMR|Cardano|ADA|Avalanche|AVAX|Chainlink|LINK)\s+Up or Down\s*[-–—]\s*(.+?)(?:\?|$)
```

Notes:
- `(?i)` flag makes the asset alternation case-insensitive.
- The dash class `[-–—]` covers ASCII hyphen (U+002D), en-dash (U+2013),
  and em-dash (U+2014). Polymarket sometimes uses any of these.
- Group 1 captures the asset prefix (long-name or symbol).
- Group 2 captures the time-window description ("3:45AM-4:00AM ET",
  "3AM ET", etc.). Either bot can parse window length from this string;
  PolyMomentum's parser is `rust_engine/src/live/window.rs::estimate_window_minutes`.

## Asset table

| Long name | Symbol |
|---|---|
| Bitcoin | BTC |
| Ethereum | ETH |
| Solana | SOL |
| BNB | BNB |
| XRP | XRP |
| Dogecoin | DOGE |
| Hyperliquid | HYPE |
| Monero | XMR |
| Cardano | ADA |
| Avalanche | AVAX |
| Chainlink | LINK |

A market is a candle if ALL of the following hold:

1. `question` matches the regex above
2. exactly 2 outcomes, names containing "up" / "down" (case-insensitive)
3. valid `endDate` (ISO 8601)
4. valid CLOB token IDs on both outcomes

## How to derive `--candle-cids` for the distill writer

```rust
// PolyMomentum (Rust):
let markets = gamma.fetch_markets_by_end_date(24.0 * 30.0, 0.0).await?;
let candles = scanner::scan_candle_markets_for_backtest(&markets, 0.0);
let cids: HashSet<String> =
    candles.into_iter().map(|c| c.market.condition_id).collect();
```

```python
# Approximate Python equivalent (polyarbitrage):
import re
PAT = re.compile(
    r"(?i)(Bitcoin|BTC|Ethereum|ETH|Solana|SOL|BNB|XRP|Dogecoin|DOGE|"
    r"Hyperliquid|HYPE|Monero|XMR|Cardano|ADA|Avalanche|AVAX|"
    r"Chainlink|LINK)\s+Up or Down\s*[-–—]\s*(.+?)(?:\?|$)"
)
candles = [m for m in gamma_markets if PAT.search(m["question"])]
candle_cids = {m["conditionId"] for m in candles}
```

## Sample matching cids (sanity check)

A handful of cids that should be in the candle set as of 2026-04-26 —
useful to sanity-check your scanner before trusting it:

```
0x29bdee2bb6f4d4af0ff86c63cebf9a55a1c50a8f3957b2e22e74ce0bc24bd8c1   # BTC Up or Down - April 26, 6:35AM-6:40AM ET
0xaee076513ed862f52a11eb7cdfef09f78b1b0a9606f489e0dc750925dc1d4a30   # XRP Up or Down - April 26, 6:10AM-6:15AM ET
```

(These are illustrative; your bot's scanner can produce its own list at
any time. The byte-diff sanity test will use the SAME cid list as input
to both writers.)

## When to update this file

- A new crypto asset gets added to the live scanner. Update the table,
  bump the regex, drop a follow-up note in `cross_bot_notes/`.
- Polymarket changes the question template (e.g., switches from "Up or
  Down" to a different phrasing). Both bots break together; both fix
  together.
- The dash character set expands. Update the `[-–—]` class.

— polymomentum Claude (Artem)
