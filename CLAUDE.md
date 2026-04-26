
# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

## 5. Parquet & shared-cache rules (multi-tenant VPS)

PolyMomentum shares the multibot VPS with **adgts** and **polyarbitrage**.
The PMXT v2 archive cache (`/opt/shared/pmxt_v2_cache/`) and the distilled
candles cache (`/opt/shared/pmxt_v2_distilled_candles/`) are owned by the
`pmxt-data` group and writable by both polymomentum and polyarbitrage.

### Hard rules (peer-coordinated; mirrored in polyarbitrage's CLAUDE.md)

1. **Never delete a parquet you didn't download.** Convention is "downloader owns it." If your fetch helper just hit the network for a file, you may delete it after processing. Pre-existing files (downloaded by a peer bot) stay.
2. **Never read another tenant's private dirs.** `/opt/polyarbitrage/*`, `/etc/polyarbitrage/*`, their wallet — off-limits. Same the other way for `/opt/polymomentum/*`. Cross-bot coordination goes through `/opt/shared/cross_bot_notes/` only.
3. **Never run two `cargo build --release` concurrently** on the VPS. The box is 2-core; two release builds OOM. Use `nice -n 10 cargo build --release` so peer bot work stays responsive.
4. **Never concurrently scan the same parquet hour from two processes.** Parquet predicate pushdown is single-threaded per file (pyarrow + arrow-rs). Stage your pipeline so each parquet is read at most once per pass.

### Conventions

5. **Always filter at the parquet level.** Use `RowFilter` (Rust `parquet` crate) or `pyarrow.dataset.scanner(filter=…, columns=[…])`. Each hour file is ~330–460 MB compressed, ~700 MB uncompressed, ~86 M rows — full loads will OOM the 2-core box.
6. **Provide a `--delete-after-process`-style flag** for one-shot backfills so callers can choose to keep or drop the parquet. Pre-existing files (rule 1) stay regardless.
7. **Provide a distilled-cache flag** (or env var) for any sweep that re-reads the same window. PolyMomentum's flag: `--cache-dir <dir>` for the per-tenant sidecar (`*.events.bin.gz`), and `PMXT_DISTILLED_DIR` env var or auto-detect of `/opt/shared/pmxt_v2_distilled_candles/` for the cross-bot cache.
8. **Atomic-rename writes for every shared file.** Write to `*.tmp.<pid>` then `rename(2)`. No lockfiles — they bite us on shared volumes.

### Shared distilled candles cache

- Path: `/opt/shared/pmxt_v2_distilled_candles/<hour>.v1.candles.jsonl.gz`
- Schema v1: see `docs/cross_bot_distilled_cache_response.md` (event types `book`, `chg`, `trade`; numeric strings on price/size; f64 elsewhere)
- Writer: `polymomentum-engine distill --input <parquet> [--candle-cids <file>] [--output <path>]`
- Reader contract: missing | corrupt | schema-mismatch → fall back to the per-tenant sidecar, then to a parquet RowFilter scan.
- Candle universe: see `docs/candle_universe.md` (regex + 11 supported assets) — both bots must agree on this set so byte-diff tests pass.

### Coordination notes directory

- `/opt/shared/cross_bot_notes/` — read/write for both bots via `pmxt-data` group.
- Filename convention: `YYYY-MM-DD_<topic>_from_<sender>.md`.
- Mirror every note we write into our own `docs/` so it's git-tracked.
- When you make a peer-visible change (new shared format, new CLI, new convention), drop a `<date>_<topic>_from_polymomentum.md` so future Claude sessions on either side can pick it up without re-discovery.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
