//! Parameter sweep over captured paper-mode JSONL.
//!
//! No L2 replay (the PMXT v2 loader was deleted with the Python tree). The
//! sweep replays each session's `signal.evaluation` events through
//! `decide_candle_trade` with a grid of `ZoneConfig` variants and computes
//! synthetic P&L using the captured `resolution.resolved` events as ground
//! truth.
//!
//! Caveats:
//! - The hypothetical fill price is whatever `up_price` / `down_price` was on
//!   the book at the chosen evaluation tick — we can't model how the book
//!   would have moved if the bot had actually placed an order earlier or
//!   later. Top-of-book + flat slippage is the same model `paper_fill` uses
//!   live, so live-vs-sweep parity holds for `traded=true` cases.
//! - "Insufficient data" warnings are emitted when a variant trades < 30
//!   resolved positions — the paper sample is meaningless below that.

pub mod replay;
pub mod strategy;

use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::sweep::replay::{run_strategy, EvaluationRow, ResolutionRow};
use crate::sweep::strategy::Strategy;

#[derive(Debug, Default, Clone)]
pub struct SweepRun {
    pub strategy_name: String,
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub realized_pnl: f64,
    pub total_fees: f64,
    pub avg_entry_price: f64,
    pub by_zone: std::collections::BTreeMap<String, ZoneStats>,
    pub by_asset: std::collections::BTreeMap<String, ZoneStats>,
    pub insufficient_data: bool,
}

impl SweepRun {
    pub fn win_rate(&self) -> f64 {
        let resolved = self.wins + self.losses;
        if resolved == 0 { 0.0 } else { self.wins as f64 / resolved as f64 }
    }

    pub fn pnl_per_trade(&self) -> f64 {
        if self.trades == 0 { 0.0 } else { self.realized_pnl / self.trades as f64 }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ZoneStats {
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub pnl: f64,
}

impl ZoneStats {
    pub fn win_rate(&self) -> f64 {
        let r = self.wins + self.losses;
        if r == 0 { 0.0 } else { self.wins as f64 / r as f64 }
    }
}

/// Top-level sweep entry point. Reads each JSONL path, builds the per-cid
/// timelines, then runs every strategy.
pub fn run_sweep(
    jsonl_paths: &[PathBuf],
    strategies: &[Strategy],
    bankroll_usd: f64,
    insufficient_threshold: u64,
) -> Result<Vec<SweepRun>> {
    let (evaluations, resolutions) = load_session_logs(jsonl_paths)?;
    let mut results = Vec::new();
    for strat in strategies {
        let mut run = run_strategy(&evaluations, &resolutions, strat, bankroll_usd);
        run.insufficient_data = run.trades < insufficient_threshold;
        results.push(run);
    }
    Ok(results)
}

fn load_session_logs(paths: &[PathBuf]) -> Result<(Vec<EvaluationRow>, Vec<ResolutionRow>)> {
    use std::io::BufRead;
    let mut evals = Vec::new();
    let mut resolves = Vec::new();
    for p in paths {
        let f = std::fs::File::open(p)
            .with_context(|| format!("open session log {}", p.display()))?;
        let reader = std::io::BufReader::new(f);
        for line in reader.lines().map_while(|l| l.ok()) {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let v: serde_json::Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let cat = v.get("cat").and_then(|x| x.as_str()).unwrap_or("");
            let typ = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
            match (cat, typ) {
                ("signal", "evaluation") => {
                    if let Some(row) = EvaluationRow::from_json(&v) {
                        evals.push(row);
                    }
                }
                ("resolution", "resolved") => {
                    if let Some(row) = ResolutionRow::from_json(&v) {
                        resolves.push(row);
                    }
                }
                _ => {}
            }
        }
    }
    // Stable ordering for deterministic replay.
    evals.sort_by(|a, b| a.ts_ms.cmp(&b.ts_ms));
    Ok((evals, resolves))
}

pub fn render_table(runs: &[SweepRun]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    writeln!(
        &mut out,
        "{:<24} {:>7} {:>7} {:>7} {:>7} {:>9} {:>11} {:>9}",
        "strategy", "trades", "wins", "losses", "WR%", "PnL", "PnL/trade", "fees"
    )
    .unwrap();
    writeln!(&mut out, "{}", "─".repeat(86)).unwrap();
    for r in runs {
        let flag = if r.insufficient_data { " *" } else { "" };
        writeln!(
            &mut out,
            "{:<22}{} {:>7} {:>7} {:>7} {:>6.1}% {:>+8.2} {:>+10.3} {:>9.4}",
            r.strategy_name,
            flag,
            r.trades,
            r.wins,
            r.losses,
            100.0 * r.win_rate(),
            r.realized_pnl,
            r.pnl_per_trade(),
            r.total_fees,
        )
        .unwrap();
    }
    if runs.iter().any(|r| r.insufficient_data) {
        writeln!(&mut out).unwrap();
        writeln!(&mut out, "* = sample below significance threshold; treat as directional only").unwrap();
    }
    out
}

pub fn render_zone_breakdown(runs: &[SweepRun]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for r in runs {
        if r.by_zone.is_empty() {
            continue;
        }
        writeln!(&mut out, "\n{} — by zone:", r.strategy_name).unwrap();
        writeln!(
            &mut out,
            "  {:<10} {:>7} {:>7} {:>7} {:>7} {:>9}",
            "zone", "trades", "wins", "losses", "WR%", "PnL"
        )
        .unwrap();
        for (zone, stats) in &r.by_zone {
            writeln!(
                &mut out,
                "  {:<10} {:>7} {:>7} {:>7} {:>6.1}% {:>+8.2}",
                zone, stats.trades, stats.wins, stats.losses, 100.0 * stats.win_rate(), stats.pnl
            )
            .unwrap();
        }
    }
    out
}
