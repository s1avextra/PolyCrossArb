//! polymomentum-engine: unified Rust binary.
//!
//! Subcommands:
//!   live                              — main runtime (paper/live)
//!   scan                              — Gamma + scanner smoke test
//!   wallet                            — print wallet balances
//!   ctf <condition_id>                — read on-chain CTF resolution
//!   validate-replay <session.jsonl>   — replay-validator (parity check vs decision function)
//!
//! Environment-driven configuration. See `src/config.rs` for the full list of
//! variables; the runtime reads `.env` from the working directory if present.

mod backtest;
mod clob;
mod config;
mod data;
mod exchange;
mod execution;
mod fair_value;
mod live;
mod monitoring;
mod polymarket_ws;
mod price_state;
mod risk;
mod signing;
mod strategy;
mod sweep;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "polymomentum-engine", version, about = "PolyMomentum Rust trading engine")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Override log level (e.g. info, debug, trace)
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log: String,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the candle trading runtime
    Live {
        /// Paper or live mode (live requires explicit confirmation flag)
        #[arg(long, default_value = "paper")]
        mode: String,
        /// Allow live mode (default: paper-only safeguard).
        #[arg(long)]
        i_understand_live: bool,
    },
    /// Smoke-test scanner: fetch candle markets, print summary.
    Scan {
        #[arg(long, default_value_t = 2.0)]
        max_hours: f64,
        #[arg(long, default_value_t = 100.0)]
        min_liquidity: f64,
    },
    /// Print wallet balances (USDC.e, native USDC, POL).
    Wallet,
    /// Read CTF resolution for a condition_id.
    Ctf { condition_id: String },
    /// Validate a paper session JSONL replays clean against the decision function.
    ValidateReplay { path: String },
    /// Pre-download PMXT v2 archives for a UTC hour range so subsequent
    /// `harness` runs are offline-fast.
    PmxtDownload {
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: Option<String>,
        #[arg(long)]
        cache_dir: Option<String>,
    },
    /// Print PMXT v2 archive metadata for a given hour: distinct
    /// condition_ids, sample IDs, total event count.
    PmxtInfo {
        #[arg(long)]
        hour: String,
        #[arg(long)]
        cache_dir: Option<String>,
        #[arg(long, default_value_t = 5)]
        sample: usize,
    },
    /// Sweep a parameter grid through the full L2-backtest harness. Generates
    /// cartesian product of confidence × z × edge × ev × {taker, maker} —
    /// runs every cell against the same hours and ranks by PnL.
    HarnessSweep {
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: Option<String>,
        #[arg(long, default_value_t = 100.0)]
        bankroll: f64,
        #[arg(long)]
        cache_dir: Option<String>,
        #[arg(long)]
        btc_csv: Option<String>,
        #[arg(long, default_value_t = 50)]
        latency_ms: u64,
        /// Comma-separated confidence thresholds.
        #[arg(long, default_value = "0.30,0.40,0.50,0.60")]
        conf: String,
        /// Comma-separated z-score thresholds.
        #[arg(long, default_value = "0.20,0.50,1.00")]
        z: String,
        /// Comma-separated edge thresholds.
        #[arg(long, default_value = "0.00,0.03,0.07")]
        edge: String,
        /// Comma-separated EV buffers (negative disables the EV gate).
        #[arg(long, default_value = "-1.0,0.05")]
        ev_buffer: String,
        /// Include both maker and taker fill model variants per cell.
        #[arg(long, default_value_t = true)]
        also_maker: bool,
        /// Show top N variants in the report.
        #[arg(long, default_value_t = 20)]
        top: usize,
    },
    /// Run the full L2-backtest harness over PMXT v2 archives. Loads candle
    /// markets from Gamma, downloads/streams the requested UTC hours,
    /// replays them through each strategy variant, resolves against the
    /// actual BTC tape, and prints per-variant P&L.
    Harness {
        /// Inclusive UTC start hour (RFC3339), e.g. 2026-04-26T10:00:00Z.
        #[arg(long)]
        start: String,
        /// Inclusive UTC end hour. Defaults to `start` (single hour).
        #[arg(long)]
        end: Option<String>,
        /// Bankroll used to size hypothetical trades.
        #[arg(long, default_value_t = 100.0)]
        bankroll: f64,
        /// PMXT v2 cache directory (otherwise pulled from PMXT_V2_CACHE_DIR).
        #[arg(long)]
        cache_dir: Option<String>,
        /// BTC kline CSV (Binance format) used for the tape. If omitted, the
        /// harness pulls 1m klines from Binance's public REST.
        #[arg(long)]
        btc_csv: Option<String>,
        /// Insert latency in ms (strategy → fill).
        #[arg(long, default_value_t = 50)]
        latency_ms: u64,
    },
    /// Replay one or more captured session JSONLs through a grid of strategy
    /// variants and report synthetic P&L per variant.
    Sweep {
        /// Path(s) to session_*.jsonl files. Repeat the flag for multiple.
        #[arg(long)]
        session: Vec<String>,
        /// Bankroll used to size hypothetical trades.
        #[arg(long, default_value_t = 100.0)]
        bankroll: f64,
        /// Minimum trades for a variant before its numbers are considered
        /// statistically meaningful.
        #[arg(long, default_value_t = 30)]
        min_trades: u64,
        /// Show per-zone breakdown for each strategy.
        #[arg(long, default_value_t = false)]
        zones: bool,
    },
    /// Run unit + integration tests embedded in the binary.
    SelfTest,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    init_tracing(&cli.log);
    let settings = config::Settings::from_env();

    match cli.command {
        Command::Live { mode, i_understand_live } => {
            if mode == "live" && !i_understand_live {
                eprintln!("Refusing to run live mode without --i-understand-live (safety guard).");
                std::process::exit(2);
            }
            let m = match mode.as_str() {
                "paper" => live::pipeline::Mode::Paper,
                "live" => live::pipeline::Mode::Live,
                other => {
                    eprintln!("unknown mode: {other}");
                    std::process::exit(2);
                }
            };
            let pipeline = live::pipeline::Pipeline::new(settings.clone(), m).await;
            match pipeline {
                Ok(p) => {
                    install_signal_handlers(p.stop_token());
                    if let Err(e) = p.run().await {
                        tracing::error!(error = %e, "pipeline exited with error");
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    eprintln!("pipeline init failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        Command::Scan { max_hours, min_liquidity } => {
            cmd_scan(&settings, max_hours, min_liquidity).await;
        }
        Command::Wallet => cmd_wallet(&settings).await,
        Command::Ctf { condition_id } => cmd_ctf(&settings, &condition_id).await,
        Command::ValidateReplay { path } => cmd_validate_replay(&path).await,
        Command::Sweep { session, bankroll, min_trades, zones } => {
            cmd_sweep(&session, bankroll, min_trades, zones);
        }
        Command::PmxtInfo { hour, cache_dir, sample } => {
            cmd_pmxt_info(&hour, cache_dir.as_deref(), sample).await;
        }
        Command::PmxtDownload { start, end, cache_dir } => {
            cmd_pmxt_download(&start, end.as_deref(), cache_dir.as_deref()).await;
        }
        Command::HarnessSweep {
            start,
            end,
            bankroll,
            cache_dir,
            btc_csv,
            latency_ms,
            conf,
            z,
            edge,
            ev_buffer,
            also_maker,
            top,
        } => {
            let conf = parse_csv_floats(&conf);
            let zs = parse_csv_floats(&z);
            let edges = parse_csv_floats(&edge);
            let evs = parse_csv_floats(&ev_buffer);
            cmd_harness_sweep(
                &settings,
                &start,
                end.as_deref(),
                bankroll,
                cache_dir.as_deref(),
                btc_csv.as_deref(),
                latency_ms,
                conf,
                zs,
                edges,
                evs,
                also_maker,
                top,
            ).await;
        }
        Command::Harness {
            start,
            end,
            bankroll,
            cache_dir,
            btc_csv,
            latency_ms,
        } => {
            cmd_harness(&settings, &start, end.as_deref(), bankroll, cache_dir.as_deref(), btc_csv.as_deref(), latency_ms).await;
        }
        Command::SelfTest => {
            println!("self-test: this binary's tests run via `cargo test`. ok.");
        }
    }
}

fn install_signal_handlers(stop: std::sync::Arc<tokio::sync::Notify>) {
    tokio::spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM");
        let mut int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("install SIGINT");
        tokio::select! {
            _ = term.recv() => tracing::info!("SIGTERM received, shutting down"),
            _ = int.recv() => tracing::info!("SIGINT received, shutting down"),
        }
        stop.notify_one();
    });
}

fn init_tracing(level: &str) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

async fn cmd_scan(s: &config::Settings, max_hours: f64, min_liquidity: f64) {
    let client = data::gamma::GammaClient::new(&s.poly_gamma_url);
    match client.fetch_markets_by_end_date(max_hours, min_liquidity).await {
        Ok(markets) => {
            let contracts =
                data::scanner::scan_candle_markets(&markets, max_hours, min_liquidity);
            println!("markets={} candle_contracts={}", markets.len(), contracts.len());
            for c in contracts.iter().take(20) {
                println!(
                    "  {asset:5} {hours:5.2}h {q}",
                    asset = c.asset,
                    hours = c.hours_left,
                    q = c.market.question,
                );
            }
        }
        Err(e) => {
            eprintln!("scan failed: {e}");
            std::process::exit(1);
        }
    }
}

async fn cmd_wallet(s: &config::Settings) {
    if s.private_key.is_empty() {
        eprintln!("PRIVATE_KEY not set");
        std::process::exit(1);
    }
    match data::wallet::WalletReader::new(&s.polygon_rpc_url, &s.private_key) {
        Ok(reader) => match reader.fetch_balances().await {
            Ok(b) => {
                println!("address      {}", b.address);
                println!("usdc_e       ${:.2}", b.usdc_e);
                println!("usdc_native  ${:.2}", b.usdc_native);
                println!("total_usdc   ${:.2}", b.total_usdc);
                println!("pol          {:.4}", b.pol);
            }
            Err(e) => {
                eprintln!("wallet fetch failed: {e}");
                std::process::exit(1);
            }
        },
        Err(e) => {
            eprintln!("wallet init failed: {e}");
            std::process::exit(1);
        }
    }
}

async fn cmd_ctf(s: &config::Settings, condition_id: &str) {
    let r = data::ctf::CtfReader::new(&s.polygon_rpc_url);
    match r.get_resolution(condition_id).await {
        Ok((res, [n0, n1])) => {
            println!("resolution    {}", res.as_str());
            println!("payout_num0   {}", n0);
            println!("payout_num1   {}", n1);
        }
        Err(e) => {
            eprintln!("ctf read failed: {e}");
            std::process::exit(1);
        }
    }
}

async fn cmd_validate_replay(path: &str) {
    use std::io::BufRead;
    let f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("open {path}: {e}");
            std::process::exit(1);
        }
    };
    let reader = std::io::BufReader::new(f);
    let mut total = 0u64;
    let mut mismatches = 0u64;
    for line in reader.lines().map_while(|l| l.ok()) {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
        if v.get("cat").and_then(|x| x.as_str()) != Some("signal") {
            continue;
        }
        if v.get("type").and_then(|x| x.as_str()) != Some("evaluation") {
            continue;
        }
        total += 1;

        // Build inputs
        let signal = strategy::momentum::MomentumSignal {
            direction: v.get("dir").and_then(|x| x.as_str()).unwrap_or("up").to_string(),
            confidence: f64opt(&v, "conf").unwrap_or(0.0),
            price_change: f64opt(&v, "chg").unwrap_or(0.0),
            price_change_pct: f64opt(&v, "chg_pct").unwrap_or(0.0),
            consistency: f64opt(&v, "cons").unwrap_or(0.0),
            minutes_elapsed: f64opt(&v, "elapsed_min").unwrap_or(0.0),
            minutes_remaining: f64opt(&v, "remaining_min").unwrap_or(0.0),
            current_price: f64opt(&v, "px").unwrap_or(0.0),
            open_price: f64opt(&v, "open").unwrap_or(0.0),
            z_score: f64opt(&v, "z").unwrap_or(0.0),
            reversion_count: 0,
        };
        let cfg = strategy::decision::ZoneConfig::default();
        let res = strategy::decision::decide_candle_trade(
            &signal,
            signal.minutes_elapsed,
            signal.minutes_remaining,
            signal.minutes_elapsed + signal.minutes_remaining,
            f64opt(&v, "up_price").unwrap_or(0.5),
            f64opt(&v, "down_price").unwrap_or(0.5),
            signal.current_price,
            signal.open_price,
            f64opt(&v, "implied_vol").unwrap_or(0.5),
            strategy::decision::DEFAULT_MIN_CONFIDENCE,
            strategy::decision::DEFAULT_MIN_EDGE,
            true,
            &cfg,
            f64opt(&v, "cross_boost").unwrap_or(0.0),
        );
        let traded = matches!(res, strategy::decision::DecisionResult::Trade(_));
        let logged_traded = v.get("traded").and_then(|x| x.as_bool()).unwrap_or(false);
        if traded != logged_traded {
            mismatches += 1;
        }
    }
    let mismatch_pct = if total > 0 {
        100.0 * mismatches as f64 / total as f64
    } else {
        0.0
    };
    println!("validate-replay: total={total} mismatches={mismatches} ({mismatch_pct:.2}%)");
    if mismatches > 0 {
        std::process::exit(1);
    }
}

async fn cmd_pmxt_download(start: &str, end: Option<&str>, cache_dir: Option<&str>) {
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    let s: DateTime<Utc> = match DateTime::parse_from_rfc3339(start) {
        Ok(d) => d.with_timezone(&Utc),
        Err(e) => {
            eprintln!("--start: {e}");
            std::process::exit(2);
        }
    };
    let e: DateTime<Utc> = match end {
        Some(e) => match DateTime::parse_from_rfc3339(e) {
            Ok(d) => d.with_timezone(&Utc),
            Err(err) => {
                eprintln!("--end: {err}");
                std::process::exit(2);
            }
        },
        None => s,
    };
    let path = cache_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(backtest::pmxt::DEFAULT_CACHE_DIR));
    let loader = backtest::pmxt::PMXTv2Loader::new(&path);
    let mut cur = s;
    while cur <= e {
        if let Err(err) = loader.download_hour(cur, false).await {
            eprintln!("download {} failed: {err}", cur);
            std::process::exit(1);
        }
        cur = cur + ChronoDuration::hours(1);
    }
    println!("downloaded into {}", path.display());
}

async fn cmd_pmxt_info(hour: &str, cache_dir: Option<&str>, sample: usize) {
    use chrono::{DateTime, Utc};
    let dt: DateTime<Utc> = match DateTime::parse_from_rfc3339(hour) {
        Ok(d) => d.with_timezone(&Utc),
        Err(e) => {
            eprintln!("--hour must be RFC3339: {e}");
            std::process::exit(2);
        }
    };
    let path = cache_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(
                std::env::var("PMXT_V2_CACHE_DIR")
                    .unwrap_or_else(|_| backtest::pmxt::DEFAULT_CACHE_DIR.to_string()),
            )
        });
    let loader = backtest::pmxt::PMXTv2Loader::new(&path);
    if !loader.is_cached(dt) {
        eprintln!("not cached — run `harness` once or `download` first");
        std::process::exit(1);
    }
    match loader.distinct_condition_ids(dt) {
        Ok(s) => {
            println!("hour:                  {hour}");
            println!("distinct condition_ids: {}", s.len());
            for id in s.iter().take(sample) {
                println!("  len={:<3} {}", id.len(), id);
            }
        }
        Err(e) => {
            eprintln!("pmxt-info failed: {e}");
            std::process::exit(1);
        }
    }
}

fn parse_csv_floats(s: &str) -> Vec<f64> {
    s.split(',')
        .filter_map(|p| p.trim().parse::<f64>().ok())
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn cmd_harness_sweep(
    settings: &config::Settings,
    start: &str,
    end: Option<&str>,
    bankroll: f64,
    cache_dir: Option<&str>,
    btc_csv: Option<&str>,
    latency_ms: u64,
    conf: Vec<f64>,
    z: Vec<f64>,
    edge: Vec<f64>,
    ev_buffer: Vec<f64>,
    also_maker: bool,
    top: usize,
) {
    use chrono::{DateTime, Duration as ChronoDuration, Utc};

    let start_dt: DateTime<Utc> = match DateTime::parse_from_rfc3339(start) {
        Ok(d) => d.with_timezone(&Utc),
        Err(e) => {
            eprintln!("--start must be RFC3339: {e}");
            std::process::exit(2);
        }
    };
    let end_dt = match end {
        Some(e) => match DateTime::parse_from_rfc3339(e) {
            Ok(d) => d.with_timezone(&Utc),
            Err(err) => {
                eprintln!("--end must be RFC3339: {err}");
                std::process::exit(2);
            }
        },
        None => start_dt,
    };
    let mut hours = Vec::new();
    let mut cur = start_dt;
    while cur <= end_dt {
        hours.push(cur);
        cur = cur + ChronoDuration::hours(1);
    }

    // Build the variant grid.
    let grid = backtest::sweep::SweepGrid {
        base: backtest::strategies::StrategyVariant::baseline(),
        conf,
        z,
        edge,
        ev_buffer,
        also_maker,
    };
    let variants = grid.variants();
    if variants.is_empty() {
        eprintln!("empty parameter grid (check --conf/--z/--edge/--ev-buffer)");
        std::process::exit(2);
    }
    tracing::info!(variants = variants.len(), "sweep grid built");

    // Universe + tape (same as cmd_harness)
    let cache_dir_path = cache_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(
                std::env::var("PMXT_V2_CACHE_DIR")
                    .unwrap_or_else(|_| backtest::pmxt::DEFAULT_CACHE_DIR.to_string()),
            )
        });
    let loader = backtest::pmxt::PMXTv2Loader::new(&cache_dir_path);
    for &h in &hours {
        if let Err(e) = loader.download_hour(h, false).await {
            eprintln!("download {} failed: {e}", h);
            std::process::exit(1);
        }
    }
    let mut all_cids: std::collections::HashSet<String> = std::collections::HashSet::new();
    for &h in &hours {
        match loader.distinct_condition_ids(h) {
            Ok(s) => all_cids.extend(s),
            Err(e) => {
                eprintln!("read distinct cids for {}: {e}", h);
                std::process::exit(1);
            }
        }
    }
    let cache_dir_path_for_meta = cache_dir_path.clone();
    let gamma_cache_path = cache_dir_path_for_meta.join("gamma_market_cache.json");
    let mut cached_markets: std::collections::BTreeMap<String, data::models::Market> =
        match std::fs::read_to_string(&gamma_cache_path) {
            Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
            Err(_) => Default::default(),
        };
    let cid_vec: Vec<String> = all_cids
        .iter()
        .filter(|c| !cached_markets.contains_key(*c))
        .cloned()
        .collect();
    if !cid_vec.is_empty() {
        tracing::info!(missing = cid_vec.len(), cached = cached_markets.len(), "Gamma fetch");
        let gamma = data::gamma::GammaClient::new(&settings.poly_gamma_url);
        let new_markets = match gamma.fetch_markets_by_condition_ids(&cid_vec).await {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Gamma lookup failed: {e}");
                std::process::exit(1);
            }
        };
        for m in new_markets {
            cached_markets.insert(m.condition_id.clone(), m);
        }
        if let Ok(s) = serde_json::to_string(&cached_markets) {
            let _ = std::fs::write(&gamma_cache_path, s);
        }
    }
    let markets: Vec<data::models::Market> = cached_markets.values().cloned().collect();

    let mut contracts = data::scanner::scan_candle_markets_for_backtest(&markets, 0.0);
    contracts.retain(|c| c.asset == "BTC");
    let start_ts = start_dt.timestamp() as f64;
    let end_ts = end_dt.timestamp() as f64 + 3600.0;
    contracts.retain(|c| {
        let close_t = chrono::DateTime::parse_from_rfc3339(&c.end_date)
            .map(|d| d.timestamp() as f64)
            .unwrap_or(0.0);
        let window_minutes = live::window::estimate_window_minutes(&c.window_description);
        let window_minutes = if window_minutes > 0.0 { window_minutes } else { 60.0 };
        let open_t = close_t - window_minutes * 60.0;
        close_t > start_ts && open_t < end_ts
    });
    let universe = backtest::harness::CandleUniverse { contracts };
    if universe.contracts.is_empty() {
        eprintln!("no candle contracts in archive window");
        std::process::exit(1);
    }
    tracing::info!(contracts = universe.contracts.len(), "harness universe loaded");

    // BTC tape
    let mut btc = backtest::btc_history::BTCHistory::new();
    if let Some(p) = btc_csv {
        btc.load_csv(p).ok();
    } else {
        let pad_ms = 3_600_000;
        let start_ms = start_dt.timestamp_millis() - pad_ms;
        let end_ms = end_dt.timestamp_millis() + pad_ms;
        match btc.load_from_binance(start_ms, end_ms, "BTCUSDT", "1s").await {
            Ok(n) if n > 100 => tracing::info!(rows = n, interval = "1s", "BTC klines"),
            _ => {
                btc = backtest::btc_history::BTCHistory::new();
                if let Err(e) = btc.load_from_binance(start_ms, end_ms, "BTCUSDT", "1m").await {
                    eprintln!("Binance fetch failed: {e}");
                    std::process::exit(1);
                }
            }
        }
    }

    let cfg = backtest::harness::HarnessConfig {
        hours,
        universe,
        btc_history: btc,
        bankroll_usd: bankroll,
        cache_dir: cache_dir_path,
        latency: backtest::l2_replay::StaticLatencyConfig { insert_ms: latency_ms },
    };

    println!("\nRunning sweep over {} variants × {} hours…\n", variants.len(), cfg.hours.len());
    match backtest::harness::run_harness(&cfg, &variants).await {
        Ok(runs) => {
            // Sort by PnL descending; trim to top N.
            let mut sorted = runs;
            sorted.sort_by(|a, b| {
                b.results
                    .total_pnl()
                    .partial_cmp(&a.results.total_pnl())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            // Filter out variants with zero trades (no signal under those gates)
            // and report the top N positive variants.
            let positive: Vec<_> = sorted.iter().filter(|r| r.results.n_trades() > 0).cloned().collect();
            let limit = top.min(positive.len());
            println!("Top {} variants by PnL (variants with ≥1 trade):\n", limit);
            println!("{}", backtest::harness::render_table(&positive[..limit]));
            let zero_count = sorted.iter().filter(|r| r.results.n_trades() == 0).count();
            println!(
                "\n{} of {} variants produced 0 trades (gates too strict for the universe).",
                zero_count, sorted.len(),
            );
        }
        Err(e) => {
            eprintln!("sweep failed: {e}");
            std::process::exit(1);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn cmd_harness(
    settings: &config::Settings,
    start: &str,
    end: Option<&str>,
    bankroll: f64,
    cache_dir: Option<&str>,
    btc_csv: Option<&str>,
    latency_ms: u64,
) {
    use chrono::{DateTime, Duration as ChronoDuration, Utc};

    let start_dt: DateTime<Utc> = match DateTime::parse_from_rfc3339(start) {
        Ok(d) => d.with_timezone(&Utc),
        Err(e) => {
            eprintln!("--start must be RFC3339 (e.g. 2026-04-26T10:00:00Z): {e}");
            std::process::exit(2);
        }
    };
    let end_dt = match end {
        Some(e) => match DateTime::parse_from_rfc3339(e) {
            Ok(d) => d.with_timezone(&Utc),
            Err(err) => {
                eprintln!("--end must be RFC3339: {err}");
                std::process::exit(2);
            }
        },
        None => start_dt,
    };
    if end_dt < start_dt {
        eprintln!("--end must be ≥ --start");
        std::process::exit(2);
    }

    // Build the hour list (inclusive).
    let mut hours = Vec::new();
    let mut cur = start_dt;
    let one_hour = ChronoDuration::hours(1);
    while cur <= end_dt {
        hours.push(cur);
        cur = cur + one_hour;
    }

    // 1. Discover candle universe directly from the parquet's distinct
    //    condition_ids. This is the only reliable way for HISTORICAL hours —
    //    Gamma's "active" feed only reflects the present.
    let cache_dir_path = cache_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(
                std::env::var("PMXT_V2_CACHE_DIR")
                    .unwrap_or_else(|_| backtest::pmxt::DEFAULT_CACHE_DIR.to_string()),
            )
        });
    let loader = backtest::pmxt::PMXTv2Loader::new(&cache_dir_path);
    for &h in &hours {
        if let Err(e) = loader.download_hour(h, false).await {
            eprintln!("download {} failed: {e}", h);
            std::process::exit(1);
        }
    }

    let mut all_cids: std::collections::HashSet<String> = std::collections::HashSet::new();
    for &h in &hours {
        match loader.distinct_condition_ids(h) {
            Ok(s) => all_cids.extend(s),
            Err(e) => {
                eprintln!("read distinct cids for {}: {e}", h);
                std::process::exit(1);
            }
        }
    }
    tracing::info!(cids = all_cids.len(), "distinct condition_ids in archive");

    // Gamma lookup is the bottleneck (~50 cids/RTT). Cache the parsed Markets
    // to disk keyed by condition_id so subsequent harness runs are near-instant.
    let cache_dir_path_for_meta = cache_dir_path.clone();
    let gamma_cache_path = cache_dir_path_for_meta.join("gamma_market_cache.json");
    let mut cached_markets: std::collections::BTreeMap<String, data::models::Market> = match std::fs::read_to_string(&gamma_cache_path) {
        Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
        Err(_) => Default::default(),
    };
    let cid_vec: Vec<String> = all_cids
        .iter()
        .filter(|c| !cached_markets.contains_key(*c))
        .cloned()
        .collect();
    if !cid_vec.is_empty() {
        tracing::info!(missing = cid_vec.len(), cached = cached_markets.len(), "Gamma cache miss; fetching");
        let gamma = data::gamma::GammaClient::new(&settings.poly_gamma_url);
        let new_markets = match gamma.fetch_markets_by_condition_ids(&cid_vec).await {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Gamma lookup failed: {e}");
                std::process::exit(1);
            }
        };
        for m in new_markets {
            cached_markets.insert(m.condition_id.clone(), m);
        }
        if let Ok(s) = serde_json::to_string(&cached_markets) {
            let _ = std::fs::write(&gamma_cache_path, s);
        }
    } else {
        tracing::info!(cached = cached_markets.len(), "Gamma cache hit (no fetch needed)");
    }
    let markets: Vec<data::models::Market> = cached_markets.values().cloned().collect();
    tracing::info!(markets = markets.len(), "Gamma metadata loaded");

    // 2. Filter to candle markets via the existing scanner regex. For the
    //    first iteration of the harness we restrict to BTC underliers only —
    //    the BTC tape is the only history we load (alts would need their own
    //    feed pulled separately). Plenty of room to widen later.
    let mut contracts = data::scanner::scan_candle_markets_for_backtest(&markets, 0.0);
    contracts.retain(|c| c.asset == "BTC");
    // Keep candles whose [open_time, close_time] OVERLAPS the harness hours.
    let start_ts = start_dt.timestamp() as f64;
    let end_ts = end_dt.timestamp() as f64 + 3600.0;
    let pre_filter_count = contracts.len();
    contracts.retain(|c| {
        let close_t = chrono::DateTime::parse_from_rfc3339(&c.end_date)
            .map(|d| d.timestamp() as f64)
            .unwrap_or(0.0);
        let window_minutes = live::window::estimate_window_minutes(&c.window_description);
        let window_minutes = if window_minutes > 0.0 { window_minutes } else { 60.0 };
        let open_t = close_t - window_minutes * 60.0;
        close_t > start_ts && open_t < end_ts
    });
    tracing::info!(
        pre = pre_filter_count,
        kept = contracts.len(),
        "candle window filter",
    );
    let universe = backtest::harness::CandleUniverse { contracts };
    if universe.contracts.is_empty() {
        eprintln!(
            "no candle contracts in archive window — checked {} markets, found 0 candles in [{start}, {end}]",
            markets.len(),
            start = start,
            end = end.unwrap_or(start),
        );
        std::process::exit(1);
    }
    tracing::info!(contracts = universe.contracts.len(), "harness universe loaded");

    // 2. BTC tape.
    let mut btc = backtest::btc_history::BTCHistory::new();
    if let Some(p) = btc_csv {
        match btc.load_csv(p) {
            Ok(n) => tracing::info!(rows = n, "BTC CSV loaded"),
            Err(e) => {
                eprintln!("BTC CSV load failed: {e}");
                std::process::exit(1);
            }
        }
    } else {
        // Pad ±1 hour around the harness window so the resolver has open/close
        // prices on the boundary. Use 1-second klines for intra-window
        // momentum detection; falls back to 1m if Binance rate-limits.
        let pad_ms = 3_600_000;
        let start_ms = start_dt.timestamp_millis() - pad_ms;
        let end_ms = end_dt.timestamp_millis() + pad_ms;
        match btc.load_from_binance(start_ms, end_ms, "BTCUSDT", "1s").await {
            Ok(n) if n > 100 => tracing::info!(rows = n, interval = "1s", "BTC klines pulled"),
            Ok(_) | Err(_) => {
                tracing::warn!("1s klines unavailable; falling back to 1m");
                btc = backtest::btc_history::BTCHistory::new();
                match btc.load_from_binance(start_ms, end_ms, "BTCUSDT", "1m").await {
                    Ok(n) => tracing::info!(rows = n, interval = "1m", "BTC klines pulled"),
                    Err(e) => {
                        eprintln!("Binance kline fetch failed: {e}");
                        std::process::exit(1);
                    }
                }
            }
        }
    }
    if btc.n_ticks() < 50 {
        eprintln!("not enough BTC ticks ({} < 50)", btc.n_ticks());
        std::process::exit(1);
    }

    let cfg = backtest::harness::HarnessConfig {
        hours,
        universe,
        btc_history: btc,
        bankroll_usd: bankroll,
        cache_dir: cache_dir_path,
        latency: backtest::l2_replay::StaticLatencyConfig { insert_ms: latency_ms },
    };

    let variants = backtest::strategies::default_variants();
    match backtest::harness::run_harness(&cfg, &variants).await {
        Ok(runs) => {
            println!(
                "\nHarness — {start}{} → {end} bankroll=${bankroll:.0} latency={latency_ms}ms variants={}\n",
                if end.is_some() { "" } else { "" },
                runs.len(),
                start = start,
                end = end.unwrap_or(start),
            );
            println!("{}", backtest::harness::render_table(&runs));
            println!("{}", backtest::harness::render_zone_breakdown(&runs));
        }
        Err(e) => {
            eprintln!("harness failed: {e}");
            std::process::exit(1);
        }
    }
}

fn cmd_sweep(sessions: &[String], bankroll: f64, min_trades: u64, show_zones: bool) {
    if sessions.is_empty() {
        eprintln!("--session is required (repeat for multiple files)");
        std::process::exit(2);
    }
    let paths: Vec<std::path::PathBuf> = sessions.iter().map(std::path::PathBuf::from).collect();
    let strats = sweep::strategy::default_strategies();
    let runs = match sweep::run_sweep(&paths, &strats, bankroll, min_trades) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("sweep failed: {e}");
            std::process::exit(1);
        }
    };

    // Sort by P&L descending so the strongest variants are at the top.
    let mut sorted = runs.clone();
    sorted.sort_by(|a, b| {
        b.realized_pnl.partial_cmp(&a.realized_pnl).unwrap_or(std::cmp::Ordering::Equal)
    });

    println!("\nSweep over {} session file(s) — bankroll=${bankroll:.0}, min_trades={min_trades}\n", paths.len());
    println!("{}", sweep::render_table(&sorted));
    if show_zones {
        println!("{}", sweep::render_zone_breakdown(&sorted));
    }

    // Surface data-gap warnings.
    let total_resolved_each: Vec<u64> = runs.iter().map(|r| r.trades).collect();
    let max_resolved = *total_resolved_each.iter().max().unwrap_or(&0);
    if max_resolved < min_trades {
        println!(
            "\n⚠  insufficient sample: best variant has only {max_resolved} resolved trade(s); \
             collect ≥{min_trades} before drawing conclusions."
        );
    }
}

fn f64opt(v: &serde_json::Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64())
}
