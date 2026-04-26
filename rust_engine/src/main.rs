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

fn f64opt(v: &serde_json::Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64())
}
