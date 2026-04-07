#!/bin/bash
set -euo pipefail
APP_DIR="/opt/polycrossarb"
echo "=== PolyCrossArb VPS Setup ==="
apt-get update && apt-get install -y build-essential pkg-config libssl-dev curl git jq
# Rust
command -v cargo &>/dev/null || (curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && source "$HOME/.cargo/env")
# uv
command -v uv &>/dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh
# App user + dirs
id polycrossarb &>/dev/null || useradd -r -s /bin/false -d "$APP_DIR" polycrossarb
mkdir -p "$APP_DIR/logs/"{arb,candle,weather,sessions}
chown -R polycrossarb:polycrossarb "$APP_DIR"
# Python deps
cd "$APP_DIR" && uv pip install --system ".[execution,dev]"
# Rust build
cd "$APP_DIR/rust_engine" && cargo build --release && cp target/release/polycrossarb-engine "$APP_DIR/"
# Services
cp "$APP_DIR/deploy/"*.service /etc/systemd/system/ && systemctl daemon-reload
# .env template
[ -f "$APP_DIR/.env" ] || cat > "$APP_DIR/.env" << 'ENVEOF'
POLY_API_KEY=
POLY_API_SECRET=
POLY_API_PASSPHRASE=
PRIVATE_KEY=
POLYGON_RPC_URL=https://polygon-rpc.com
ALERT_WEBHOOK_URL=
LOG_FORMAT=json
CLOB_DIRECT=1
ENVEOF
chmod 600 "$APP_DIR/.env" && chown polycrossarb:polycrossarb "$APP_DIR/.env"
echo "=== Done. Edit $APP_DIR/.env then: systemctl start polycrossarb-{candle,arb,weather,rust} ==="
