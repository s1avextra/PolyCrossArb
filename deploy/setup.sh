#!/bin/bash
# One-shot VPS bootstrap. Idempotent — safe to re-run.
set -euo pipefail
APP_DIR="/opt/polycrossarb"
ENV_DIR="/etc/polycrossarb"
ENV_FILE="$ENV_DIR/env"
KILL_DIR="/tmp/polycrossarb"
echo "=== PolyCrossArb VPS Setup ==="

# 1. System dependencies
apt-get update && apt-get install -y \
    build-essential pkg-config libssl-dev curl git jq sqlite3

# 2. Rust + uv toolchains
command -v cargo &>/dev/null || (
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    # shellcheck disable=SC1090
    source "$HOME/.cargo/env"
)
command -v uv &>/dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh

# 3. Service user, app dirs, releases dir, kill-switch dir
id polycrossarb &>/dev/null || useradd -r -s /bin/false -d "$APP_DIR" polycrossarb
mkdir -p \
    "$APP_DIR/logs/"{arb,candle,weather,sessions} \
    "$APP_DIR/releases" \
    "$KILL_DIR"
chown -R polycrossarb:polycrossarb "$APP_DIR" "$KILL_DIR"

# 4. Secrets directory — outside the deploy tree so deploys cannot
#    accidentally rsync over it.
mkdir -p "$ENV_DIR"
chown root:polycrossarb "$ENV_DIR"
chmod 750 "$ENV_DIR"
if [ ! -f "$ENV_FILE" ]; then
    cat > "$ENV_FILE" << 'ENVEOF'
# PolyCrossArb production env. Keep root:polycrossarb 0640.
POLY_API_KEY=
POLY_API_SECRET=
POLY_API_PASSPHRASE=
PRIVATE_KEY=
POLYGON_RPC_URL=https://polygon-rpc.com

# Alerter — REQUIRED in live mode (set ALERT_REQUIRED=1 to fail fast)
ALERT_WEBHOOK_URL=
ALERT_CHANNEL=slack
ALERT_REQUIRED=1

# Logging
LOG_FORMAT=json
CLOB_DIRECT=1

# Operational kill switch (touch this file from any shell to halt trading)
KILL_SWITCH_PATH=/tmp/polycrossarb/KILL
ENVEOF
    chmod 640 "$ENV_FILE"
    chown root:polycrossarb "$ENV_FILE"
    echo "Wrote secrets template to $ENV_FILE — fill in values before starting services"
fi
# Compatibility symlink so older systemd units that point at /opt/polycrossarb/.env still work
[ -L "$APP_DIR/.env" ] || ln -sf "$ENV_FILE" "$APP_DIR/.env"

# 5. Initial release: copy current source into releases/<bootstrap>
if [ ! -e "$APP_DIR/current" ]; then
    BOOTSTRAP_ID="bootstrap-$(date -u +%Y-%m-%dT%H%M%SZ)"
    BOOTSTRAP_DIR="$APP_DIR/releases/$BOOTSTRAP_ID"
    mkdir -p "$BOOTSTRAP_DIR"
    rsync -a --exclude '.env' --exclude 'logs/' --exclude 'data/' \
          --exclude 'releases/' --exclude '.git/' --exclude '__pycache__/' \
          --exclude '.venv/' "$APP_DIR/" "$BOOTSTRAP_DIR/" || true
    ln -sfn "$BOOTSTRAP_DIR" "$APP_DIR/current"
    chown -R polycrossarb:polycrossarb "$BOOTSTRAP_DIR"
fi

# 6. Python deps and rust build (out of `current`)
cd "$APP_DIR/current" && uv pip install --system ".[execution,dev]"
cd "$APP_DIR/current/rust_engine" && cargo build --release \
    && cp target/release/polycrossarb-engine "$APP_DIR/"

# 7. Systemd units
cp "$APP_DIR/current/deploy/"*.service /etc/systemd/system/
cp "$APP_DIR/current/deploy/"*.timer /etc/systemd/system/ 2>/dev/null || true
systemctl daemon-reload
systemctl enable polycrossarb-healthcheck.timer 2>/dev/null || true

echo "=== Done. Edit $ENV_FILE then: systemctl start polycrossarb-{candle,arb,weather,rust} ==="
echo "=== Healthcheck timer: systemctl status polycrossarb-healthcheck.timer ==="
