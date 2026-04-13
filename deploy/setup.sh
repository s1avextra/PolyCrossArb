#!/bin/bash
# One-shot VPS bootstrap. Idempotent — safe to re-run.
set -euo pipefail
APP_DIR="/opt/polymomentum"
ENV_DIR="/etc/polymomentum"
ENV_FILE="$ENV_DIR/env"
KILL_DIR="/tmp/polymomentum"
echo "=== PolyMomentum VPS Setup ==="

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
id polymomentum &>/dev/null || useradd -r -s /bin/false -d "$APP_DIR" polymomentum
mkdir -p \
    "$APP_DIR/logs/"{arb,candle,weather,sessions} \
    "$APP_DIR/releases" \
    "$KILL_DIR"
chown -R polymomentum:polymomentum "$APP_DIR" "$KILL_DIR"

# 4. Secrets directory — outside the deploy tree so deploys cannot
#    accidentally rsync over it.
mkdir -p "$ENV_DIR"
chown root:polymomentum "$ENV_DIR"
chmod 750 "$ENV_DIR"
if [ ! -f "$ENV_FILE" ]; then
    cat > "$ENV_FILE" << 'ENVEOF'
# PolyMomentum production env. Keep root:polymomentum 0640.
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
KILL_SWITCH_PATH=/tmp/polymomentum/KILL
ENVEOF
    chmod 640 "$ENV_FILE"
    chown root:polymomentum "$ENV_FILE"
    echo "Wrote secrets template to $ENV_FILE — fill in values before starting services"
fi
# Compatibility symlink so older systemd units that point at /opt/polymomentum/.env still work
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
    chown -R polymomentum:polymomentum "$BOOTSTRAP_DIR"
fi

# 6. Python deps and rust build (out of `current`)
# Create venv if it doesn't exist, then install into it
[ -d "$APP_DIR/.venv" ] || uv venv "$APP_DIR/.venv" --python python3
cd "$APP_DIR/current" && uv pip install --python "$APP_DIR/.venv/bin/python" ".[execution,dev]"
cd "$APP_DIR/current/rust_engine" && cargo build --release \
    && cp target/release/polymomentum-engine "$APP_DIR/"

# 7. Systemd units
cp "$APP_DIR/current/deploy/"*.service /etc/systemd/system/
cp "$APP_DIR/current/deploy/"*.timer /etc/systemd/system/ 2>/dev/null || true
systemctl daemon-reload
systemctl enable polymomentum-healthcheck.timer 2>/dev/null || true

echo "=== Done. Edit $ENV_FILE then: systemctl start polymomentum-{candle,arb,weather,rust} ==="
echo "=== Healthcheck timer: systemctl status polymomentum-healthcheck.timer ==="
