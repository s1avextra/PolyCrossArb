#!/bin/bash
# One-shot Rust-only VPS bootstrap. Idempotent — safe to re-run.
set -euo pipefail

APP_DIR="/opt/polymomentum"
ENV_DIR="/etc/polymomentum"
ENV_FILE="$ENV_DIR/env"
KILL_DIR="/tmp/polymomentum"

echo "=== PolyMomentum VPS Setup (Rust-only) ==="

# 1. System dependencies (sqlite3 for healthcheck, build tools for cargo).
apt-get update && apt-get install -y \
    build-essential pkg-config libssl-dev curl git jq sqlite3

# 2. Rust toolchain.
command -v cargo &>/dev/null || (
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    # shellcheck disable=SC1090
    source "$HOME/.cargo/env"
)

# 3. Service user, app dirs, kill-switch dir.
id polymomentum &>/dev/null || useradd -r -s /bin/false -d "$APP_DIR" polymomentum
mkdir -p \
    "$APP_DIR/logs/"{candle,sessions} \
    "$APP_DIR/data" \
    "$KILL_DIR"
chown -R polymomentum:polymomentum "$APP_DIR" "$KILL_DIR"

# 4. Secrets directory — outside the deploy tree.
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

# Slack alerter — REQUIRED in live mode (set ALERT_REQUIRED=1 to fail fast)
SLACK_WEBHOOK_URL=
ALERT_REQUIRED=1

# Logging
RUST_LOG=info

# Operational kill switch (touch this file from any shell to halt trading)
KILL_SWITCH_PATH=/tmp/polymomentum/KILL
ENVEOF
    chmod 640 "$ENV_FILE"
    chown root:polymomentum "$ENV_FILE"
    echo "Wrote secrets template to $ENV_FILE — fill in values before starting service"
fi

# 5. Drop the healthcheck script alongside the binary (no /current/ release tree).
HERE="$(cd "$(dirname "$0")" && pwd)"
install -m 0755 "$HERE/healthcheck.sh" "$APP_DIR/healthcheck.sh"
chown polymomentum:polymomentum "$APP_DIR/healthcheck.sh"

# 6. Systemd units.
cp "$HERE/polymomentum-engine.service" /etc/systemd/system/
cp "$HERE/polymomentum-healthcheck.service" /etc/systemd/system/
cp "$HERE/polymomentum-healthcheck.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable polymomentum-healthcheck.timer 2>/dev/null || true

echo
echo "=== Done. Next: ==="
echo "  1. Edit $ENV_FILE"
echo "  2. From your dev box: bash deploy/deploy.sh user@vps --enable-service --mode paper"
echo "  3. Watch logs: journalctl -u polymomentum-engine -f"
