#!/bin/bash
# Rust-only deploy: build the polymomentum-engine binary, copy to VPS,
# update systemd unit, restart.
#
# Usage:
#   deploy/deploy_rust.sh user@vps-ip [--enable-service] [--mode paper|live]
#
# Layout on VPS:
#   /opt/polymomentum/
#     polymomentum-engine                 ← Rust binary
#     logs/                               ← shared log dir (state.db, sessions/)
#     data/                               ← shared data dir
#   /etc/polymomentum/env                 ← .env-style config
#   /etc/systemd/system/polymomentum-engine.service
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 user@vps-ip [--enable-service] [--mode paper|live]" >&2
    exit 1
fi

VPS="$1"; shift
ENABLE=false
MODE="paper"
while [ $# -gt 0 ]; do
    case "$1" in
        --enable-service) ENABLE=true; shift ;;
        --mode) MODE="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [ "$MODE" != "paper" ] && [ "$MODE" != "live" ]; then
    echo "--mode must be paper or live" >&2
    exit 2
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
APP_DIR="/opt/polymomentum"

echo "=== Building Rust binary (release) ==="
(cd "$ROOT_DIR/rust_engine" && cargo build --release --bin polymomentum-engine)

BIN="$ROOT_DIR/rust_engine/target/release/polymomentum-engine"
if [ ! -f "$BIN" ]; then
    echo "Build did not produce $BIN" >&2
    exit 1
fi

echo "=== Copying binary to $VPS ==="
ssh "$VPS" "mkdir -p $APP_DIR/logs/candle $APP_DIR/logs/sessions $APP_DIR/data"
scp "$BIN" "$VPS:$APP_DIR/polymomentum-engine.new"
ssh "$VPS" "chown polymomentum:polymomentum $APP_DIR/polymomentum-engine.new && \
    chmod 0755 $APP_DIR/polymomentum-engine.new && \
    mv $APP_DIR/polymomentum-engine.new $APP_DIR/polymomentum-engine"

echo "=== Updating systemd unit (mode=$MODE) ==="
SERVICE_TMP="$(mktemp)"
sed "s|--mode paper|--mode $MODE|" "$ROOT_DIR/deploy/polymomentum-engine.service" > "$SERVICE_TMP"
scp "$SERVICE_TMP" "$VPS:/tmp/polymomentum-engine.service"
rm -f "$SERVICE_TMP"
ssh "$VPS" "sudo mv /tmp/polymomentum-engine.service /etc/systemd/system/polymomentum-engine.service && \
    sudo systemctl daemon-reload"

if $ENABLE; then
    echo "=== Enabling service ==="
    ssh "$VPS" "sudo systemctl enable polymomentum-engine && sudo systemctl restart polymomentum-engine"
else
    echo "Service installed but not enabled. To enable: ssh $VPS 'sudo systemctl enable --now polymomentum-engine'"
fi

echo "=== Done ==="
echo "Logs: ssh $VPS 'journalctl -u polymomentum-engine -f'"
