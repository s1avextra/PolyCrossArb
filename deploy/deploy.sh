#!/bin/bash
set -euo pipefail
[ $# -lt 1 ] && echo "Usage: $0 user@vps-ip [--rust]" && exit 1
VPS="$1"; REBUILD_RUST=false
[[ "${2:-}" == "--rust" ]] && REBUILD_RUST=true
echo "=== Deploying to $VPS ==="
rsync -avz --delete --exclude '.env' --exclude 'logs/' --exclude 'data/' \
    --exclude 'rust_engine/target/' --exclude '.git/' --exclude '__pycache__/' \
    --exclude '.venv/' --exclude 'uv.lock' . "$VPS:/opt/polycrossarb/"
ssh "$VPS" "cd /opt/polycrossarb && uv pip install --system '.[execution]'"
$REBUILD_RUST && ssh "$VPS" "cd /opt/polycrossarb/rust_engine && cargo build --release && cp target/release/polycrossarb-engine /opt/polycrossarb/"
ssh "$VPS" "systemctl restart polycrossarb-candle polycrossarb-arb polycrossarb-weather"
$REBUILD_RUST && ssh "$VPS" "systemctl restart polycrossarb-rust"
echo "=== Done ==="
