#!/bin/bash
# Versioned deploy: rsync source into a timestamped releases/ directory,
# atomically swap the `current` symlink, install Python deps, optionally
# rebuild Rust, restart services, and prune old releases.
#
# Usage:
#   deploy/deploy.sh user@vps-ip [--rust] [--keep N]
#
# Layout on VPS:
#   /opt/polycrossarb/
#     releases/
#       2026-04-11T141500Z/        ← rsync target
#       2026-04-11T120000Z/
#       2026-04-10T180000Z/
#     current → releases/2026-04-11T141500Z/   ← atomic symlink
#     .env                                       ← stays out of releases
#     logs/                                       ← stays out of releases
#     polycrossarb-engine                         ← rust binary, copied per release
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 user@vps-ip [--rust] [--keep N]" >&2
    exit 1
fi

VPS="$1"; shift
REBUILD_RUST=false
KEEP=3
while [ $# -gt 0 ]; do
    case "$1" in
        --rust) REBUILD_RUST=true; shift ;;
        --keep) KEEP="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

APP_DIR="/opt/polycrossarb"
RELEASE_ID="$(date -u +%Y-%m-%dT%H%M%SZ)"
RELEASE_DIR="$APP_DIR/releases/$RELEASE_ID"

echo "=== Deploying to $VPS as $RELEASE_ID ==="

# 1. Make the new release dir on the VPS
ssh "$VPS" "mkdir -p '$RELEASE_DIR'"

# 2. Sync source into the release dir (NOT into APP_DIR root anymore)
rsync -avz --delete \
    --exclude '.env' \
    --exclude 'logs/' \
    --exclude '/data/' \
    --exclude 'rust_engine/target/' \
    --exclude '.git/' \
    --exclude '__pycache__/' \
    --exclude '.venv/' \
    --exclude 'releases/' \
    --exclude 'current' \
    --exclude 'uv.lock' \
    ./ "$VPS:$RELEASE_DIR/"

# 3. Fix ownership, install deps, swap symlink, restart — single SSH session.
RUST_CMDS=""
if $REBUILD_RUST; then
    RUST_CMDS="
cd '$RELEASE_DIR/rust_engine' && cargo build --release && cp target/release/polycrossarb-engine '$APP_DIR/'
systemctl restart polycrossarb-rust"
fi

ssh "$VPS" bash -s <<DEPLOY_EOF
set -euo pipefail
source \$HOME/.cargo/env 2>/dev/null || true

# Fix ownership (rsync preserves macOS uid which polycrossarb can't write to)
chown -R polycrossarb:polycrossarb '$RELEASE_DIR'

# Install Python deps into the project venv
cd '$RELEASE_DIR' && uv pip install --python /opt/polycrossarb/.venv/bin/python '.[execution]'

# Optionally rebuild Rust
$RUST_CMDS

# Atomic symlink flip
ln -sfn '$RELEASE_DIR' '$APP_DIR/current.new' && mv -Tf '$APP_DIR/current.new' '$APP_DIR/current'

# Restart active services only
systemctl restart polycrossarb-candle

# Prune old releases keeping last $KEEP
cd '$APP_DIR/releases' && ls -1t | tail -n +$((KEEP + 1)) | xargs -r -I {} rm -rf '{}'
DEPLOY_EOF

echo "=== Deployed $RELEASE_ID. Rollback: ssh $VPS '$APP_DIR/current/deploy/rollback.sh' ==="
