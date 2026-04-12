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

# 3. Install Python deps for this release (no virtualenv — system-wide
#    via uv, same as before but pinned to the release dir for cwd).
ssh "$VPS" "cd '$RELEASE_DIR' && uv pip install --system '.[execution]'"

# 4. Optionally rebuild rust binary in the release and copy to top level.
if $REBUILD_RUST; then
    ssh "$VPS" "cd '$RELEASE_DIR/rust_engine' && cargo build --release && cp target/release/polycrossarb-engine '$APP_DIR/'"
fi

# 5. Atomic symlink flip
ssh "$VPS" "ln -sfn '$RELEASE_DIR' '$APP_DIR/current.new' && mv -Tf '$APP_DIR/current.new' '$APP_DIR/current'"

# 6. Restart services
ssh "$VPS" "systemctl restart polycrossarb-candle polycrossarb-arb polycrossarb-weather"
if $REBUILD_RUST; then
    ssh "$VPS" "systemctl restart polycrossarb-rust"
fi

# 7. Prune old releases keeping last $KEEP
ssh "$VPS" "cd '$APP_DIR/releases' && ls -1t | tail -n +$((KEEP + 1)) | xargs -r -I {} rm -rf '{}'"

echo "=== Deployed $RELEASE_ID. Rollback: ssh $VPS 'cd $APP_DIR && deploy/rollback.sh' ==="
