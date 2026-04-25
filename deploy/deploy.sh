#!/bin/bash
# Versioned deploy: rsync source into a timestamped releases/ directory,
# atomically swap the `current` symlink, install Python deps, optionally
# rebuild Rust, restart services, and prune old releases.
#
# Usage:
#   deploy/deploy.sh user@vps-ip [--rust] [--keep N]
#
# Layout on VPS:
#   /opt/polymomentum/
#     releases/
#       2026-04-11T141500Z/        ← rsync target
#       2026-04-11T120000Z/
#       2026-04-10T180000Z/
#     current → releases/2026-04-11T141500Z/   ← atomic symlink
#     .env                                       ← stays out of releases
#     logs/                                       ← stays out of releases
#     polymomentum-engine                         ← rust binary, copied per release
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

APP_DIR="/opt/polymomentum"
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
cd '$RELEASE_DIR/rust_engine' && cargo build --release && cp target/release/polymomentum-engine '$APP_DIR/'
systemctl restart polymomentum-rust"
fi

ssh "$VPS" bash -s <<DEPLOY_EOF
set -euo pipefail
source \$HOME/.cargo/env 2>/dev/null || true

# Fix ownership (rsync preserves macOS uid which polymomentum can't write to)
chown -R polymomentum:polymomentum '$RELEASE_DIR'

# Symlink logs/ and data/ to shared paths so state.db, JSONL sessions,
# candle_trades.jsonl, and collector CSVs persist across deploys. Without
# this, each release gets a fresh empty state.db and any in-flight paper
# positions become orphaned in the previous release dir on symlink swap.
mkdir -p '$APP_DIR/logs/candle' '$APP_DIR/logs/sessions' '$APP_DIR/data/live'
chown -R polymomentum:polymomentum '$APP_DIR/logs' '$APP_DIR/data'

# One-shot migration: if the currently-active release has real logs/ or data/
# dirs (pre-fix), copy any files not yet in the shared dir before we swap.
# Uses cp -n so existing shared files win (safe to run on every deploy).
if [ -d '$APP_DIR/current/logs' ] && [ ! -L '$APP_DIR/current/logs' ]; then
    cp -rn '$APP_DIR/current/logs/.' '$APP_DIR/logs/' 2>/dev/null || true
fi
if [ -d '$APP_DIR/current/data' ] && [ ! -L '$APP_DIR/current/data' ]; then
    cp -rn '$APP_DIR/current/data/.' '$APP_DIR/data/' 2>/dev/null || true
fi
chown -R polymomentum:polymomentum '$APP_DIR/logs' '$APP_DIR/data'

rm -rf '$RELEASE_DIR/logs' '$RELEASE_DIR/data'
ln -sfn '$APP_DIR/logs' '$RELEASE_DIR/logs'
ln -sfn '$APP_DIR/data' '$RELEASE_DIR/data'

# Install Python deps into the project venv
cd '$RELEASE_DIR' && uv pip install --python /opt/polymomentum/.venv/bin/python '.[execution]'

# Optionally rebuild Rust
$RUST_CMDS

# Atomic symlink flip
ln -sfn '$RELEASE_DIR' '$APP_DIR/current.new' && mv -Tf '$APP_DIR/current.new' '$APP_DIR/current'

# Restart active services only
systemctl restart polymomentum-candle

# Prune old releases keeping last \$KEEP.
# Sort by ISO8601 name (reverse = newest-first) not by mtime — rsync
# preserves source mtimes so the new release can appear "older" than
# old ones and get incorrectly pruned.
cd '$APP_DIR/releases' && ls -1 | sort -r | tail -n +$((KEEP + 1)) | xargs -r -I {} rm -rf '{}'
DEPLOY_EOF

echo "=== Deployed $RELEASE_ID. Rollback: ssh $VPS '$APP_DIR/current/deploy/rollback.sh' ==="
