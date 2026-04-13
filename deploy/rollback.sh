#!/bin/bash
# Roll the /opt/polymomentum/current symlink back to a previous release
# and restart all services. Usage:
#   deploy/rollback.sh                 # roll back to the immediately prior release
#   deploy/rollback.sh release-id      # roll back to a specific release id
#   deploy/rollback.sh --list          # show available releases
set -euo pipefail

APP_DIR="${POLYMOMENTUM_DIR:-/opt/polymomentum}"
RELEASES_DIR="$APP_DIR/releases"
CURRENT_LINK="$APP_DIR/current"
SERVICES="${POLYMOMENTUM_SERVICES:-polymomentum-candle polymomentum-rust}"

if [ ! -d "$RELEASES_DIR" ]; then
    echo "ERROR: $RELEASES_DIR does not exist — no versioned deploys yet" >&2
    exit 1
fi

# List mode
if [ "${1:-}" = "--list" ]; then
    echo "Available releases (newest first):"
    ls -1t "$RELEASES_DIR" | nl
    if [ -L "$CURRENT_LINK" ]; then
        cur=$(basename "$(readlink -f "$CURRENT_LINK")")
        echo
        echo "Current: $cur"
    fi
    exit 0
fi

# Pick target release
target="${1:-}"
if [ -z "$target" ]; then
    # Find the release just BEFORE the current symlink target
    if [ ! -L "$CURRENT_LINK" ]; then
        echo "ERROR: $CURRENT_LINK is not a symlink — cannot infer prior release" >&2
        exit 1
    fi
    cur=$(basename "$(readlink -f "$CURRENT_LINK")")
    # ls -t = newest first; pick the first one that is not the current
    target=$(ls -1t "$RELEASES_DIR" | grep -v "^$cur$" | head -n 1 || true)
    if [ -z "$target" ]; then
        echo "ERROR: no release older than current ($cur) found" >&2
        exit 1
    fi
fi

target_path="$RELEASES_DIR/$target"
if [ ! -d "$target_path" ]; then
    echo "ERROR: release '$target' does not exist under $RELEASES_DIR" >&2
    exit 1
fi

# Atomic symlink swap
echo "Rolling current → $target"
ln -sfn "$target_path" "$CURRENT_LINK.new"
mv -Tf "$CURRENT_LINK.new" "$CURRENT_LINK"

# Restart services
for svc in $SERVICES; do
    if systemctl list-unit-files | grep -q "^$svc"; then
        echo "Restarting $svc"
        systemctl restart "$svc" || true
    fi
done

echo "Rollback to $target complete"
