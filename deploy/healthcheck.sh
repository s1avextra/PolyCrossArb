#!/bin/bash
# PolyMomentum healthcheck — invoked by polymomentum-healthcheck.timer.
# Checks: service up, kill switch, breaker, last-trade staleness, disk pressure.
set -uo pipefail

APP_DIR="${POLYMOMENTUM_DIR:-/opt/polymomentum}"
SERVICE="${POLYMOMENTUM_SERVICE:-polymomentum-engine}"
WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"
KILL_FILE="${KILL_FILE:-/tmp/polymomentum/KILL}"
STATE_DB="${STATE_DB:-$APP_DIR/logs/candle/state.db}"
INACTIVE_HOURS="${INACTIVE_HOURS:-2}"
LOGS_LIMIT_MB="${LOGS_LIMIT_MB:-2048}"

# Per-category cooldown so we don't spam.
COOLDOWN_DIR="${COOLDOWN_DIR:-/var/tmp/polymomentum-healthcheck}"
mkdir -p "$COOLDOWN_DIR"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-1800}"

now=$(date +%s)

alert() {
    local category="$1"; shift
    local msg="$*"
    local last_file="$COOLDOWN_DIR/$category.last"
    if [ -f "$last_file" ]; then
        local last
        last=$(cat "$last_file")
        if [ $((now - last)) -lt "$COOLDOWN_SECONDS" ]; then
            return 0
        fi
    fi
    echo "$now" > "$last_file"
    logger -t polymomentum "HEALTH[$category]: $msg"
    if [ -n "$WEBHOOK_URL" ]; then
        curl -s -X POST "$WEBHOOK_URL" \
            -H 'Content-Type: application/json' \
            -d "{\"text\": \":heart: HEALTH[$category]: $msg\"}" \
            >/dev/null 2>&1 || true
    fi
}

# 1. Service liveness — restart and alert if dead.
if ! systemctl is-active --quiet "$SERVICE" 2>/dev/null; then
    alert "service_down" "$SERVICE inactive — restarting"
    systemctl restart "$SERVICE" 2>/dev/null || true
fi

# 2. Disk free.
DISK=$(df "$APP_DIR" 2>/dev/null | awk 'NR==2{gsub("%","",$5); print $5}')
if [ -n "$DISK" ] && [ "$DISK" -gt 90 ]; then
    alert "disk_full" "disk usage ${DISK}% on $APP_DIR"
fi

# 3. Kill switch.
if [ -f "$KILL_FILE" ]; then
    alert "kill_switch" "kill switch active at $KILL_FILE"
fi

# 4. State DB sanity — circuit breaker, last trade.
if [ -f "$STATE_DB" ] && command -v sqlite3 >/dev/null 2>&1; then
    DB_OUT=$(sqlite3 "$STATE_DB" \
        "SELECT 'breaker=' || COALESCE((SELECT value FROM meta WHERE key='candle_breaker_tripped'), '0'); \
         SELECT 'ts=' || COALESCE((SELECT MAX(timestamp) FROM trades), '0');" 2>/dev/null || echo "")
    BREAKER=$(echo "$DB_OUT" | sed -n 's/^breaker=//p')
    LAST_TRADE_TS=$(echo "$DB_OUT" | sed -n 's/^ts=//p')
    if [ "$BREAKER" = "1" ]; then
        alert "circuit_breaker" "candle circuit breaker is tripped — manual reset required"
    fi

    LAST_TRADE_TS=${LAST_TRADE_TS:-0}
    LAST_TRADE_TS=${LAST_TRADE_TS%.*}
    if [ "$LAST_TRADE_TS" != "0" ]; then
        AGE=$((now - LAST_TRADE_TS))
        MAX_AGE=$((INACTIVE_HOURS * 3600))
        if [ "$AGE" -gt "$MAX_AGE" ]; then
            HOURS=$((AGE / 3600))
            alert "no_trades" "no trades for ${HOURS}h (limit ${INACTIVE_HOURS}h)"
        fi
    fi
fi

# 5. Disk-pressure on logs/.
LOGS_SIZE=$(du -sm "$APP_DIR/logs" 2>/dev/null | awk '{print $1}')
if [ -n "$LOGS_SIZE" ] && [ "$LOGS_SIZE" -gt "$LOGS_LIMIT_MB" ]; then
    alert "logs_full" "logs/ at ${LOGS_SIZE}MB > ${LOGS_LIMIT_MB}MB cap — rotate or trim"
fi

exit 0
