#!/bin/bash
# PolyCrossArb healthcheck — invoked by polycrossarb-healthcheck.timer.
# Checks: services up, HTTP /health, disk, kill switch present, recent
# trade activity, circuit breaker state, alerter cooldown.
set -uo pipefail

APP_DIR="${POLYCROSSARB_DIR:-/opt/polycrossarb}"
SERVICES="${POLYCROSSARB_SERVICES:-polycrossarb-candle polycrossarb-arb polycrossarb-weather polycrossarb-rust}"
WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"
KILL_FILE="${KILL_FILE:-/tmp/polycrossarb/KILL}"
STATE_DB="${STATE_DB:-$APP_DIR/logs/state.db}"
INACTIVE_HOURS="${INACTIVE_HOURS:-2}"

# Track per-category cooldowns so we don't spam.
COOLDOWN_DIR="${COOLDOWN_DIR:-/var/tmp/polycrossarb-healthcheck}"
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
    logger -t polycrossarb "HEALTH[$category]: $msg"
    if [ -n "$WEBHOOK_URL" ]; then
        curl -s -X POST "$WEBHOOK_URL" \
            -H 'Content-Type: application/json' \
            -d "{\"text\": \":heart: HEALTH[$category]: $msg\"}" \
            >/dev/null 2>&1 || true
    fi
}

# 1. Service liveness — restart and alert if any service is dead.
for svc in $SERVICES; do
    if ! systemctl is-active --quiet "$svc" 2>/dev/null; then
        alert "service_down" "$svc inactive — restarting"
        systemctl restart "$svc" 2>/dev/null || true
    fi
done

# 2. HTTP /health endpoint (if the bot exposes one).
HTTP=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null || echo "000")
if [ "$HTTP" != "200" ] && [ "$HTTP" != "000" ]; then
    alert "http_health" "endpoint returned $HTTP"
fi

# 3. Disk free.
DISK=$(df "$APP_DIR" 2>/dev/null | awk 'NR==2{gsub("%","",$5); print $5}')
if [ -n "$DISK" ] && [ "$DISK" -gt 90 ]; then
    alert "disk_full" "disk usage ${DISK}% on $APP_DIR"
fi

# 4. Kill switch — if present, *something* halted trading.
if [ -f "$KILL_FILE" ]; then
    alert "kill_switch" "kill switch active at $KILL_FILE"
fi

# 5. State DB sanity — circuit breaker, last trade, paper position count.
if [ -f "$STATE_DB" ] && command -v sqlite3 >/dev/null 2>&1; then
    BREAKER=$(sqlite3 "$STATE_DB" "SELECT value FROM meta WHERE key='candle_breaker_tripped'" 2>/dev/null || echo "")
    if [ "$BREAKER" = "1" ]; then
        alert "circuit_breaker" "candle circuit breaker is tripped — manual reset required"
    fi

    LAST_TRADE_TS=$(sqlite3 "$STATE_DB" "SELECT MAX(timestamp) FROM trades" 2>/dev/null || echo "0")
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

# 6. Disk-pressure on logs/.
LOGS_SIZE=$(du -sm "$APP_DIR/logs" 2>/dev/null | awk '{print $1}')
LOGS_LIMIT_MB="${LOGS_LIMIT_MB:-2048}"
if [ -n "$LOGS_SIZE" ] && [ "$LOGS_SIZE" -gt "$LOGS_LIMIT_MB" ]; then
    alert "logs_full" "logs/ at ${LOGS_SIZE}MB > ${LOGS_LIMIT_MB}MB cap — rotate or trim"
fi

exit 0
