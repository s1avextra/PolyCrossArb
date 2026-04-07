#!/bin/bash
SERVICES="polycrossarb-candle polycrossarb-arb polycrossarb-weather polycrossarb-rust"
WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"
alert() {
    logger -t polycrossarb "HEALTH: $1"
    [ -n "$WEBHOOK_URL" ] && curl -s -X POST "$WEBHOOK_URL" -H 'Content-Type: application/json' -d "{\"text\": \"$1\"}" >/dev/null 2>&1 || true
}
for svc in $SERVICES; do
    systemctl is-active --quiet "$svc" 2>/dev/null || { alert ":warning: $svc DOWN — restarting"; systemctl restart "$svc" 2>/dev/null; }
done
HTTP=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null || echo "000")
[ "$HTTP" != "200" ] && alert ":x: Health endpoint returned $HTTP"
DISK=$(df /opt/polycrossarb 2>/dev/null | awk 'NR==2{print $5}' | tr -d '%')
[ -n "$DISK" ] && [ "$DISK" -gt 90 ] && alert ":warning: Disk ${DISK}%"
