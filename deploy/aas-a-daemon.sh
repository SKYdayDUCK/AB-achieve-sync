#!/usr/bin/env bash
# Containers usually have no systemd: nohup + pidfile. Usage: aas-a-daemon.sh <BASE_DIR> start|stop|status|logs
set -euo pipefail
BASE=${1:?base dir}; CMD=${2:?start|stop|status|logs}
CONF="$BASE/aas-keys/a.yaml"; PIDF="$BASE/aas/aas-a.pid"; LOG="$BASE/aas/aas-a.log"
case "$CMD" in
  start) if [ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null; then echo "already running $(cat "$PIDF")"; exit 0; fi
         nohup "$BASE/aas-venv/bin/aas-a" --config "$CONF" run --interval "${AAS_INTERVAL:-15}" >>"$LOG" 2>&1 & echo $! > "$PIDF"; sleep 2
         kill -0 "$(cat "$PIDF")" && echo "started $(cat "$PIDF")" || { tail -20 "$LOG"; exit 1; } ;;
  stop)  [ -f "$PIDF" ] && kill "$(cat "$PIDF")" 2>/dev/null; rm -f "$PIDF"; echo stopped ;;
  status) if [ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null; then echo "running $(cat "$PIDF")"; else echo "not running"; fi
          "$BASE/aas-venv/bin/aas-a" --config "$CONF" status ;;
  logs)  tail -n 50 "$LOG" ;;
esac
