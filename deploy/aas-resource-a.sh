#!/usr/bin/env bash
set -Eeuo pipefail
ACTION="${1:-status}"
CONFIG="${2:?configuration YAML is required}"
PRESET="${3:-lora}"
PYTHON="${AAS_PYTHON:-/opt/conda/bin/python}"
SOURCE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME="${AAS_RUNTIME:-/localdisk-tmp/aas-resource-sync}"
mkdir -p "$RUNTIME"
PIDFILE="$RUNTIME/a.pid"
running() {
  [[ -f "$PIDFILE" ]] || return 1
  local proc; proc="$(cat "$PIDFILE")"
  [[ "$proc" =~ ^[0-9]+$ ]] && kill -0 "$proc" 2>/dev/null || return 1
  [[ -r "/proc/$proc/cmdline" ]] && tr '\0' '\n' < "/proc/$proc/cmdline" | grep -F -- "$CONFIG" >/dev/null
}
case "$ACTION" in
  start)
    if running; then echo 'AAS A already running'; exit 0; fi
    export PYTHONPATH="$SOURCE/src${PYTHONPATH:+:$PYTHONPATH}"
    nohup "$PYTHON" -c 'from aas.resources.cli import main_a; main_a()' --config "$CONFIG" --preset "$PRESET" run >"$RUNTIME/a.log" 2>"$RUNTIME/a.error.log" &
    echo "$!" > "$PIDFILE"
    echo "AAS A started pid=$!"
    ;;
  stop)
    if running; then kill "$(cat "$PIDFILE")"; fi
    ;;
  status)
    if running; then echo "AAS A running pid=$(cat "$PIDFILE")"; else echo 'AAS A stopped'; fi
    ;;
  *) echo 'Use start|stop|status CONFIG [images|files|lora]' >&2; exit 2;;
esac
