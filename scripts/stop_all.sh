#!/bin/bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PID_DIR="$REPO_ROOT/data/pids"

echo "=== Stopping All Services ==="
for pidfile in "$PID_DIR"/*.pid; do
    if [ -f "$pidfile" ]; then
        pid="$(cat "$pidfile")"
        name="$(basename "$pidfile" .pid)"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            echo "  Stopped $name (PID $pid)"
        else
            echo "  $name: not running"
        fi
        rm -f "$pidfile"
    fi
done
echo "=== Done ==="
