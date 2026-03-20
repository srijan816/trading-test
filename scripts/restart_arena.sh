#!/bin/bash
# Restart Arena dashboard and scheduler processes.
# Run with: bash scripts/restart_arena.sh
#
# This kills any running dashboard (port 8050) and scheduler processes,
# then restarts both from the repo root.

set -euo pipefail
cd /Volumes/SrijanExt/Code/finance/trading-test

# ---------------------------------------------------------------------------
# Kill dashboard (uvicorn on port 8050)
# ---------------------------------------------------------------------------
DASH_PID=$(lsof -ti:8050 2>/dev/null || true)
if [ -n "$DASH_PID" ]; then
    echo "[$(date)] Killing dashboard (PID $DASH_PID) on port 8050..."
    kill $DASH_PID 2>/dev/null || true
    sleep 1
    # Force kill if still alive
    if kill -0 $DASH_PID 2>/dev/null; then
        kill -9 $DASH_PID 2>/dev/null || true
    fi
    echo "[$(date)] Dashboard stopped."
else
    echo "[$(date)] No dashboard process found on port 8050."
fi

# ---------------------------------------------------------------------------
# Kill scheduler ("arena scheduler" in command line)
# ---------------------------------------------------------------------------
SCHED_PIDS=$(pgrep -f "arena scheduler" 2>/dev/null || true)
if [ -n "$SCHED_PIDS" ]; then
    echo "[$(date)] Killing scheduler (PIDs: $SCHED_PIDS)..."
    for PID in $SCHED_PIDS; do
        kill $PID 2>/dev/null || true
        sleep 1
        if kill -0 $PID 2>/dev/null; then
            kill -9 $PID 2>/dev/null || true
        fi
    done
    echo "[$(date)] Scheduler stopped."
else
    echo "[$(date)] No scheduler process found."
fi

# ---------------------------------------------------------------------------
# Wait for ports to be released
# ---------------------------------------------------------------------------
echo "[$(date)] Waiting 2s for ports to be released..."
sleep 2

# ---------------------------------------------------------------------------
# Source .env to pick up env vars for this session
# ---------------------------------------------------------------------------
if [ -f .env ]; then
    echo "[$(date)] Sourcing .env..."
    set -a
    source .env
    set +a
fi

# ---------------------------------------------------------------------------
# Start dashboard
# ---------------------------------------------------------------------------
mkdir -p logs
export PYTHONPATH=src
DASH_LOG="/tmp/arena-dashboard.log"
echo "[$(date)] Starting dashboard on 127.0.0.1:8050 -> $DASH_LOG"
PYTHONPATH=src .venv/bin/python -m uvicorn arena.dashboard.app:app \
    --host 127.0.0.1 \
    --port 8050 \
    >> "$DASH_LOG" 2>&1 &
DASH_NEW_PID=$!
echo "Dashboard PID: $DASH_NEW_PID"

# ---------------------------------------------------------------------------
# Start scheduler
# ---------------------------------------------------------------------------
SCHED_LOG="/tmp/arena-scheduler.log"
echo "[$(date)] Starting scheduler -> $SCHED_LOG"
PYTHONPATH=src .venv/bin/python -m arena scheduler \
    >> "$SCHED_LOG" 2>&1 &
SCHED_NEW_PID=$!
echo "Scheduler PID: $SCHED_NEW_PID"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Arena restarted ==="
echo "Dashboard: PID $DASH_NEW_PID (log: $DASH_LOG)"
echo "Scheduler: PID $SCHED_NEW_PID (log: $SCHED_LOG)"
echo "Dashboard should be live at: http://127.0.0.1:8050"
echo ""
echo "To follow dashboard logs: tail -f $DASH_LOG"
echo "To follow scheduler logs: tail -f $SCHED_LOG"
