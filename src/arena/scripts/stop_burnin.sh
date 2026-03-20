#!/bin/bash
# Stop the burn-in gracefully
cd /Volumes/SrijanExt/Code/finance/trading-test

PID_FILE="logs/burnin.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "No PID file found at $PID_FILE"
    echo "Checking for running processes..."
    ps aux | grep "run_burnin\\|arena scheduler" | grep -v grep
    exit 1
fi

PID=$(cat "$PID_FILE")
echo "Stopping burn-in (PID: $PID)..."

# Send SIGTERM to the process group
kill -TERM -$PID 2>/dev/null || kill -TERM $PID 2>/dev/null

# Wait up to 10 seconds for graceful shutdown
for i in $(seq 1 10); do
    if ! kill -0 $PID 2>/dev/null; then
        echo "Burn-in stopped gracefully after ${i}s"
        rm -f "$PID_FILE"
        exit 0
    fi
    sleep 1
done

# Force kill if still running
echo "Forcing shutdown..."
kill -9 -$PID 2>/dev/null || kill -9 $PID 2>/dev/null
rm -f "$PID_FILE"
echo "Burn-in force-stopped"
