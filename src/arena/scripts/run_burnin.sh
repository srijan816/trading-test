#!/bin/bash
# Arena burn-in: continuous paper trading for calibration data collection
# Run with: nohup bash src/arena/scripts/run_burnin.sh > logs/burnin.log 2>&1 &

set -euo pipefail
cd /Volumes/SrijanExt/Code/finance/trading-test

export PYTHONPATH=src
PYTHON_BIN=".venv/bin/python"

# Source .env so all env vars (RISK_*, NEXUS_*, etc.) are available to the scheduler
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

LOG_DIR="logs"
mkdir -p "$LOG_DIR"

echo "$(date): Starting Arena burn-in"
echo "$(date): Execution mode: paper"

# Verify paper mode before entering the scheduler loop.
MODE=$("$PYTHON_BIN" -c "
import tomllib
with open('config/arena.toml', 'rb') as f:
    c = tomllib.load(f)
print(c.get('execution', {}).get('mode', 'paper'))
")
if [ "$MODE" != "paper" ]; then
    echo "ABORT: execution mode is '$MODE', not 'paper'"
    exit 1
fi

# Main loop: run the scheduler. It owns market scans, strategy cadences,
# intraday monitoring, settlement checks, and daily snapshots.
while true; do
    echo "$(date): Starting scheduler cycle"
    if "$PYTHON_BIN" -m arena scheduler 2>&1 | tee -a "$LOG_DIR/scheduler_$(date +%Y%m%d).log"; then
        echo "$(date): Scheduler exited cleanly, restarting in 60s"
    else
        EXIT_CODE=${PIPESTATUS[0]}
        echo "$(date): Scheduler exited with code $EXIT_CODE, restarting in 60s"
    fi
    sleep 60
done
