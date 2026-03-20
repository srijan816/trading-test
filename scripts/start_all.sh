#!/bin/bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$REPO_ROOT/data/logs"
PID_DIR="$REPO_ROOT/data/pids"
PYTHON_BIN="$REPO_ROOT/.venv/bin/python"

if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="python3"
fi

mkdir -p "$LOG_DIR" "$PID_DIR"

echo "=== Arena Trading System — Starting All Services ==="
echo "Repo root: $REPO_ROOT"
echo ""

if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    # shellcheck source=/dev/null
    source "$REPO_ROOT/.env"
    set +a
fi

echo "[1/5] Checking SearXNG..."
if curl -s "${SEARXNG_URL:-http://localhost:8081}/healthz" > /dev/null 2>&1; then
    echo "  SearXNG: OK (${SEARXNG_URL:-http://localhost:8081})"
else
    echo "  SearXNG: NOT RUNNING — start with: docker start searxng"
    echo "  (continuing without SearXNG — Nexus search may fail)"
fi

echo "[2/5] Checking Crawl4AI..."
if curl -s "${CRAWL4AI_URL:-http://localhost:11235}/health" > /dev/null 2>&1; then
    echo "  Crawl4AI: OK (${CRAWL4AI_URL:-http://localhost:11235})"
else
    echo "  Starting Crawl4AI..."
    docker start crawl4ai 2>/dev/null || \
    docker run -d -p 11235:11235 --name crawl4ai --shm-size=1g \
        unclecode/crawl4ai:latest > /dev/null
    sleep 3
    echo "  Crawl4AI: started (${CRAWL4AI_URL:-http://localhost:11235})"
fi

echo "[3/5] Starting Nexus..."
cd "$REPO_ROOT/services/nexus"
npm run build > /dev/null 2>&1 || true
nohup npx next start -p "${NEXUS_PORT:-3001}" > "$LOG_DIR/nexus.log" 2>&1 &
NEXUS_PID=$!
echo "  Nexus: PID $NEXUS_PID (port ${NEXUS_PORT:-3001})"

echo "[4/5] Starting Arbitrage Bot..."
cd "$REPO_ROOT/services/polymarket-arbitrage"
if [ -f "dist/main.js" ]; then
    nohup npm start > "$LOG_DIR/arbitrage.log" 2>&1 &
    ARB_PID=$!
    echo "  Arbitrage: PID $ARB_PID (simulation mode)"
else
    ARB_PID=""
    echo "  Arbitrage: not built — run 'npm run build' first"
fi

echo "[5/5] Starting Arena..."
cd "$REPO_ROOT"

echo "  Waiting for Nexus health check..."
for i in $(seq 1 30); do
    if curl -s "http://localhost:${NEXUS_PORT:-3001}/api/v1/health" > /dev/null 2>&1; then
        echo "  Nexus: ready"
        break
    fi
    sleep 1
done

nohup env PYTHONPATH="$REPO_ROOT/src" "$PYTHON_BIN" -m arena.main scheduler > "$LOG_DIR/arena.log" 2>&1 &
ARENA_PID=$!
echo "  Arena: PID $ARENA_PID"

echo ""
echo "=== All Services Started ==="
echo "  SearXNG:   ${SEARXNG_URL:-http://localhost:8081}"
echo "  Crawl4AI:  ${CRAWL4AI_URL:-http://localhost:11235}"
echo "  Nexus:     http://localhost:${NEXUS_PORT:-3001}"
echo "  Arena:     PID $ARENA_PID"
echo "  Arbitrage: PID ${ARB_PID:-not started}"
echo ""
echo "Logs: $LOG_DIR"
echo "Stop all: kill $NEXUS_PID ${ARB_PID:-} $ARENA_PID"

printf '%s\n' "$NEXUS_PID" > "$PID_DIR/nexus.pid"
printf '%s\n' "${ARB_PID:-}" > "$PID_DIR/arbitrage.pid"
printf '%s\n' "$ARENA_PID" > "$PID_DIR/arena.pid"
