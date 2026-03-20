# Minimum Running Services

This repo has two layers:

1. Arena itself in [/Volumes/SrijanExt/Code/finance/trading-test](/Volumes/SrijanExt/Code/finance/trading-test)
2. The live Nexus Docker stack in [/Volumes/SrijanExt/Code/perplexia](/Volumes/SrijanExt/Code/perplexia)

Important: the Nexus service that Arena uses on port `3001` is the Dockerized external copy from `/Volumes/SrijanExt/Code/perplexia`, not the source under `services/nexus/`.

## What Should Be Running All The Time

Core services:

- `colima` / Docker runtime
- `nexus-searxng`
- `nexus-crawl4ai`
- `nexus-vane`
- `nexus-deep-research`
- Arena scheduler: `python -m arena.main scheduler`
- Arena dashboard: `python -m arena.dashboard.app`

Optional service:

- Polymarket arbitrage bot in simulation mode from [/Volumes/SrijanExt/Code/finance/trading-test/services/polymarket-arbitrage](/Volumes/SrijanExt/Code/finance/trading-test/services/polymarket-arbitrage)

## Current URLs

- Dashboard: [http://127.0.0.1:8050/](http://127.0.0.1:8050/)
- Nexus health: [http://127.0.0.1:3001/api/v1/health](http://127.0.0.1:3001/api/v1/health)
- SearXNG: [http://127.0.0.1:8081](http://127.0.0.1:8081)
- Crawl4AI: [http://127.0.0.1:11235/health](http://127.0.0.1:11235/health)

## Fast Health Check

Run from [/Volumes/SrijanExt/Code/finance/trading-test](/Volumes/SrijanExt/Code/finance/trading-test):

```bash
lsof -nP -iTCP -sTCP:LISTEN | egrep ':(3001|8050|8081|11235)\b'
curl -s http://127.0.0.1:3001/api/v1/health
curl -s http://127.0.0.1:8050/ | head
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
```

## How To Restart The Core Stack

### 1. Docker / Nexus stack

Run from [/Volumes/SrijanExt/Code/perplexia](/Volumes/SrijanExt/Code/perplexia):

```bash
docker compose up -d deep-research crawl4ai searxng vane
```

If you changed Nexus source code in `services/nexus`, sync it into the external repo first, then rebuild:

```bash
cp /Volumes/SrijanExt/Code/finance/trading-test/services/nexus/src/market-research.ts \
   /Volumes/SrijanExt/Code/perplexia/deep-research/src/market-research.ts

cp /Volumes/SrijanExt/Code/finance/trading-test/services/nexus/src/lib/minimax-client.ts \
   /Volumes/SrijanExt/Code/perplexia/deep-research/src/lib/minimax-client.ts

docker compose up -d --build deep-research
```

### 2. Arena scheduler

Run from [/Volumes/SrijanExt/Code/finance/trading-test](/Volumes/SrijanExt/Code/finance/trading-test):

```bash
env PYTHONPATH=/Volumes/SrijanExt/Code/finance/trading-test/src \
  /Volumes/SrijanExt/Code/finance/trading-test/.venv/bin/python -m arena.main scheduler
```

### 3. Arena dashboard

Run from [/Volumes/SrijanExt/Code/finance/trading-test](/Volumes/SrijanExt/Code/finance/trading-test):

```bash
env PYTHONPATH=/Volumes/SrijanExt/Code/finance/trading-test/src \
  /Volumes/SrijanExt/Code/finance/trading-test/.venv/bin/python -m arena.dashboard.app
```

### 4. Polymarket arbitrage bot

Run from [/Volumes/SrijanExt/Code/finance/trading-test/services/polymarket-arbitrage](/Volumes/SrijanExt/Code/finance/trading-test/services/polymarket-arbitrage):

```bash
./run.sh
```

Keep `PRODUCTION=false` in [.env](/Volumes/SrijanExt/Code/finance/trading-test/services/polymarket-arbitrage/.env) unless you explicitly want live trading.

## Logs To Watch

- Arena scheduler: [/Volumes/SrijanExt/Code/finance/trading-test/data/logs/arena.log](/Volumes/SrijanExt/Code/finance/trading-test/data/logs/arena.log)
- Dashboard: [/Volumes/SrijanExt/Code/finance/trading-test/data/logs/dashboard.log](/Volumes/SrijanExt/Code/finance/trading-test/data/logs/dashboard.log)
- Arbitrage wrapper: [/Volumes/SrijanExt/Code/finance/trading-test/data/logs/arbitrage-wrapper.log](/Volumes/SrijanExt/Code/finance/trading-test/data/logs/arbitrage-wrapper.log)
- Arbitrage bot live log: [/Volumes/SrijanExt/Code/finance/trading-test/services/polymarket-arbitrage/arbitrage.log](/Volumes/SrijanExt/Code/finance/trading-test/services/polymarket-arbitrage/arbitrage.log)

Useful tails:

```bash
tail -f /Volumes/SrijanExt/Code/finance/trading-test/data/logs/arena.log
tail -f /Volumes/SrijanExt/Code/finance/trading-test/services/polymarket-arbitrage/arbitrage.log
docker logs -f nexus-deep-research
```

## Notes

- The old [scripts/start_all.sh](/Volumes/SrijanExt/Code/finance/trading-test/scripts/start_all.sh) assumes Nexus runs from the in-repo `services/nexus` folder. That is not the live setup right now.
- If the Dashboard nav looks stale or new pages 404, restart the dashboard process.
- If Nexus fixes compile locally but are not reflected on port `3001`, rebuild the external Docker service in `/Volumes/SrijanExt/Code/perplexia`.
