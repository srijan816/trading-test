# Nexus Research Engine

Trading-focused research subsystem. Part of the Arena trading system.

## Standalone Development
```bash
cd services/nexus
cp ../../.env .env  # or create a local .env
npm install
npm run dev
```

## As Part of Arena
Started automatically by `scripts/start_all.sh`.

## Endpoints
- `POST /api/v1/research` — generic research
- `POST /api/v1/market-research` — structured probability output
- `GET /api/v1/health` — dependency-aware health check
