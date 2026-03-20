# Architecture

## System Overview
Arena is a Python trading and calibration system that scans prediction markets, runs strategy-specific analysis, enriches candidates with research and weather context, writes decisions and executions into SQLite, settles resolved markets, and feeds both forecast scoring and a live dashboard. Its external research layer is Nexus, a TypeScript service that handles web search, source crawling, probability synthesis, and market-specific response shaping.

## Service Map
| Service | Role | Default Port | Key Files |
| --- | --- | --- | --- |
| Arena Python | Market scan, strategy execution, paper/live execution, settlement, dashboard | Dashboard on `8050` when run directly | `src/arena/main.py`, `src/arena/dashboard/app.py` |
| Nexus TypeScript | `/api/v1/research` and `/api/v1/market-research` synthesis/search API | `3001` | `services/nexus/src/market-research.ts` |
| SearXNG | Meta-search backend for Nexus | `8081` host -> `8080` container | `services/nexus/docker-compose.yml` |
| Crawl4AI | Page extraction and markdown fitting for Nexus | `11235` | `services/nexus/docker-compose.yml` |
| Vane | Supporting search/synthesis service in the Nexus stack | `3000` | `services/nexus/docker-compose.yml` |
| Colima Docker | Container runtime used for the external Perplexia/Nexus stack | N/A | `/Volumes/SrijanExt/Code/perplexia/docker-compose.yml` |

## Directory Structure
- `src/arena/`: core Python app
- `src/arena/adapters/`: exchange, search, and LLM clients
- `src/arena/analytics/`: post-trade and performance analytics helpers
- `src/arena/calibration/`: CRPS tracking, resolution hooks, calibration feedback
- `src/arena/dashboard/`: FastAPI dashboard app, queries, Jinja templates, static assets
- `src/arena/data_sources/`: weather ensemble logic, station observations, FourCastNet
- `src/arena/engine/`: execution, settlement, portfolio accounting
- `src/arena/exchanges/`: venue-specific execution adapters
- `src/arena/export/`: CLI and spreadsheet export paths
- `src/arena/filters/`: pre-trade filters such as spread gating
- `src/arena/intelligence/`: info packet builder, Nexus research integration, shared types
- `src/arena/risk/`: risk manager and exposure checks
- `src/arena/strategies/`: active strategy implementations
- `services/nexus/`: TypeScript research service and its Docker config
- `scripts/`: operational and support scripts
- `config/`: app, provider, and strategy TOML configuration
- `data/`: SQLite database, caches, exports, and runtime artifacts

## Data Flow
1. `scan_markets()` in [main.py](/Volumes/SrijanExt/Code/finance/trading-test/src/arena/main.py) refreshes venue markets into SQLite.
2. `run_strategy_once()` selects an enabled strategy and asks it to generate a decision.
3. Strategy packet builders use `info_packet.py` to assemble market context, including search results and structured Nexus market research.
4. `research.py` sends the market payload to Nexus `/api/v1/market-research`, which may search SearXNG, crawl sources with Crawl4AI, and synthesize a probability with MiniMax/OpenRouter.
5. Arena stores the research response in `research_log`, including report text, sources, edge assessment, cache flags, and reasoning trace.
6. Strategies emit a decision; `execute_decision()` runs paper or venue execution and records positions/executions.
7. `SettlementEngine` resolves finished markets and triggers resolution hooks.
8. Calibration tools score outcomes, update Brier/CRPS history, and feed future calibration context back into the research payload.

## Key Integrations
- Weather ensemble: `weather_ensemble.py` produces forecast means, sigmas, source blends, and bias-aware weather context.
- FourCastNet: `nvidia_fourcastnet.py` adds NVIDIA forecast support for weather-driven markets.
- Kalshi: adapter and exchange layers exist for market discovery and execution, gated by config/env flags.
- Spread filter: `spread_filter.py` blocks thin or wide-spread opportunities before execution.
- Research pipeline: `info_packet.py`, `research.py`, and `research_log` capture what Nexus was asked, what it returned, and whether that research influenced a decision.
- Dashboard: `src/arena/dashboard/` surfaces research rows, decisions, calibration, forecast accuracy, positions, and system health.

## Configuration
- `.env` is organized into API keys, Nexus service settings, upstream service URLs, trading/risk flags, and optional Kalshi credentials.
- `config/arena.toml` controls database path, scheduler intervals, venue settings, and arena-wide execution knobs.
- `config/models.toml` and `services/nexus/src/config/model-routes.ts` define provider/model routing between decomposition, search synthesis, and probability synthesis stages.
- Strategy TOMLs under `config/strategies/` control enablement, search behavior, model settings, and risk rules per strategy.

## Docker Setup
- The in-repo compose file at [services/nexus/docker-compose.yml](/Volumes/SrijanExt/Code/finance/trading-test/services/nexus/docker-compose.yml) defines SearXNG, Vane, Crawl4AI, and the deep-research service on ports `8081`, `3000`, `11235`, and `3001`.
- The external runtime stack lives under `/Volumes/SrijanExt/Code/perplexia/` and is intended to be started with Colima-backed Docker.
- Arena itself runs outside Docker in the local Python environment and talks to Nexus over `NEXUS_URL`, normally `http://localhost:3001`.
