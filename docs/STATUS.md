# Status

| Component | Status | Key File | Notes |
| --- | --- | --- | --- |
| `algo_partition` | Active | `src/arena/main.py` | Wired into the strategy factory and not listed in `DISABLED_STRATEGIES`. |
| `algo_momentum` | Dead | `src/arena/main.py` | Explicitly disabled in `DISABLED_STRATEGIES` as `invalid model, do not run`. |
| `algo_forecast` | Active | `src/arena/strategies/algo_forecast.py` | Present in the live factory path and enabled in strategy config. |
| `algo_harvester` | Active | `src/arena/strategies/algo_harvester.py` | Present in the live factory path and enabled in strategy config. |
| `algo_meanrev` | Dead | `src/arena/main.py` | Explicitly disabled in `DISABLED_STRATEGIES` as `invalid model, do not run`. |
| `llm_analyst` | Active | `config/strategies/llm_analyst.toml` | LLM strategy config exists and is not in the hard-disabled set. |
| `llm_news_trader` | Active | `config/strategies/llm_news_trader.toml` | LLM strategy config exists and uses Nexus-backed search/research. |
| `weather_ensemble` | Active | `src/arena/data_sources/weather_ensemble.py` | Feeds weather probabilities, bias corrections, and calibration context. |
| `fourcastnet` | Simulating | `src/arena/data_sources/nvidia_fourcastnet.py` | Integrated as a forecast source, but operational value depends on NVIDIA credentials/runtime availability. |
| `station_observations` | Active | `src/arena/data_sources/station_observations.py` | Used for observed temperatures and resolution/CRPS backfill. |
| `polymarket` adapter | Active | `src/arena/adapters/polymarket.py` | Used for market ingestion and execution flow. |
| `kalshi` adapter | Inactive | `src/arena/adapters/kalshi.py` | Code is live, but env/config defaults leave Kalshi disabled. |
| `search_perplexia` | Active | `src/arena/adapters/search_perplexia.py` | Primary Python-side Nexus/Perplexia research client. |
| `spread_filter` | Active | `src/arena/filters/spread_filter.py` | Applied in the execution path to reject weak or illiquid markets. |
| `crps_tracker` | Active | `src/arena/calibration/crps_tracker.py` | Tracks forecast calibration for weather markets after settlement. |
| `resolution_hook` | Active | `src/arena/calibration/resolution_hook.py` | Scores settled markets and feeds Brier/CRPS outcomes. |
| Nexus `/research` | Active | `services/nexus/src/app/api/v1/research/route.ts` | Legacy free-form research endpoint still served and mirrored by `/api/research`. |
| Nexus `/market-research` | Active | `services/nexus/src/app/api/v1/market-research/route.ts` | Structured market research endpoint used by Arena. |
| Dashboard overview/positions/decisions/performance/forecast/calibration/health | Active | `src/arena/dashboard/app.py` | All pages are mounted in the FastAPI nav and backed by query helpers. |
| Dashboard research pipeline | Active | `src/arena/dashboard/templates/partials/research_pipeline_page.html` | Live research log view with row detail panel and decision-usage flags. |
| Arena scheduler/runner | Active | `src/arena/main.py` | Scans markets, runs strategies, executes decisions, and polls settlements. |
| SQLite persistence | Active | `src/arena/db.py` | Central store for markets, decisions, executions, resolutions, and research logs. |
| SearXNG | Active | `services/nexus/docker-compose.yml` | Search backend exposed on host port `8081`. |
| Crawl4AI | Active | `services/nexus/docker-compose.yml` | Content extraction backend exposed on host port `11235`. |
| Vane | Active | `services/nexus/docker-compose.yml` | Supporting search/synthesis service exposed on host port `3000`. |
| Colima Docker runtime | Active | `/Volumes/SrijanExt/Code/perplexia/docker-compose.yml` | External compose stack expected to run the Nexus services locally. |
