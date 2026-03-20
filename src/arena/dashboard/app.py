from __future__ import annotations

from pathlib import Path
from typing import Any

# Load .env before any other imports so env vars are available throughout
from arena.env import load_local_env
load_local_env()

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn

from .queries import DashboardQueries, parse_dt, pct_change, relative_time


PROJECT_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE_DIR = PROJECT_ROOT / "src" / "arena" / "dashboard" / "templates"
STATIC_DIR = PROJECT_ROOT / "src" / "arena" / "dashboard" / "static"

app = FastAPI(title="Arena Dashboard")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))
dashboard = DashboardQueries()

NAV_ITEMS = [
    {"label": "Overview", "path": "/", "api_path": "/api/overview", "page": "overview"},
    {"label": "Live Positions", "path": "/positions", "api_path": "/api/positions", "page": "positions"},
    {"label": "Orders", "path": "/orders", "api_path": "/api/orders", "page": "orders"},
    {"label": "Decision Log", "path": "/decisions", "api_path": "/api/decisions", "page": "decisions"},
    {"label": "Execution Funnel", "path": "/execution-funnel", "api_path": "/api/execution-funnel", "page": "execution_funnel"},
    {"label": "Research Pipeline", "path": "/research-pipeline", "api_path": "/api/research-pipeline", "page": "research_pipeline"},
    {"label": "Discovery", "path": "/discovery", "api_path": "/api/discovery", "page": "discovery"},
    {"label": "Strategy Performance", "path": "/performance", "api_path": "/api/performance", "page": "performance"},
    {"label": "Forecast Accuracy", "path": "/forecast-accuracy", "api_path": "/api/forecast-accuracy", "page": "forecast_accuracy"},
    {"label": "Calibration", "path": "/calibration", "api_path": "/api/calibration", "page": "calibration"},
    {"label": "System Health", "path": "/health", "api_path": "/api/health", "page": "health"},
]

templates.env.filters["money"] = lambda value: "n/a" if value is None else f"${float(value):,.2f}"
templates.env.filters["money0"] = lambda value: "n/a" if value is None else f"${float(value):,.0f}"
templates.env.filters["pct"] = lambda value: "n/a" if value is None else f"{float(value) * 100:.1f}%"
templates.env.filters["pct_points"] = lambda value: "n/a" if value is None else f"{float(value):.1f}%"
templates.env.filters["bps"] = lambda value: "n/a" if value is None else f"{int(value)} bps"
templates.env.filters["num"] = lambda value: "n/a" if value is None else f"{float(value):,.3f}".rstrip("0").rstrip(".")
templates.env.filters["intcomma"] = lambda value: "n/a" if value is None else f"{int(value):,}"
templates.env.filters["relative_time"] = lambda value: relative_time(parse_dt(value))
templates.env.filters["timestamp"] = lambda value: parse_dt(value).strftime("%Y-%m-%d %H:%M:%S UTC") if parse_dt(value) else "n/a"
templates.env.filters["pct_change"] = pct_change


def base_context(request: Request, page: str, content_template: str, content_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "request": request,
        "nav_items": NAV_ITEMS,
        "current_page": page,
        "topbar": dashboard.get_topbar(),
        "content_template": content_template,
        "content": content_data,
    }


def render_full(request: Request, page: str, template_name: str, content_data: dict[str, Any]) -> HTMLResponse:
    return templates.TemplateResponse(
        "full_page.html",
        base_context(request, page, template_name, content_data),
    )


def render_partial(request: Request, template_name: str, content_data: dict[str, Any]) -> HTMLResponse:
    return templates.TemplateResponse(
        template_name,
        {
            "request": request,
            "current_page": request.query_params.get("page", ""),
            "content": content_data,
        },
    )


@app.get("/", response_class=HTMLResponse)
def overview_page(request: Request) -> HTMLResponse:
    return render_full(request, "overview", "partials/overview_page.html", dashboard.get_overview())


@app.get("/overview", response_class=HTMLResponse)
def overview_alias(request: Request) -> HTMLResponse:
    return overview_page(request)


@app.get("/positions", response_class=HTMLResponse)
def positions_page(request: Request) -> HTMLResponse:
    return render_full(request, "positions", "partials/positions_page.html", dashboard.get_positions_page())


@app.get("/orders", response_class=HTMLResponse)
def orders_page(request: Request) -> HTMLResponse:
    return render_full(request, "orders", "partials/orders_page.html", dashboard.get_orders_page())


@app.get("/decisions", response_class=HTMLResponse)
def decisions_page(
    request: Request,
    strategy: str = Query("all"),
    action_filter: str = Query("all"),
    date_range: str = Query("today"),
) -> HTMLResponse:
    data = dashboard.get_decision_log(strategy=strategy, action_filter=action_filter, date_range=date_range)
    return render_full(request, "decisions", "partials/decision_log_page.html", data)


@app.get("/execution-funnel", response_class=HTMLResponse)
def execution_funnel_page(
    request: Request,
    strategy: str = Query("all"),
    hours: int = Query(24),
) -> HTMLResponse:
    data = dashboard.get_execution_funnel_page(strategy=strategy, hours=hours)
    return render_full(request, "execution_funnel", "partials/execution_funnel_page.html", data)


@app.get("/performance", response_class=HTMLResponse)
def performance_page(request: Request) -> HTMLResponse:
    return render_full(request, "performance", "partials/performance_page.html", dashboard.get_strategy_performance())


@app.get("/research-pipeline", response_class=HTMLResponse)
def research_pipeline_page(
    request: Request,
    strategy: str = Query("all"),
    endpoint: str = Query("all"),
    date_range: str = Query("today"),
) -> HTMLResponse:
    data = dashboard.get_research_pipeline(strategy=strategy, endpoint=endpoint, date_range=date_range)
    return render_full(request, "research_pipeline", "partials/research_pipeline_page.html", data)


@app.get("/discovery", response_class=HTMLResponse)
def discovery_page(request: Request) -> HTMLResponse:
    return render_full(request, "discovery", "partials/discovery_page.html", dashboard.get_discovery_page())


@app.get("/forecast-accuracy", response_class=HTMLResponse)
def forecast_accuracy_page(request: Request) -> HTMLResponse:
    return render_full(request, "forecast_accuracy", "partials/forecast_accuracy_page.html", dashboard.get_forecast_accuracy())


@app.get("/calibration", response_class=HTMLResponse)
def calibration_page(request: Request) -> HTMLResponse:
    return render_full(request, "calibration", "partials/calibration_page.html", dashboard.get_calibration())


@app.get("/health", response_class=HTMLResponse)
def health_page(request: Request) -> HTMLResponse:
    return render_full(request, "health", "partials/health_page.html", dashboard.get_system_health())


@app.get("/api/topbar", response_class=HTMLResponse)
def api_topbar(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "partials/topbar.html",
        {
            "request": request,
            "topbar": dashboard.get_topbar(),
        },
    )


@app.get("/api/overview", response_class=HTMLResponse)
def api_overview(request: Request) -> HTMLResponse:
    return render_partial(request, "partials/overview_page.html", dashboard.get_overview())


@app.get("/api/positions", response_class=HTMLResponse)
def api_positions(request: Request) -> HTMLResponse:
    return render_partial(request, "partials/positions_page.html", dashboard.get_positions_page())


@app.get("/api/orders", response_class=HTMLResponse)
def api_orders(request: Request) -> HTMLResponse:
    return render_partial(request, "partials/orders_page.html", dashboard.get_orders_page())


@app.get("/api/decisions", response_class=HTMLResponse)
def api_decisions(
    request: Request,
    strategy: str = Query("all"),
    action_filter: str = Query("all"),
    date_range: str = Query("today"),
) -> HTMLResponse:
    data = dashboard.get_decision_log(strategy=strategy, action_filter=action_filter, date_range=date_range)
    return render_partial(request, "partials/decision_log_page.html", data)


@app.get("/api/execution-funnel", response_class=HTMLResponse)
def api_execution_funnel(
    request: Request,
    strategy: str = Query("all"),
    hours: int = Query(24),
) -> HTMLResponse:
    data = dashboard.get_execution_funnel_page(strategy=strategy, hours=hours)
    return render_partial(request, "partials/execution_funnel_page.html", data)


@app.get("/api/decision/{decision_id}", response_class=HTMLResponse)
def api_decision_detail(request: Request, decision_id: str) -> HTMLResponse:
    detail = dashboard.get_decision_detail(decision_id)
    return templates.TemplateResponse(
        "partials/decision_detail.html",
        {
            "request": request,
            "detail": detail,
        },
    )


@app.get("/api/performance", response_class=HTMLResponse)
def api_performance(request: Request) -> HTMLResponse:
    return render_partial(request, "partials/performance_page.html", dashboard.get_strategy_performance())


@app.get("/api/research-pipeline", response_class=HTMLResponse)
def api_research_pipeline(
    request: Request,
    strategy: str = Query("all"),
    endpoint: str = Query("all"),
    date_range: str = Query("today"),
) -> HTMLResponse:
    data = dashboard.get_research_pipeline(strategy=strategy, endpoint=endpoint, date_range=date_range)
    return render_partial(request, "partials/research_pipeline_page.html", data)


@app.get("/api/discovery", response_class=HTMLResponse)
def api_discovery(request: Request) -> HTMLResponse:
    return render_partial(request, "partials/discovery_page.html", dashboard.get_discovery_page())


@app.get("/api/research/{research_id}", response_class=HTMLResponse)
def api_research_detail(request: Request, research_id: int) -> HTMLResponse:
    detail = dashboard.get_research_detail(research_id)
    return templates.TemplateResponse(
        "partials/research_detail.html",
        {
            "request": request,
            "detail": detail,
        },
    )


@app.get("/api/forecast-accuracy", response_class=HTMLResponse)
def api_forecast_accuracy(request: Request) -> HTMLResponse:
    return render_partial(request, "partials/forecast_accuracy_page.html", dashboard.get_forecast_accuracy())


@app.get("/api/calibration", response_class=HTMLResponse)
def api_calibration(request: Request) -> HTMLResponse:
    return render_partial(request, "partials/calibration_page.html", dashboard.get_calibration())


@app.get("/api/health", response_class=HTMLResponse)
def api_health(request: Request) -> HTMLResponse:
    return render_partial(request, "partials/health_page.html", dashboard.get_system_health())


@app.get("/api/health/logs", response_class=HTMLResponse)
def api_health_logs(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "partials/health_log_viewer.html",
        {"request": request, "log_tail": dashboard.get_system_health()["log_tail"]},
    )


@app.post("/api/reset-paper-trading")
def api_reset_paper_trading() -> JSONResponse:
    from arena.engine.paper_reset import reset_paper_trading
    try:
        summary = reset_paper_trading(dashboard.db_path, reason="dashboard reset")
        return JSONResponse(content={"status": "ok", **summary})
    except Exception as exc:
        return JSONResponse(content={"status": "error", "error": str(exc)}, status_code=500)


if __name__ == "__main__":
    uvicorn.run("arena.dashboard.app:app", host="127.0.0.1", port=8050, reload=False)
