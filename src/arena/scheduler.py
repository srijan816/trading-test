from __future__ import annotations

import asyncio
from functools import partial
from typing import Callable

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from arena.config import AppConfig
from arena.data_sources.nvidia_fourcastnet import get_polling_schedule


def build_scheduler(app_config: AppConfig, jobs: dict[str, Callable[..., object]]) -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone=app_config.arena.get("timezone", "UTC"))
    scheduler.add_job(jobs["scan_markets"], "interval", minutes=int(app_config.scheduler["scan_markets_minutes"]), id="scan_markets", replace_existing=True)
    scheduler.add_job(jobs["poll_resolutions"], "interval", minutes=int(app_config.scheduler["poll_resolutions_minutes"]), id="poll_resolutions", replace_existing=True)
    scheduler.add_job(jobs["mark_to_market"], "interval", minutes=int(app_config.scheduler["mark_to_market_minutes"]), id="mark_to_market", replace_existing=True)
    if "run_discovery_scout" in jobs:
        scheduler.add_job(
            jobs["run_discovery_scout"],
            "interval",
            minutes=int(app_config.scheduler.get("discovery_scout_minutes", 60)),
            id="run_discovery_scout",
            replace_existing=True,
        )
    if "monitor_limit_orders" in jobs:
        scheduler.add_job(
            jobs["monitor_limit_orders"],
            "interval",
            seconds=30,
            id="monitor_limit_orders",
            replace_existing=True,
        )
    scheduler.add_job(jobs["export_dashboard"], "interval", minutes=int(app_config.scheduler["export_dashboard_minutes"]), id="export_dashboard", replace_existing=True)
    scheduler.add_job(jobs["check_manual_responses"], "interval", minutes=int(app_config.scheduler["check_manual_responses_minutes"]), id="check_manual_responses", replace_existing=True)
    if "monitor_intraday" in jobs:
        scheduler.add_job(jobs["monitor_intraday"], "interval", minutes=15, id="monitor_intraday", replace_existing=True)
    if "manage_open_positions" in jobs:
        scheduler.add_job(jobs["manage_open_positions"], "interval", minutes=15, id="manage_open_positions", replace_existing=True)
    if "capture_daily_snapshots" in jobs and app_config.scheduler.get("daily_snapshot_cron"):
        scheduler.add_job(
            jobs["capture_daily_snapshots"],
            CronTrigger.from_crontab(str(app_config.scheduler["daily_snapshot_cron"]), timezone=scheduler.timezone),
            id="capture_daily_snapshots",
            replace_existing=True,
        )
    if "poll_fourcastnet" in jobs:
        for schedule_time in get_polling_schedule():
            hour, minute = schedule_time.split(":")
            scheduler.add_job(
                jobs["poll_fourcastnet"],
                CronTrigger(hour=int(hour), minute=int(minute), timezone=scheduler.timezone),
                id=f"poll_fourcastnet_{hour}{minute}",
                replace_existing=True,
            )
    if "run_weekly_retrospective" in jobs and app_config.scheduler.get("weekly_retrospective_cron"):
        scheduler.add_job(
            jobs["run_weekly_retrospective"],
            CronTrigger.from_crontab(str(app_config.scheduler["weekly_retrospective_cron"]), timezone=scheduler.timezone),
            id="run_weekly_retrospective",
            replace_existing=True,
        )
    if "run_monthly_meta_prompt" in jobs and app_config.scheduler.get("monthly_meta_cron"):
        scheduler.add_job(
            jobs["run_monthly_meta_prompt"],
            CronTrigger.from_crontab(str(app_config.scheduler["monthly_meta_cron"]), timezone=scheduler.timezone),
            id="run_monthly_meta_prompt",
            replace_existing=True,
        )
    for strategy_id, strategy in app_config.strategies.items():
        if not bool(strategy.strategy.get("enabled", True)):
            continue
        cadence = int(strategy.strategy.get("schedule", {}).get("cadence_minutes", 60))
        scheduler.add_job(jobs["run_strategy"], "interval", minutes=cadence, kwargs={"strategy_id": strategy_id}, id=f"run_strategy_{strategy_id}", replace_existing=True)
    return scheduler
