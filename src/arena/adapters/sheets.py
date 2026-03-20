from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any
import json

import gspread

from arena.adapters.base import DashboardSink
from arena.config import ROOT
from arena.google_auth import authorized_gspread_client
from arena.models import CalibrationRow, CostRow


class GoogleSheetsSink(DashboardSink):
    def __init__(self, spreadsheet_id: str, credentials_env: str = "GOOGLE_APPLICATION_CREDENTIALS") -> None:
        self.spreadsheet_id = spreadsheet_id
        self.credentials_env = credentials_env
        self.state_path = ROOT / "data" / "google_sheets_state.json"

    def _client(self):
        return authorized_gspread_client()

    def _spreadsheet(self):
        client = self._client()
        spreadsheet_id = self.spreadsheet_id or self._load_spreadsheet_id()
        if spreadsheet_id:
            return client.open_by_key(spreadsheet_id)
        sheet = client.create("Arena Tracking")
        self._save_spreadsheet_id(sheet.id)
        return sheet

    async def export_leaderboard(self, snapshots: list[dict]) -> None:
        await self._update_tab("Leaderboard", snapshots)

    async def export_trade_log(self, executions: list[dict]) -> None:
        await self._update_tab("Trade Feed", executions)

    async def export_reasoning_log(self, decisions: list[dict]) -> None:
        await self._update_tab("LLM Reasoning", decisions)

    async def export_calibration(self, data: list[CalibrationRow]) -> None:
        await self._update_tab("Calibration", self._normalize_rows(data))

    async def export_costs(self, data: list[CostRow]) -> None:
        await self._update_tab("Costs", self._normalize_rows(data))

    async def _update_tab(self, title: str, rows: list[dict]) -> None:
        sheet = self._spreadsheet()
        try:
            worksheet = sheet.worksheet(title)
        except gspread.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=title, rows=100, cols=20)
        if not rows:
            worksheet.clear()
            worksheet.update("A1", [["empty"]])
            return
        headers = list(rows[0].keys())
        values = [headers] + [[self._sheet_value(row.get(header, "")) for header in headers] for row in rows]
        worksheet.clear()
        worksheet.update("A1", values)

    def _sheet_value(self, value: Any) -> str | int | float:
        if isinstance(value, (str, int, float)) or value is None:
            normalized = "" if value is None else value
        else:
            normalized = json.dumps(value, ensure_ascii=False, default=str)
        if isinstance(normalized, str) and len(normalized) > 45000:
            return normalized[:45000] + "...[truncated]"
        return normalized

    def _normalize_rows(self, rows: list[Any]) -> list[dict]:
        normalized: list[dict] = []
        for row in rows:
            if is_dataclass(row):
                normalized.append(asdict(row))
            elif isinstance(row, dict):
                normalized.append(row)
            else:
                normalized.append({"value": str(row)})
        return normalized

    def _load_spreadsheet_id(self) -> str:
        if self.state_path.exists():
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            return data.get("spreadsheet_id", "")
        return ""

    def _save_spreadsheet_id(self, spreadsheet_id: str) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps({"spreadsheet_id": spreadsheet_id}, indent=2), encoding="utf-8")
