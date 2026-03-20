from __future__ import annotations

from datetime import datetime, timezone
import re


def derive_event_group(question: str, category: str, venue: str, slug: str | None = None) -> str | None:
    lowered = question.lower()
    if category != "weather":
        return None
    city = _extract_city(question)
    event_date = _extract_date(question)
    metric = _extract_metric(lowered)
    if not city or not event_date or not metric:
        return None
    normalized_city = re.sub(r"[^a-z0-9]+", "_", city.lower()).strip("_")
    return f"{venue}:weather:{metric}:{normalized_city}:{event_date}"


def _extract_city(question: str) -> str | None:
    for pattern in (
        r"\bin ([A-Za-z .'-]+?) be\b",
        r"\bin ([A-Za-z .'-]+?) on\b",
    ):
        match = re.search(pattern, question)
        if match:
            return match.group(1).strip()
    return None


def _extract_date(question: str) -> str | None:
    match = re.search(r"\bon ([A-Z][a-z]+) (\d{1,2})(?:,? (\d{4}))?", question)
    if not match:
        return None
    month_name, day_text, year_text = match.groups()
    year = int(year_text) if year_text else datetime.now(timezone.utc).year
    try:
        return datetime.strptime(f"{month_name} {int(day_text)} {year}", "%B %d %Y").date().isoformat()
    except ValueError:
        return None


def _extract_metric(lowered_question: str) -> str | None:
    if "highest temperature" in lowered_question:
        return "highest_temperature"
    if "lowest temperature" in lowered_question:
        return "lowest_temperature"
    if "precipitation" in lowered_question:
        return "precipitation"
    if re.search(r"\brain\b", lowered_question):
        return "rain"
    if "snow" in lowered_question:
        return "snow"
    if "wind" in lowered_question:
        return "wind"
    return None
