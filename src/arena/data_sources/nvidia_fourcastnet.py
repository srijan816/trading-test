from __future__ import annotations

import io
import json
import logging
import os
import pathlib
import tarfile
import tempfile
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import requests

logger = logging.getLogger(__name__)

try:
    from earth2studio.data import ARCO
    from earth2studio.models.px.fcn3 import VARIABLES

    EARTH2STUDIO_AVAILABLE = True
except ImportError:
    ARCO = None
    VARIABLES = None
    EARTH2STUDIO_AVAILABLE = False
    logger.warning(
        "earth2studio not installed. FourCastNet will be disabled until installed."
    )

FOURCASTNET_API_URL = "https://climate.api.nvidia.com/v1/nvidia/fourcastnet"
CACHE_DIR = pathlib.Path(__file__).resolve().parents[3] / "data" / "cache" / "fourcastnet"
CACHE_MAX_AGE_HOURS = 6
T2M_CHANNEL_INDEX = 2
ROOT = pathlib.Path(__file__).resolve().parents[3]

CACHE_DIR.mkdir(parents=True, exist_ok=True)

if EARTH2STUDIO_AVAILABLE and VARIABLES is not None:
    try:
        variable_names = [str(item).lower() for item in VARIABLES]
        if "t2m" in variable_names:
            T2M_CHANNEL_INDEX = variable_names.index("t2m")
    except Exception as exc:
        logger.warning("Unable to verify FourCastNet t2m channel index from earth2studio: %s", exc)

def _has_configured_api_key() -> bool:
    if os.environ.get("NVIDIA_API_KEY") or os.environ.get("NVIDA_API_KEY"):
        return True
    env_path = ROOT / ".env"
    if not env_path.exists():
        return False
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if line.startswith("NVIDIA_API_KEY=") or line.startswith("NVIDA_API_KEY="):
                return True
    except Exception:
        return False
    return False


if not _has_configured_api_key():
    logger.warning("NVIDIA_API_KEY not set, FourCastNet source disabled")


def _get_cache_path(lat: float, lon: float, target_date: str, init_time: datetime) -> pathlib.Path:
    lat_token = f"{lat:.4f}".replace("-", "m").replace(".", "p")
    lon_token = f"{lon:.4f}".replace("-", "m").replace(".", "p")
    return CACHE_DIR / f"{lat_token}_{lon_token}_{target_date}_{init_time:%Y%m%d%H}.json"


def _fetch_initial_state(init_time: datetime) -> Optional[np.ndarray]:
    if not EARTH2STUDIO_AVAILABLE:
        logger.error(
            "earth2studio not installed. Cannot fetch ERA5 initial state. Install with: pip install earth2studio"
        )
        return None

    try:
        ds = ARCO()
        da = ds(time=init_time, variable=VARIABLES)
        array = da.to_numpy()
        array = np.asarray(array, dtype="float32")
        if array.ndim == 3:
            array = array[None, None, ...]
        elif array.ndim == 4:
            array = array[None, ...]
        if array.shape != (1, 1, 73, 721, 1440):
            logger.warning("Unexpected FourCastNet initial-state shape %s", getattr(array, "shape", None))
        return array.astype("float32", copy=False)
    except Exception as exc:
        logger.error("Failed to fetch ERA5 initial state for FourCastNet: %s", exc)
        return None


def _find_nearest_grid_index(lat: float, lon: float) -> tuple[int, int]:
    lat_idx = int(round((90.0 - float(lat)) / 0.25))
    lon_idx = int(round((float(lon) % 360.0) / 0.25))
    lat_idx = max(0, min(720, lat_idx))
    lon_idx = max(0, min(1439, lon_idx))
    return lat_idx, lon_idx


def _extract_t2m_forecast(
    tar_bytes: bytes,
    lat: float,
    lon: float,
    target_date: str,
    init_time: datetime,
) -> Optional[float]:
    lat_idx, lon_idx = _find_nearest_grid_index(lat, lon)
    target = datetime.strptime(target_date, "%Y-%m-%d").date()
    t2m_values: list[float] = []

    try:
        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:*") as archive:
            for member in archive.getmembers():
                if not member.isfile() or not member.name.endswith(".npy"):
                    continue
                try:
                    lead_hours = int(pathlib.Path(member.name).stem.split("_")[0])
                except Exception:
                    continue
                forecast_time = init_time + timedelta(hours=lead_hours)
                if forecast_time.date() != target:
                    continue
                extracted = archive.extractfile(member)
                if extracted is None:
                    continue
                with extracted:
                    array = np.load(extracted, allow_pickle=False)
                array = np.asarray(array)
                if array.ndim == 4:
                    t2m = array[0, T2M_CHANNEL_INDEX, lat_idx, lon_idx]
                elif array.ndim == 5:
                    t2m = array[0, 0, T2M_CHANNEL_INDEX, lat_idx, lon_idx]
                elif array.ndim == 3:
                    t2m = array[T2M_CHANNEL_INDEX, lat_idx, lon_idx]
                else:
                    logger.warning(
                        "Unexpected FourCastNet forecast array shape %s in %s",
                        array.shape,
                        member.name,
                    )
                    continue
                t2m_f = (float(t2m) - 273.15) * 9.0 / 5.0 + 32.0
                t2m_values.append(t2m_f)
    except Exception as exc:
        logger.error("Failed to parse FourCastNet tar response: %s", exc)
        return None

    return max(t2m_values) if t2m_values else None


def fetch_fourcastnet_forecast(lat: float, lon: float, target_date: str) -> Optional[float]:
    api_key = os.environ.get("NVIDIA_API_KEY") or os.environ.get("NVIDA_API_KEY")
    if not api_key:
        return None

    now = datetime.now(timezone.utc)
    if now.hour >= 12:
        init_time = now.replace(hour=12, minute=0, second=0, microsecond=0)
    else:
        init_time = now.replace(hour=0, minute=0, second=0, microsecond=0)

    cache_path = _get_cache_path(lat, lon, target_date, init_time)
    try:
        if cache_path.exists():
            age = datetime.now(timezone.utc) - datetime.fromtimestamp(cache_path.stat().st_mtime, tz=timezone.utc)
            if age < timedelta(hours=CACHE_MAX_AGE_HOURS):
                payload = json.loads(cache_path.read_text(encoding="utf-8"))
                cached = payload.get("forecast_high_f")
                if cached is not None:
                    return float(cached)
    except Exception as exc:
        logger.warning("Failed to read FourCastNet cache %s: %s", cache_path, exc)

    state = _fetch_initial_state(init_time)
    if state is None:
        return None

    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as tmp:
            tmp_path = tmp.name
            np.save(tmp, state)

        target = datetime.strptime(target_date, "%Y-%m-%d")
        hours_ahead = (target - init_time.replace(tzinfo=None)).total_seconds() / 3600.0 + 24.0
        simulation_length = max(1, min(int(hours_ahead / 6.0) + 1, 40))

        data = {
            "input_time": init_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "simulation_length": simulation_length,
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "accept": "application/x-tar",
        }

        for attempt in range(2):
            try:
                with open(tmp_path, "rb") as handle:
                    files = {"input_array": ("input_array.npy", handle, "application/octet-stream")}
                    response = requests.post(
                        FOURCASTNET_API_URL,
                        headers=headers,
                        data=data,
                        files=files,
                        timeout=180,
                    )
            except Exception as exc:
                if attempt == 0:
                    logger.warning("FourCastNet request failed, retrying once: %s", exc)
                    time.sleep(5)
                    continue
                logger.error("FourCastNet request failed: %s", exc)
                return None

            if response.status_code == 200:
                result = _extract_t2m_forecast(response.content, lat, lon, target_date, init_time)
                if result is not None:
                    try:
                        cache_path.write_text(
                            json.dumps(
                                {
                                    "forecast_high_f": round(float(result), 2),
                                    "target_date": target_date,
                                    "lat": lat,
                                    "lon": lon,
                                    "init_time": init_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                    "cached_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                },
                                indent=2,
                                sort_keys=True,
                            ),
                            encoding="utf-8",
                        )
                    except Exception as exc:
                        logger.warning("Failed writing FourCastNet cache %s: %s", cache_path, exc)
                return result

            if response.status_code == 429:
                logger.warning("FourCastNet rate limited")
                return None
            if response.status_code == 402:
                logger.warning("FourCastNet credits exhausted")
                return None
            if response.status_code in {500, 502, 503} and attempt == 0:
                logger.warning(
                    "FourCastNet transient error %s, retrying once", response.status_code
                )
                time.sleep(5)
                continue

            logger.error(
                "FourCastNet request failed with status %s: %s",
                response.status_code,
                response.text[:500],
            )
            return None
    except Exception as exc:
        logger.error("FourCastNet forecast failed for lat=%s lon=%s target_date=%s: %s", lat, lon, target_date, exc)
        return None
    finally:
        if tmp_path:
            try:
                pathlib.Path(tmp_path).unlink(missing_ok=True)
            except Exception:
                pass

    return None


def get_polling_schedule() -> list[str]:
    return ["01:00", "07:00", "13:00", "19:00"]
