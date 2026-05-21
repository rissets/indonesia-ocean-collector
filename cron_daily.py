"""
Daily incremental collector — collects yesterday's data from all sources.

Designed to run at 01:00 UTC daily via cron or scheduler.py.
Writes structured JSON log to data/cron_daily.log for monitoring.

Usage:
  python cron_daily.py              # collect yesterday
  python cron_daily.py --date 2024-03-15  # collect specific date
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from collectors import cmems_collector, erddap_collector
from collectors.openmeteo_collector import collect_weather
from db.writer import upsert_dataframe

# ---------------------------------------------------------------------------
LOG_FILE = Path("data/cron_daily.log")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

_json_handler = logging.FileHandler(LOG_FILE)
_json_handler.setLevel(logging.INFO)


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        })


_json_handler.setFormatter(_JsonFormatter())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout), _json_handler],
)
logger = logging.getLogger("cron_daily")

MAX_RECORDS = 10_000


def run_daily(target_date: str) -> dict:
    """Collect and upsert data for a single date. Returns summary dict."""
    t0 = time.time()
    summary = {
        "date": target_date,
        "sources": {},
        "total_upserted": 0,
        "errors": [],
        "duration_s": 0,
    }

    logger.info("Daily collect start: %s", target_date)

    # ERDDAP — SST, chlorophyll, SSH/currents
    try:
        sst = erddap_collector.collect_sst(target_date, target_date, MAX_RECORDS, stride=2)
        chl = erddap_collector.collect_chlorophyll(target_date, target_date, MAX_RECORDS, stride=2)
        ssh = erddap_collector.collect_ssh_currents(target_date, target_date, MAX_RECORDS, stride=1)
        n = sum(upsert_dataframe(df) for df in [sst, chl, ssh] if not df.empty)
        summary["sources"]["erddap"] = n
        summary["total_upserted"] += n
    except Exception as exc:
        logger.error("ERDDAP daily error: %s", exc)
        summary["errors"].append(f"erddap: {exc}")

    # CMEMS — SST, currents, SSH, chlorophyll
    try:
        cmems = cmems_collector.collect_all(target_date, target_date, MAX_RECORDS)
        if not cmems.empty:
            n = upsert_dataframe(cmems)
            summary["sources"]["cmems"] = n
            summary["total_upserted"] += n
    except Exception as exc:
        logger.error("CMEMS daily error: %s", exc)
        summary["errors"].append(f"cmems: {exc}")

    # Open-Meteo — wave, wind, solar
    try:
        weather = collect_weather(target_date, target_date, MAX_RECORDS)
        if not weather.empty:
            n = upsert_dataframe(weather)
            summary["sources"]["openmeteo"] = n
            summary["total_upserted"] += n
    except Exception as exc:
        logger.error("Open-Meteo daily error: %s", exc)
        summary["errors"].append(f"openmeteo: {exc}")

    summary["duration_s"] = round(time.time() - t0, 1)
    status = "OK" if not summary["errors"] else "PARTIAL"
    logger.info(
        "Daily collect %s: status=%s rows=%d duration=%.1fs sources=%s errors=%d",
        target_date, status, summary["total_upserted"],
        summary["duration_s"], summary["sources"], len(summary["errors"]),
    )
    return summary


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Daily incremental ocean data collector")
    p.add_argument("--date", default=None, metavar="YYYY-MM-DD",
                   help="Date to collect (default: yesterday UTC)")
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    target = args.date or (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    summary = run_daily(target)
    if summary["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
