"""
Historical batch collector — 2021-01-01 to today, monthly chunks.

Usage:
  python batch_collect.py                        # full run 2021→today
  python batch_collect.py --start 2023-01-01     # resume from a date
  python batch_collect.py --sources erddap cmems # specific sources only
  python batch_collect.py --dry-run              # print plan, no API calls

Progress is checkpointed in data/batch_progress.json so interrupted runs
can be resumed without re-collecting completed months.

Monitoring: structured JSON log lines are written to data/batch_collect.log
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
# Logging — both human-readable console + JSON file for monitoring
# ---------------------------------------------------------------------------
LOG_FILE = Path("data/batch_collect.log")
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
logger = logging.getLogger("batch_collect")

# ---------------------------------------------------------------------------
BATCH_START = "2021-01-01"
PROGRESS_FILE = Path("data/batch_progress.json")
ALL_SOURCES = ["erddap", "cmems", "openmeteo"]


# ---------------------------------------------------------------------------
# Progress checkpoint helpers
# ---------------------------------------------------------------------------

def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"completed_months": []}


def _save_progress(progress: dict) -> None:
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2))


def _month_range(start: str, end: str) -> list[tuple[str, str]]:
    """Return list of (month_start, month_end) tuples covering start→end."""
    months = []
    cur = date.fromisoformat(start).replace(day=1)
    end_d = date.fromisoformat(end)
    while cur <= end_d:
        # last day of month
        if cur.month == 12:
            last = cur.replace(day=31)
        else:
            last = (cur.replace(month=cur.month + 1, day=1) - timedelta(days=1))
        last = min(last, end_d)
        months.append((cur.strftime("%Y-%m-%d"), last.strftime("%Y-%m-%d")))
        # advance to next month
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1, day=1)
        else:
            cur = cur.replace(month=cur.month + 1, day=1)
    return months


# ---------------------------------------------------------------------------
# Per-month collection
# ---------------------------------------------------------------------------

def collect_month(
    month_start: str,
    month_end: str,
    sources: list[str],
    max_records: int,
    dry_run: bool,
) -> dict:
    """Collect one month from all requested sources and upsert to DB."""
    result = {"month": month_start[:7], "sources": {}, "total_upserted": 0, "errors": []}

    if dry_run:
        logger.info("[DRY-RUN] Would collect %s → %s from %s", month_start, month_end, sources)
        return result

    # ERDDAP
    if "erddap" in sources:
        try:
            sst = erddap_collector.collect_sst(month_start, month_end, max_records, stride=2)
            chl = erddap_collector.collect_chlorophyll(month_start, month_end, max_records, stride=2)
            ssh = erddap_collector.collect_ssh_currents(month_start, month_end, max_records, stride=1)
            n = 0
            for df in [sst, chl, ssh]:
                if not df.empty:
                    n += upsert_dataframe(df)
            result["sources"]["erddap"] = n
            result["total_upserted"] += n
        except Exception as exc:
            logger.error("ERDDAP error for %s: %s", month_start[:7], exc)
            result["errors"].append(f"erddap: {exc}")

    # CMEMS
    if "cmems" in sources:
        try:
            cmems = cmems_collector.collect_all(month_start, month_end, max_records)
            if not cmems.empty:
                n = upsert_dataframe(cmems)
                result["sources"]["cmems"] = n
                result["total_upserted"] += n
        except Exception as exc:
            logger.error("CMEMS error for %s: %s", month_start[:7], exc)
            result["errors"].append(f"cmems: {exc}")

    # Open-Meteo
    if "openmeteo" in sources:
        try:
            weather = collect_weather(month_start, month_end, max_records)
            if not weather.empty:
                n = upsert_dataframe(weather)
                result["sources"]["openmeteo"] = n
                result["total_upserted"] += n
        except Exception as exc:
            logger.error("Open-Meteo error for %s: %s", month_start[:7], exc)
            result["errors"].append(f"openmeteo: {exc}")

    logger.info(
        "Month %s done: %d rows upserted | sources=%s | errors=%d",
        result["month"], result["total_upserted"], result["sources"], len(result["errors"]),
    )
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Historical batch collector 2021→today")
    p.add_argument("--start", default=BATCH_START, metavar="YYYY-MM-DD",
                   help=f"Start date (default: {BATCH_START})")
    p.add_argument("--end", default=date.today().strftime("%Y-%m-%d"), metavar="YYYY-MM-DD",
                   help="End date (default: today)")
    p.add_argument("--sources", nargs="+", choices=ALL_SOURCES, default=ALL_SOURCES)
    p.add_argument("--max-records", type=int, default=5000, metavar="N",
                   help="Max records per source per month (default: 5000)")
    p.add_argument("--dry-run", action="store_true", help="Print plan without calling APIs")
    p.add_argument("--resume", action="store_true", default=True,
                   help="Skip already-completed months (default: True)")
    p.add_argument("--no-resume", dest="resume", action="store_false",
                   help="Re-collect all months even if already done")
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    progress = _load_progress() if args.resume else {"completed_months": []}
    completed = set(progress["completed_months"])

    months = _month_range(args.start, args.end)
    pending = [(s, e) for s, e in months if s[:7] not in completed]

    logger.info("=" * 60)
    logger.info("Batch collect: %s → %s", args.start, args.end)
    logger.info("Total months: %d | Pending: %d | Already done: %d",
                len(months), len(pending), len(completed))
    logger.info("Sources: %s | Max records/month: %d", args.sources, args.max_records)
    logger.info("=" * 60)

    summary = {"total_months": len(months), "processed": 0, "total_upserted": 0, "errors": []}
    t0 = time.time()

    for i, (ms, me) in enumerate(pending, 1):
        logger.info("[%d/%d] Collecting %s → %s ...", i, len(pending), ms, me)
        result = collect_month(ms, me, args.sources, args.max_records, args.dry_run)

        summary["processed"] += 1
        summary["total_upserted"] += result["total_upserted"]
        summary["errors"].extend(result["errors"])

        if not args.dry_run and not result["errors"]:
            completed.add(ms[:7])
            progress["completed_months"] = sorted(completed)
            _save_progress(progress)

        elapsed = time.time() - t0
        rate = i / elapsed * 60 if elapsed > 0 else 0
        remaining = (len(pending) - i) / (rate / 60) if rate > 0 else 0
        logger.info("Progress: %d/%d months | %.0f rows total | ETA ~%.0f min",
                    i, len(pending), summary["total_upserted"], remaining)

    logger.info("=" * 60)
    logger.info("Batch complete: %d months processed, %d rows upserted, %d errors",
                summary["processed"], summary["total_upserted"], len(summary["errors"]))
    if summary["errors"]:
        logger.warning("Errors encountered: %s", summary["errors"])
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
