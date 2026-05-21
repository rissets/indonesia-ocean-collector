"""
Cron scheduler — runs cron_daily.py at 01:00 UTC every day.

Uses APScheduler (lightweight, no external broker needed).
Exposes a /health and /status HTTP endpoint on port 8080 for monitoring.

Usage:
  python scheduler.py              # start scheduler + HTTP monitor
  python scheduler.py --port 9090  # custom port

Monitoring endpoints:
  GET /health  → {"status": "ok", "next_run": "..."}
  GET /status  → last run summary + scheduler state
  GET /logs    → last 100 lines of cron_daily.log
"""

from __future__ import annotations

import json
import logging
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
except ImportError:
    print("APScheduler not installed. Run: pip install apscheduler", file=sys.stderr)
    sys.exit(1)

from cron_daily import run_daily
from datetime import date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scheduler")

LOG_FILE = Path("data/cron_daily.log")
_state: dict = {
    "last_run": None,
    "last_summary": None,
    "scheduler_started": None,
}


def _job() -> None:
    target = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info("Scheduled job triggered for date: %s", target)
    summary = run_daily(target)
    _state["last_run"] = datetime.now(timezone.utc).isoformat()
    _state["last_summary"] = summary


# ---------------------------------------------------------------------------
# HTTP monitoring server
# ---------------------------------------------------------------------------

class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # suppress default access log
        pass

    def _send_json(self, code: int, body: dict) -> None:
        data = json.dumps(body, indent=2).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        scheduler: BackgroundScheduler = self.server.scheduler  # type: ignore[attr-defined]

        if self.path == "/health":
            jobs = scheduler.get_jobs()
            next_run = jobs[0].next_run_time.isoformat() if jobs else None
            self._send_json(200, {
                "status": "ok",
                "scheduler_running": scheduler.running,
                "next_run_utc": next_run,
            })

        elif self.path == "/status":
            jobs = scheduler.get_jobs()
            next_run = jobs[0].next_run_time.isoformat() if jobs else None
            self._send_json(200, {
                "scheduler_running": scheduler.running,
                "scheduler_started": _state["scheduler_started"],
                "next_run_utc": next_run,
                "last_run_utc": _state["last_run"],
                "last_summary": _state["last_summary"],
            })

        elif self.path == "/logs":
            lines: list[str] = []
            if LOG_FILE.exists():
                lines = LOG_FILE.read_text().splitlines()[-100:]
            self._send_json(200, {"lines": lines})

        else:
            self._send_json(404, {"error": "not found"})


def _start_http(port: int, scheduler: BackgroundScheduler) -> None:
    server = HTTPServer(("0.0.0.0", port), _Handler)
    server.scheduler = scheduler  # type: ignore[attr-defined]
    logger.info("Monitoring HTTP server on http://0.0.0.0:%d", port)
    logger.info("  GET /health  — scheduler liveness + next run time")
    logger.info("  GET /status  — last run summary")
    logger.info("  GET /logs    — last 100 log lines")
    server.serve_forever()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(port: int = 8080) -> None:
    import argparse
    p = argparse.ArgumentParser(description="Ocean data cron scheduler (01:00 UTC daily)")
    p.add_argument("--port", type=int, default=8080, help="HTTP monitoring port (default: 8080)")
    p.add_argument("--run-now", action="store_true", help="Run job immediately on startup (for testing)")
    args = p.parse_args()

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        _job,
        trigger=CronTrigger(hour=1, minute=0, timezone="UTC"),
        id="daily_ocean_collect",
        name="Daily ocean data collector",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    scheduler.start()
    _state["scheduler_started"] = datetime.now(timezone.utc).isoformat()

    jobs = scheduler.get_jobs()
    next_run = jobs[0].next_run_time.isoformat() if jobs else "unknown"
    logger.info("Scheduler started. Next run: %s", next_run)

    if args.run_now:
        logger.info("--run-now flag set, executing job immediately.")
        _job()

    # HTTP monitor in background thread
    t = threading.Thread(target=_start_http, args=(args.port, scheduler), daemon=True)
    t.start()

    logger.info("Scheduler running. Press Ctrl+C to stop.")
    try:
        import time
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler.")
        scheduler.shutdown()


if __name__ == "__main__":
    main()
