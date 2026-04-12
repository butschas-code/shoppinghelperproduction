#!/usr/bin/env python3
"""Automatic daily price ingestion.

Intended to be called once per day via cron / launchd / Task Scheduler.
Each run appends new price rows (never deletes old ones) so the database
accumulates a full time-series of prices.

Usage:
    python run_daily_ingest.py          # run once and exit
    crontab: 0 3 * * * cd /path/to/project && .venv/bin/python run_daily_ingest.py

Logs are written to both stdout and logs/ingest.log.
"""

from __future__ import annotations

import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(PROJECT_ROOT / ".env")

from app.core.config import DATABASE_URL  # noqa: E402
from app.core.logging import add_file_handler, get_logger  # noqa: E402
from app.db.migrate import create_tables  # noqa: E402
from app.db.session import get_db_ctx  # noqa: E402
from app.services.alerts import AlertCollector  # noqa: E402
from app.services.ingest import is_retailer_ingest_key, run_full_ingest  # noqa: E402

LOG_FILE = PROJECT_ROOT / "logs" / "ingest.log"
add_file_handler(LOG_FILE)

logger = get_logger("daily_ingest")

BACKUP_DIR = PROJECT_ROOT / "backups"
DB_PATH = PROJECT_ROOT / "prices.db"
BACKUP_KEEP = 30


def backup_database() -> Path | None:
    """Copy prices.db → backups/prices_YYYY_MM_DD.db, prune old backups.

    Skipped when using Postgres or any non-file SQLite URL (e.g. :memory:).
    """
    url = str(DATABASE_URL).lower()
    if not url.startswith("sqlite"):
        logger.info("Skipping file backup — DATABASE_URL is not SQLite (e.g. Postgres)")
        return None
    if ":memory:" in url:
        logger.info("Skipping file backup — in-memory SQLite")
        return None
    if not DB_PATH.exists():
        logger.warning("Database %s not found — skipping backup", DB_PATH)
        return None

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y_%m_%d")
    dest = BACKUP_DIR / f"prices_{today}.db"

    shutil.copy2(str(DB_PATH), str(dest))
    size_mb = dest.stat().st_size / (1024 * 1024)
    logger.info("Database backed up → %s (%.1f MB)", dest.name, size_mb)

    # Prune: keep only the newest BACKUP_KEEP files
    backups = sorted(BACKUP_DIR.glob("prices_*.db"))
    if len(backups) > BACKUP_KEEP:
        for old in backups[: len(backups) - BACKUP_KEEP]:
            old.unlink()
            logger.info("Deleted old backup: %s", old.name)

    return dest


def main() -> int:
    start = time.monotonic()
    utc_now = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("Ingestion started — %s UTC", utc_now.strftime("%Y-%m-%d %H:%M"))

    create_tables()

    alerts = AlertCollector()

    try:
        summary = run_full_ingest()
    except Exception as exc:
        logger.exception("FATAL: ingestion pipeline crashed")
        elapsed = time.monotonic() - start
        logger.info("Ingestion FAILED (%.0fs elapsed)", elapsed)
        alerts.check_crash(exc)
        alerts.check_runtime(elapsed)
        alerts.dispatch()
        return 1

    all_ok = True
    total_products = 0
    for retailer_id, info in summary.items():
        if not is_retailer_ingest_key(retailer_id):
            continue
        status = info.get("status", "?")
        count = info.get("count", 0)
        if status == "ok":
            total_products += count
            logger.info("  %s — %d products", retailer_id, count)
        else:
            all_ok = False
            error = info.get("error", "unknown")
            logger.error("  %s — FAILED: %s", retailer_id, error)

    elapsed = time.monotonic() - start

    if all_ok:
        logger.info(
            "Ingestion SUCCESS — %d total products (%.0fs elapsed)",
            total_products, elapsed,
        )
        backup_database()
    else:
        logger.warning(
            "Ingestion PARTIAL — %d products, some retailers failed (%.0fs elapsed)",
            total_products, elapsed,
        )

    # Run all alert checks
    alerts.check_retailer_results(summary)
    alerts.check_runtime(elapsed)
    with get_db_ctx() as db:
        alerts.check_retailer_duration(db, summary)
        alerts.check_data_sanity(db)
    alerts.dispatch()

    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
