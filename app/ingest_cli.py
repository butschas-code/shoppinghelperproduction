#!/usr/bin/env python3
"""CLI entry-point for running data ingestion.

Usage:
    python -m app.ingest_cli run
    python app/ingest_cli.py run
"""

from __future__ import annotations

import sys

from app.core.logging import get_logger
from app.db.migrate import create_tables
from app.services.ingest import is_retailer_ingest_key, run_full_ingest

logger = get_logger(__name__)


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] != "run":
        print("Usage: python -m app.ingest_cli run")
        sys.exit(1)

    logger.info("Creating / verifying tables ...")
    create_tables()

    logger.info("Starting full ingestion ...")
    summary = run_full_ingest()

    logger.info("Ingestion complete.")
    for retailer_id, info in summary.items():
        if not is_retailer_ingest_key(retailer_id):
            continue
        status = info.get("status", "?")
        count = info.get("count", "-")
        logger.info("  %s: %s (offers: %s)", retailer_id, status, count)


if __name__ == "__main__":
    main()
