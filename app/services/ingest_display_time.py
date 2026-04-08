"""Resolve 'last ingest' time for the UI: GitHub Actions daily-ingest workflow, else DB scrape time."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core import config
from app.core.logging import get_logger
from app.db.models import ProductOffer

logger = get_logger(__name__)

RIGA = ZoneInfo("Europe/Riga")


def _github_last_workflow_run_utc() -> datetime | None:
    """Return completed/updated time of latest run for the daily ingest workflow, or None."""
    repo = config.GITHUB_REPOSITORY.strip()
    if not repo or "/" not in repo:
        return None
    wf = config.GITHUB_WORKFLOW_FILE.strip() or "daily-ingest.yml"
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{wf}/runs"
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if config.GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {config.GITHUB_TOKEN}"
    try:
        r = requests.get(url, params={"per_page": 1}, headers=headers, timeout=8)
        r.raise_for_status()
        data = r.json()
        runs = data.get("workflow_runs") or []
        if not runs:
            return None
        run = runs[0]
        # When the run last finished updating (completed or still in progress)
        raw = run.get("updated_at") or run.get("run_started_at")
        if not raw:
            return None
        # ISO 8601 with Z
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception as e:
        logger.warning("GitHub workflow time fetch failed: %s", e)
        return None


def _db_max_scraped_utc(db: Session) -> datetime | None:
    ts = db.query(func.max(ProductOffer.scraped_at)).scalar()
    if not ts:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def get_hero_refresh_display(db: Session) -> tuple[str, bool, str]:
    """Return (time label for template, is_today in Riga, source 'github'|'database').

    Prefer GitHub Actions last run time when GITHUB_REPOSITORY is set; otherwise
    latest product scrape timestamp (same basis as footer).
    """
    ts = _github_last_workflow_run_utc()
    source = "github"
    if ts is None:
        ts = _db_max_scraped_utc(db)
        source = "database"
    if ts is None:
        return "—", False, "database"

    local = ts.astimezone(RIGA)
    now_riga = datetime.now(RIGA)
    time_only = local.strftime("%H:%M")
    full_alt = local.strftime("%d %b %H:%M")
    is_today = local.date() == now_riga.date()
    label = time_only if is_today else full_alt
    return label, is_today, source
