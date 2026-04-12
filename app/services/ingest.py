"""Orchestrates ingestion from all configured retailer adapters.

Key design: **no long-lived DB sessions**.  Every DB touch opens a fresh
session, does its work, commits, and closes.  This is essential for managed
Postgres providers (Neon, Supabase) that aggressively kill idle / idle-in-
transaction SSL connections.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import insert
from sqlalchemy.orm import Session

from app.core import config
from app.core.logging import get_logger
from app.db.models import IngestLog, ProductOffer, Retailer
from app.db.session import get_db_ctx
from app.retailers import get_all_adapters
from app.services.anomaly import detect_anomalies
from app.services.basket_index import update_basket_index
from app.services.health import run_health_checks
from app.services.normalize import generate_fingerprint
from app.services.product_type import detect_product_type

logger = get_logger(__name__)

_MAX_RETRIES = 4


# ── helpers ────────────────────────────────────────────────────────────

def _build_rows(
    retailer_id: str,
    scraped_at: datetime,
    offers: list[Any],
) -> list[dict[str, Any]]:
    """Convert OfferDTO list to plain dicts for core INSERT (no ORM overhead)."""
    rows: list[dict[str, Any]] = []
    for dto in offers:
        pt = detect_product_type(dto.title, dto.category_path or dto.category_root)
        rows.append({
            "retailer_id": retailer_id,
            "scraped_at": scraped_at,
            "title": dto.title,
            "brand": dto.brand,
            "size_text": dto.size_text,
            "price": dto.price,
            "unit_price": dto.unit_price,
            "unit": dto.unit,
            "url": dto.url,
            "raw_json": dto.raw_json,
            "source": dto.source,
            "fingerprint": generate_fingerprint(dto.title, retailer_id, dto.size_text),
            "product_type": pt or None,
            "category_path": dto.category_path,
            "category_root": dto.category_root,
        })
    return rows


def _insert_chunk(chunk: list[dict[str, Any]]) -> None:
    """INSERT one chunk of rows in its own session.

    Opens a *brand-new* DB session, inserts, commits, closes.  On transient
    errors the dead session is discarded and a fresh one is created for the
    retry — the old socket is never reused.
    """
    stmt = insert(ProductOffer.__table__)
    last_exc: BaseException | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            with get_db_ctx() as db:
                db.execute(stmt, chunk)
                db.commit()
            return
        except Exception as exc:
            last_exc = exc
            if attempt >= _MAX_RETRIES - 1:
                break
            wait = 2 ** attempt
            logger.warning(
                "Chunk INSERT failed (%s), retry %d/%d in %ds: %s",
                type(exc).__name__, attempt + 1, _MAX_RETRIES, wait, exc,
            )
            time.sleep(wait)
    logger.exception("Chunk INSERT failed after %d attempts", _MAX_RETRIES)
    raise last_exc  # type: ignore[misc]


def _write_offers(rows: list[dict[str, Any]]) -> None:
    """Split *rows* into small chunks and insert each independently."""
    if not rows:
        return
    chunk_size = config.INGEST_COMMIT_BATCH
    total = len(rows)
    for start in range(0, total, chunk_size):
        chunk = rows[start : start + chunk_size]
        _insert_chunk(chunk)
        logger.info("  … inserted %d / %d rows", min(start + chunk_size, total), total)


def _ensure_retailer(meta: Any) -> None:
    """Create the Retailer row if it doesn't exist yet."""
    with get_db_ctx() as db:
        if not db.get(Retailer, meta.id):
            db.add(Retailer(
                id=meta.id,
                name=meta.name,
                country=meta.country,
                currency=meta.currency,
                base_url=meta.base_url,
            ))
            db.commit()


def _upsert_ingest_log(
    date: str, retailer_id: str,
    duration: float, count: int,
) -> None:
    with get_db_ctx() as db:
        existing = (
            db.query(IngestLog)
            .filter(IngestLog.date == date, IngestLog.retailer_id == retailer_id)
            .first()
        )
        if existing:
            existing.duration_seconds = round(duration, 1)
            existing.product_count = count
        else:
            db.add(IngestLog(
                date=date,
                retailer_id=retailer_id,
                duration_seconds=round(duration, 1),
                product_count=count,
            ))
        db.commit()


# ── public API ─────────────────────────────────────────────────────────

def is_retailer_ingest_key(key: str) -> bool:
    """True for per-adapter summary rows; False for metadata (_anomalies, _health)."""
    return not key.startswith("_")


def run_full_ingest() -> dict[str, dict]:
    """Run ingestion for every registered adapter.

    1. Scrape (no DB connection held — can take 40+ min for Playwright).
    2. Build plain dict rows (CPU-only, no DB).
    3. Insert in small chunks — each chunk opens a fresh session, inserts,
       commits, closes.  On SSL / connection failure the dead session is
       discarded and a *new* connection is created for the retry.

    This guarantees we never reuse a stale connection.
    """
    summary: dict[str, dict] = {}
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")

    for adapter in get_all_adapters():
        meta = adapter.retailer_meta()
        logger.info("--- Ingesting: %s (%s) ---", meta.name, meta.id)

        # ── 1. Scrape (no DB connection) ───────────────────────────────
        t0 = time.monotonic()
        fetch_error: Exception | None = None
        offers: list[Any] = []
        try:
            offers = adapter.fetch_offers()
        except Exception as exc:
            fetch_error = exc
            logger.exception("Scrape failed for %s", meta.id)

        duration = time.monotonic() - t0

        # ── 2. Ensure retailer row ────────────────────────────────────
        try:
            _ensure_retailer(meta)
        except Exception:
            logger.exception("Cannot create retailer row for %s", meta.id)

        if fetch_error is not None:
            _upsert_ingest_log(today, meta.id, duration, 0)
            summary[meta.id] = {
                "status": "error",
                "error": str(fetch_error),
                "duration": round(duration, 1),
            }
            continue

        logger.info(
            "Received %d offers from %s in %.1fs — writing to DB",
            len(offers), meta.id, duration,
        )

        # ── 3. Build rows (CPU only) ──────────────────────────────────
        rows = _build_rows(meta.id, now, offers)

        # ── 4. Insert chunks (each chunk = own session) ───────────────
        try:
            _write_offers(rows)
            _upsert_ingest_log(today, meta.id, duration, len(offers))
            summary[meta.id] = {
                "status": "ok",
                "count": len(offers),
                "duration": round(duration, 1),
            }
        except Exception as exc:
            logger.exception("DB write failed for %s", meta.id)
            _upsert_ingest_log(today, meta.id, duration, 0)
            summary[meta.id] = {
                "status": "error",
                "error": str(exc),
                "duration": round(duration, 1),
            }

    # ── Post-ingest steps ──────────────────────────────────────────────
    with get_db_ctx() as db:
        try:
            logger.info("--- Computing daily basket index ---")
            update_basket_index(db)
        except Exception:
            logger.exception("Basket index computation failed (non-fatal)")

        try:
            logger.info("--- Running anomaly detection ---")
            anomalies = detect_anomalies(db)
            summary["_anomalies"] = {
                "count": len(anomalies),
                "types": _anomaly_type_counts(anomalies),
            }
        except Exception:
            logger.exception("Anomaly detection failed (non-fatal)")

        try:
            logger.info("--- Running data health checks ---")
            health = run_health_checks(db, summary)
            summary["_health"] = {
                "global_status": health.global_status,
                "basket_ok": health.basket_ok,
                "history_ok": health.history_ok,
            }
        except Exception:
            logger.exception("Health check failed (non-fatal)")

    return summary


def _anomaly_type_counts(anomalies: list) -> dict[str, int]:
    counts: dict[str, int] = {}
    for a in anomalies:
        counts[a.anomaly_type] = counts.get(a.anomaly_type, 0) + 1
    return counts
