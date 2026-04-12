"""Orchestrates ingestion from all configured retailer adapters."""

from __future__ import annotations

import time
from datetime import datetime, timezone

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


def is_retailer_ingest_key(key: str) -> bool:
    """True for per-adapter summary rows; False for metadata (_anomalies, _health)."""
    return not key.startswith("_")


def run_full_ingest() -> dict[str, dict]:
    """Run ingestion for every registered adapter. Returns per-retailer summary.

    A fresh DB session is opened *after* each retailer's scrape completes so
    that long-running scrapers (Maxima/Playwright ≈ 40 min) never hold an
    idle Postgres connection.  Managed Postgres providers (Neon, Supabase,
    Railway) drop idle SSL sessions in ~5 minutes, which was causing the
    ``SSL connection has been closed unexpectedly`` errors seen in CI.
    """
    summary: dict[str, dict] = {}
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")

    for adapter in get_all_adapters():
        meta = adapter.retailer_meta()
        logger.info("--- Ingesting: %s (%s) ---", meta.name, meta.id)

        # ── 1. Scrape (no DB connection held) ──────────────────────────
        t0 = time.monotonic()
        fetch_error: Exception | None = None
        offers = []
        try:
            offers = adapter.fetch_offers()
        except Exception as exc:
            fetch_error = exc
            logger.exception("Scrape failed for %s", meta.id)

        duration = time.monotonic() - t0

        # ── 2. Write with a fresh connection ───────────────────────────
        with get_db_ctx() as db:
            # Ensure the Retailer row exists.
            if not db.get(Retailer, meta.id):
                db.add(Retailer(
                    id=meta.id,
                    name=meta.name,
                    country=meta.country,
                    currency=meta.currency,
                    base_url=meta.base_url,
                ))
                db.commit()

            if fetch_error is not None:
                _upsert_ingest_log(db, today, meta.id, duration, 0)
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
            try:
                batch = config.INGEST_COMMIT_BATCH
                for i, dto in enumerate(offers, start=1):
                    product_type = detect_product_type(
                        dto.title, dto.category_path or dto.category_root
                    )
                    db.add(ProductOffer(
                        retailer_id=meta.id,
                        scraped_at=now,
                        title=dto.title,
                        brand=dto.brand,
                        size_text=dto.size_text,
                        price=dto.price,
                        unit_price=dto.unit_price,
                        unit=dto.unit,
                        url=dto.url,
                        raw_json=dto.raw_json,
                        source=dto.source,
                        fingerprint=generate_fingerprint(
                            dto.title, meta.id, dto.size_text,
                        ),
                        product_type=product_type or None,
                        category_path=dto.category_path,
                        category_root=dto.category_root,
                    ))
                    if i % batch == 0:
                        db.commit()
                        logger.info("  … committed %d/%d rows", i, len(offers))
                if len(offers) == 0 or len(offers) % batch != 0:
                    db.commit()
                _upsert_ingest_log(db, today, meta.id, duration, len(offers))
                summary[meta.id] = {
                    "status": "ok",
                    "count": len(offers),
                    "duration": round(duration, 1),
                }
            except Exception as exc:
                logger.exception("DB write failed for %s", meta.id)
                db.rollback()
                _upsert_ingest_log(db, today, meta.id, duration, 0)
                summary[meta.id] = {
                    "status": "error",
                    "error": str(exc),
                    "duration": round(duration, 1),
                }

    # ── Post-ingest steps (each opens its own fresh session) ───────────
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


def _upsert_ingest_log(
    db: Session, date: str, retailer_id: str,
    duration: float, count: int,
) -> None:
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
