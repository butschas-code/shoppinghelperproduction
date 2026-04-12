"""Orchestrates ingestion from all configured retailer adapters."""

from __future__ import annotations

import time
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.db.models import IngestLog, ProductOffer, Retailer
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


def run_full_ingest(db: Session) -> dict[str, dict]:
    """Run ingestion for every registered adapter. Returns per-retailer summary."""
    summary: dict[str, dict] = {}
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")

    for adapter in get_all_adapters():
        meta = adapter.retailer_meta()
        logger.info("--- Ingesting: %s (%s) ---", meta.name, meta.id)

        existing = db.get(Retailer, meta.id)
        if not existing:
            db.add(
                Retailer(
                    id=meta.id,
                    name=meta.name,
                    country=meta.country,
                    currency=meta.currency,
                    base_url=meta.base_url,
                )
            )
            db.commit()

        t0 = time.monotonic()
        try:
            offers = adapter.fetch_offers()
            duration = time.monotonic() - t0
            logger.info(
                "Received %d offers from %s in %.1fs",
                len(offers), meta.id, duration,
            )

            for dto in offers:
                product_type = detect_product_type(dto.title, dto.category_path or dto.category_root)
                db.add(
                    ProductOffer(
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
                    )
                )

            db.commit()
            _upsert_ingest_log(db, today, meta.id, duration, len(offers))
            summary[meta.id] = {
                "status": "ok",
                "count": len(offers),
                "duration": round(duration, 1),
            }
        except Exception as exc:
            duration = time.monotonic() - t0
            logger.exception("Ingest failed for %s", meta.id)
            db.rollback()
            _upsert_ingest_log(db, today, meta.id, duration, 0)
            summary[meta.id] = {
                "status": "error",
                "error": str(exc),
                "duration": round(duration, 1),
            }

    # Compute daily basket index after all retailers are ingested
    try:
        logger.info("--- Computing daily basket index ---")
        update_basket_index(db)
    except Exception:
        logger.exception("Basket index computation failed (non-fatal)")

    # Detect price anomalies
    try:
        logger.info("--- Running anomaly detection ---")
        anomalies = detect_anomalies(db)
        summary["_anomalies"] = {
            "count": len(anomalies),
            "types": _anomaly_type_counts(anomalies),
        }
    except Exception:
        logger.exception("Anomaly detection failed (non-fatal)")

    # Run post-ingestion health checks
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
