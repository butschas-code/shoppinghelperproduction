"""Failure alert system for daily ingestion.

Alerts trigger on:
  1. Ingestion crash (unhandled exception).
  2. Retailer returning fewer than ALERT_MIN_PRODUCTS products.
  3. Suspiciously fast run (runtime dropped >60 % vs. recent average).
  4. Per-retailer duration deviates >50 % from 7-day average.
  5. Data sanity: product count dropped >50 % vs previous run.
  6. Data sanity: average price dropped >70 % vs previous run.
  7. Data sanity: >20 % of scraped products have missing/zero prices.

Delivery channels:
  - Always:  ``logs/alerts.log`` with prominent ERROR banner.
  - Optional: SMTP email when ``SMTP_HOST`` env var is configured.
"""

from __future__ import annotations

import logging
import smtplib
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core import config
from app.core.logging import get_logger
from app.core.retailer_meta import get_all_retailer_info
from app.db.models import BasketIndex, IngestLog, PriceAnomaly, ProductOffer
from app.services.ingest import is_retailer_ingest_key

logger = get_logger(__name__)

_ALERTS_LOG = Path(__file__).resolve().parents[2] / "logs" / "alerts.log"

_BANNER = """
╔══════════════════════════════════════════════════════════════╗
║  ⚠  ALERT  ⚠   {severity:^44s}  ║
╠══════════════════════════════════════════════════════════════╣
║  {headline:<58s}  ║
╚══════════════════════════════════════════════════════════════╝"""


@dataclass
class Alert:
    severity: str          # "CRITICAL" | "WARNING"
    headline: str
    details: str = ""
    timestamp: str = ""

    def __post_init__(self) -> None:
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


@dataclass
class AlertCollector:
    """Accumulates alerts during a single ingestion run, then dispatches them."""

    alerts: list[Alert] = field(default_factory=list)

    def add(self, severity: str, headline: str, details: str = "") -> None:
        self.alerts.append(Alert(severity=severity, headline=headline, details=details))

    @property
    def has_alerts(self) -> bool:
        return len(self.alerts) > 0

    # ------------------------------------------------------------------
    # Checks — call these from the ingestion driver
    # ------------------------------------------------------------------

    def check_crash(self, exc: BaseException) -> None:
        self.add(
            "CRITICAL",
            "Ingestion pipeline crashed",
            f"{type(exc).__name__}: {exc}",
        )

    def check_retailer_results(
        self, summary: dict[str, dict], min_products: int | None = None,
    ) -> None:
        threshold = min_products if min_products is not None else config.ALERT_MIN_PRODUCTS
        for retailer_id, info in summary.items():
            if not is_retailer_ingest_key(retailer_id):
                continue
            status = info.get("status", "?")
            count = info.get("count", 0)

            if status != "ok":
                self.add(
                    "CRITICAL",
                    f"{retailer_id}: ingestion failed",
                    info.get("error", "unknown error"),
                )
            elif count < threshold:
                self.add(
                    "WARNING",
                    f"{retailer_id}: only {count} products (expected >= {threshold})",
                    "Possible scraping breakage — site layout may have changed.",
                )

    def check_runtime(
        self,
        elapsed_seconds: float,
        history_file: Path | None = None,
        drop_pct: float = 0.60,
    ) -> None:
        """Alert when the run finishes suspiciously fast compared to history.

        Reads/writes a tiny text file that stores the last 10 run durations.
        If the current runtime is below ``(1 - drop_pct)`` of the recent
        average, flag a warning.
        """
        hfile = history_file or (_ALERTS_LOG.parent / "runtime_history.txt")
        hfile.parent.mkdir(parents=True, exist_ok=True)

        past: list[float] = []
        if hfile.exists():
            for line in hfile.read_text().splitlines():
                line = line.strip()
                if line:
                    try:
                        past.append(float(line))
                    except ValueError:
                        pass

        if len(past) >= 3:
            avg = sum(past) / len(past)
            if elapsed_seconds < avg * (1.0 - drop_pct):
                self.add(
                    "WARNING",
                    f"Runtime dropped sharply: {elapsed_seconds:.0f}s vs avg {avg:.0f}s",
                    f"Threshold: >{drop_pct:.0%} drop from recent average of {avg:.0f}s. "
                    "This may indicate a retailer returned early or the scraper was blocked.",
                )

        past.append(elapsed_seconds)
        # Keep last 10 runs
        hfile.write_text("\n".join(f"{v:.1f}" for v in past[-10:]) + "\n")

    def check_retailer_duration(
        self,
        db: Session,
        summary: dict[str, dict],
        deviation_pct: float = 0.50,
        lookback_days: int = 7,
    ) -> None:
        """Alert when a retailer's ingestion duration deviates >50 % from
        its 7-day average stored in ``ingest_log``.
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        for retailer_id, info in summary.items():
            if not is_retailer_ingest_key(retailer_id):
                continue
            current_dur = info.get("duration")
            if current_dur is None:
                continue

            rows = (
                db.query(IngestLog.duration_seconds)
                .filter(
                    IngestLog.retailer_id == retailer_id,
                    IngestLog.date < today,
                )
                .order_by(IngestLog.date.desc())
                .limit(lookback_days)
                .all()
            )

            past = [r[0] for r in rows if r[0] is not None]
            if len(past) < 2:
                continue

            avg = sum(past) / len(past)
            if avg == 0:
                continue

            deviation = abs(current_dur - avg) / avg
            if deviation > deviation_pct:
                direction = "faster" if current_dur < avg else "slower"
                self.add(
                    "WARNING",
                    f"{retailer_id}: ingestion {deviation:.0%} {direction} than 7-day avg",
                    f"Today: {current_dur:.0f}s, 7-day avg: {avg:.0f}s "
                    f"({lookback_days}-day window, {len(past)} data points). "
                    f"Threshold: {deviation_pct:.0%} deviation.",
                )

    def check_data_sanity(
        self,
        db: Session,
        count_drop_pct: float = 0.50,
        price_drop_pct: float = 0.70,
        missing_price_pct: float = 0.20,
    ) -> None:
        """Compare today's scrape against the previous one per retailer.

        Fires CRITICAL alerts when:
        - Product count dropped more than *count_drop_pct* (default 50 %).
        - Average price dropped more than *price_drop_pct* (default 70 %).
        - More than *missing_price_pct* (default 20 %) of products have
          a zero or null price.
        """
        # Distinct scrape timestamps per retailer, most recent first
        scrape_dates = (
            db.query(
                ProductOffer.retailer_id,
                ProductOffer.scraped_at,
            )
            .group_by(ProductOffer.retailer_id, ProductOffer.scraped_at)
            .order_by(ProductOffer.retailer_id, ProductOffer.scraped_at.desc())
            .all()
        )

        # Group by retailer: ordered list of scrape timestamps (newest first)
        ts_by_retailer: dict[str, list[datetime]] = {}
        for rid, ts in scrape_dates:
            ts_by_retailer.setdefault(rid, []).append(ts)

        for rid, timestamps in ts_by_retailer.items():
            if len(timestamps) < 1:
                continue

            latest_ts = timestamps[0]
            prev_ts = timestamps[1] if len(timestamps) >= 2 else None

            # --- Stats for today's (latest) scrape ---
            today_count: int = (
                db.query(func.count(ProductOffer.id))
                .filter(
                    ProductOffer.retailer_id == rid,
                    ProductOffer.scraped_at == latest_ts,
                )
                .scalar() or 0
            )
            today_avg: float = (
                db.query(func.avg(ProductOffer.price))
                .filter(
                    ProductOffer.retailer_id == rid,
                    ProductOffer.scraped_at == latest_ts,
                    ProductOffer.price > 0,
                )
                .scalar() or 0.0
            )
            zero_count: int = (
                db.query(func.count(ProductOffer.id))
                .filter(
                    ProductOffer.retailer_id == rid,
                    ProductOffer.scraped_at == latest_ts,
                    (ProductOffer.price == 0) | (ProductOffer.price.is_(None)),
                )
                .scalar() or 0
            )

            # Check 1: missing / zero prices
            if today_count > 0:
                bad_ratio = zero_count / today_count
                if bad_ratio > missing_price_pct:
                    self.add(
                        "CRITICAL",
                        f"{rid}: {bad_ratio:.0%} of products have missing/zero prices",
                        f"{zero_count} of {today_count} products have price=0 or NULL.",
                    )

            if prev_ts is None:
                continue

            # --- Stats for previous scrape ---
            prev_count: int = (
                db.query(func.count(ProductOffer.id))
                .filter(
                    ProductOffer.retailer_id == rid,
                    ProductOffer.scraped_at == prev_ts,
                )
                .scalar() or 0
            )
            prev_avg: float = (
                db.query(func.avg(ProductOffer.price))
                .filter(
                    ProductOffer.retailer_id == rid,
                    ProductOffer.scraped_at == prev_ts,
                    ProductOffer.price > 0,
                )
                .scalar() or 0.0
            )

            # Check 2: product count drop
            if prev_count > 0:
                drop = 1.0 - (today_count / prev_count)
                if drop > count_drop_pct:
                    self.add(
                        "CRITICAL",
                        f"{rid}: product count dropped {drop:.0%}",
                        f"Today: {today_count}, previous: {prev_count}. "
                        "Possible scraping failure or site structure change.",
                    )

            # Check 3: average price crash
            if prev_avg > 0:
                price_drop = 1.0 - (today_avg / prev_avg)
                if price_drop > price_drop_pct:
                    self.add(
                        "CRITICAL",
                        f"{rid}: avg price crashed {price_drop:.0%}",
                        f"Today avg: {today_avg:.2f}€, previous avg: {prev_avg:.2f}€. "
                        "Likely data corruption or parser extracting wrong field.",
                    )

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def dispatch(self) -> None:
        if not self.alerts:
            return

        self._write_alert_log()
        self._log_banners()

        if config.SMTP_HOST:
            self._send_email()

    def _write_alert_log(self) -> None:
        _ALERTS_LOG.parent.mkdir(parents=True, exist_ok=True)
        alert_logger = logging.getLogger("alerts_file")
        if not any(
            isinstance(h, logging.FileHandler)
            and h.baseFilename == str(_ALERTS_LOG.resolve())
            for h in alert_logger.handlers
        ):
            fh = logging.FileHandler(str(_ALERTS_LOG), encoding="utf-8")
            fh.setFormatter(logging.Formatter("%(message)s"))
            alert_logger.addHandler(fh)
            alert_logger.setLevel(logging.WARNING)

        for a in self.alerts:
            alert_logger.warning(
                "[%s] %s | %s | %s",
                a.timestamp, a.severity, a.headline, a.details,
            )

    def _log_banners(self) -> None:
        for a in self.alerts:
            banner = _BANNER.format(severity=a.severity, headline=a.headline)
            for line in banner.strip().splitlines():
                logger.error(line)
            if a.details:
                logger.error("  Details: %s", a.details)
            logger.error("")

    def _send_email(self) -> None:
        recipients = [r.strip() for r in config.SMTP_TO.split(",") if r.strip()]
        if not recipients:
            logger.warning("SMTP_HOST set but SMTP_TO is empty — skipping email")
            return

        subject = f"[LV Price Compare] {len(self.alerts)} alert(s) — {self.alerts[0].headline}"
        body_parts: list[str] = []
        for a in self.alerts:
            body_parts.append(
                f"[{a.severity}] {a.headline}\n"
                f"  Time: {a.timestamp}\n"
                f"  Details: {a.details}\n"
            )
        body = "\n".join(body_parts)

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = config.SMTP_FROM or config.SMTP_USER
        msg["To"] = ", ".join(recipients)
        msg.set_content(body)

        try:
            with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=30) as srv:
                srv.ehlo()
                if config.SMTP_PORT != 25:
                    srv.starttls()
                if config.SMTP_USER:
                    srv.login(config.SMTP_USER, config.SMTP_PASS)
                srv.send_message(msg)
            logger.info("Alert email sent to %s", ", ".join(recipients))
        except Exception:
            logger.exception("Failed to send alert email")


# ======================================================================
# Standalone status report email
# ======================================================================


def send_status_email(db: Session) -> bool:
    """Send a comprehensive DB status report via SMTP.

    Returns True on success, False on failure.
    """
    if not config.SMTP_HOST:
        logger.warning("SMTP_HOST not set — cannot send status email")
        return False

    recipients = [r.strip() for r in config.SMTP_TO.split(",") if r.strip()]
    if not recipients:
        logger.warning("SMTP_TO is empty — cannot send status email")
        return False

    report = _build_status_report(db)

    msg = EmailMessage()
    msg["Subject"] = f"[LV Price Compare] DB Status Report — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC"
    msg["From"] = config.SMTP_FROM or config.SMTP_USER
    msg["To"] = ", ".join(recipients)
    msg.set_content(report)

    try:
        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=30) as srv:
            srv.ehlo()
            if config.SMTP_PORT != 25:
                srv.starttls()
            if config.SMTP_USER:
                srv.login(config.SMTP_USER, config.SMTP_PASS)
            srv.send_message(msg)
        logger.info("Status email sent to %s", ", ".join(recipients))
        return True
    except Exception:
        logger.exception("Failed to send status email")
        return False


def _build_status_report(db: Session) -> str:
    """Build a plain-text status report with DB stats."""
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines: list[str] = []
    lines.append("=" * 60)
    lines.append("  LV PRICE COMPARE — STATUS REPORT")
    lines.append(f"  Generated: {utc_now}")
    lines.append("=" * 60)
    lines.append("")

    # --- Per-retailer stats ---
    all_meta = {info.retailer_id: info for info in get_all_retailer_info()}

    latest_sub = (
        db.query(
            ProductOffer.retailer_id,
            func.max(ProductOffer.scraped_at).label("latest"),
        )
        .group_by(ProductOffer.retailer_id)
        .subquery()
    )
    count_rows = (
        db.query(ProductOffer.retailer_id, func.count(ProductOffer.id))
        .join(
            latest_sub,
            (ProductOffer.retailer_id == latest_sub.c.retailer_id)
            & (ProductOffer.scraped_at == latest_sub.c.latest),
        )
        .group_by(ProductOffer.retailer_id)
        .all()
    )
    product_counts = dict(count_rows)

    last_updated_rows = (
        db.query(
            ProductOffer.retailer_id,
            func.max(ProductOffer.scraped_at),
        )
        .group_by(ProductOffer.retailer_id)
        .all()
    )
    last_updated = {r[0]: r[1] for r in last_updated_rows}

    total_offers = db.query(func.count(ProductOffer.id)).scalar() or 0
    distinct_scrapes = (
        db.query(ProductOffer.scraped_at)
        .distinct()
        .count()
    )

    lines.append("RETAILERS")
    lines.append("-" * 60)
    lines.append(f"{'Retailer':<20} {'Type':<16} {'Products':>10} {'Last Update':>20}")
    lines.append("-" * 60)

    grand_total = 0
    for rid in sorted(all_meta.keys()):
        meta = all_meta[rid]
        count = product_counts.get(rid, 0)
        grand_total += count
        updated = last_updated.get(rid)
        updated_str = updated.strftime("%Y-%m-%d %H:%M") if updated else "never"
        lines.append(
            f"{meta.display_name:<20} {meta.catalog_type.value:<16} {count:>10,} {updated_str:>20}"
        )

    lines.append("-" * 60)
    lines.append(f"{'TOTAL (latest)':<36} {grand_total:>10,}")
    lines.append(f"{'Total rows in DB':<36} {total_offers:>10,}")
    lines.append(f"{'Distinct scrape runs':<36} {distinct_scrapes:>10,}")
    lines.append("")

    # --- Basket index ---
    lines.append("BASKET INDEX (last 7 days)")
    lines.append("-" * 60)
    recent_baskets = (
        db.query(BasketIndex)
        .order_by(BasketIndex.date.desc())
        .limit(50)
        .all()
    )
    if recent_baskets:
        seen_dates: dict[str, list[str]] = {}
        for b in recent_baskets:
            seen_dates.setdefault(b.date, []).append(
                f"  {b.retailer_id:<16} {b.basket_total:>8.2f} EUR"
            )
        for date_str in sorted(seen_dates.keys(), reverse=True)[:7]:
            lines.append(f"\n  {date_str}:")
            for entry in seen_dates[date_str]:
                lines.append(entry)
    else:
        lines.append("  No basket index data yet.")
    lines.append("")

    # --- Recent anomalies ---
    lines.append("PRICE ANOMALIES (last 7 days)")
    lines.append("-" * 60)
    anomaly_counts = (
        db.query(
            PriceAnomaly.anomaly_type,
            func.count(PriceAnomaly.id),
        )
        .filter(
            PriceAnomaly.date >= (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        )
        .group_by(PriceAnomaly.anomaly_type)
        .all()
    )
    if anomaly_counts:
        for atype, acount in anomaly_counts:
            lines.append(f"  {atype:<25} {acount:>6}")
    else:
        lines.append("  No anomalies detected.")
    lines.append("")

    # --- Recent ingest logs ---
    lines.append("INGESTION LOG (last 5 runs per retailer)")
    lines.append("-" * 60)
    recent_logs = (
        db.query(IngestLog)
        .order_by(IngestLog.date.desc())
        .limit(30)
        .all()
    )
    if recent_logs:
        lines.append(f"{'Date':<12} {'Retailer':<16} {'Products':>10} {'Duration':>10}")
        for log in recent_logs[:20]:
            dur = f"{log.duration_seconds:.0f}s" if log.duration_seconds else "N/A"
            lines.append(
                f"{log.date:<12} {log.retailer_id:<16} {log.product_count:>10,} {dur:>10}"
            )
    else:
        lines.append("  No ingestion logs yet.")
    lines.append("")

    lines.append("=" * 60)
    lines.append("End of report")
    lines.append("=" * 60)

    return "\n".join(lines)
