import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")


def _normalize_database_url(url: str) -> str:
    """Use psycopg3 driver for plain postgres:// URLs (Neon, Railway, etc.)."""
    if url.startswith("postgres://"):
        rest = url[len("postgres://") :]
        return "postgresql+psycopg://" + rest
    if url.startswith("postgresql://") and not url.startswith("postgresql+"):
        rest = url[len("postgresql://") :]
        return "postgresql+psycopg://" + rest
    return url


DATABASE_URL: str = _normalize_database_url(os.getenv("DATABASE_URL", "sqlite:///prices.db"))

# Postgres (Neon, etc.): recycle pooled connections before idle/proxy timeouts close them.
DATABASE_POOL_RECYCLE: int = int(os.getenv("DATABASE_POOL_RECYCLE", "300"))

# Ingest: commit every N offers so one retailer does not hold a single huge transaction
# (reduces "SSL connection has been closed unexpectedly" on long inserts).
INGEST_COMMIT_BATCH: int = max(1, int(os.getenv("INGEST_COMMIT_BATCH", "1000")))

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

USER_AGENT: str = os.getenv(
    "USER_AGENT",
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
)

REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "15"))
RETRY_COUNT: int = int(os.getenv("RETRY_COUNT", "3"))
RATE_LIMIT_MIN: float = float(os.getenv("RATE_LIMIT_MIN", "0.5"))
RATE_LIMIT_MAX: float = float(os.getenv("RATE_LIMIT_MAX", "1.5"))

# ---------------------------------------------------------------------------
# Alert / SMTP settings (optional — email alerts disabled when host is empty)
# ---------------------------------------------------------------------------
ALERT_MIN_PRODUCTS: int = int(os.getenv("ALERT_MIN_PRODUCTS", "100"))
SMTP_HOST: str = os.getenv("SMTP_HOST", "")
SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER: str = os.getenv("SMTP_USER", "")
SMTP_PASS: str = os.getenv("SMTP_PASS", "")
SMTP_FROM: str = os.getenv("SMTP_FROM", "")
SMTP_TO: str = os.getenv("SMTP_TO", "")  # comma-separated recipients

# Base URL for confirmation links in emails (no trailing slash)
BASE_URL: str = os.getenv("BASE_URL", "http://localhost:8000").rstrip("/")

# Optional: show last GitHub Actions ingest time on homepage (e.g. butschas-code/shoppinghelperproduction)
GITHUB_REPOSITORY: str = os.getenv("GITHUB_REPOSITORY", "").strip()
GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_WORKFLOW_FILE: str = os.getenv("GITHUB_WORKFLOW_FILE", "daily-ingest.yml").strip()

# ---------------------------------------------------------------------------
# App / admin (production: set ADMIN_SECRET; never enable ALLOW_INSECURE_ADMIN)
# ---------------------------------------------------------------------------
APP_DEBUG: bool = os.getenv("APP_DEBUG", "false").lower() in ("1", "true", "yes")
ADMIN_SECRET: str = os.getenv("ADMIN_SECRET", "").strip()
ALLOW_INSECURE_ADMIN: bool = os.getenv("ALLOW_INSECURE_ADMIN", "").lower() in (
    "1",
    "true",
    "yes",
)

# ---------------------------------------------------------------------------
# Retailer category URLs
# Each adapter fetches products from every URL in its list.
# Rimi URLs discovered via their /e-veikals/api/v1/content/category-tree API.
# ---------------------------------------------------------------------------
_RIMI = "https://www.rimi.lv/e-veikals/lv/produkti"

RETAILER_CATEGORY_URLS: dict[str, list[str]] = {
    "rimi_lv": [
        # ── Fruits & Vegetables ──────────────────────────────────────
        f"{_RIMI}/augli-un-darzeni/augli-un-ogas/c/SH-2-1",
        f"{_RIMI}/augli-un-darzeni/darzeni/c/SH-2-2",
        # ── Dairy ────────────────────────────────────────────────────
        f"{_RIMI}/piena-produkti-un-olas/piens/c/SH-11-8",
        f"{_RIMI}/piena-produkti-un-olas/sviests-un-margarins/c/SH-11-7",
        f"{_RIMI}/piena-produkti-un-olas/siers/c/SH-11-9",
        f"{_RIMI}/piena-produkti-un-olas/jogurti-un-deserti/c/SH-11-2",
        f"{_RIMI}/piena-produkti-un-olas/biezpiens/c/SH-11-1",
        f"{_RIMI}/piena-produkti-un-olas/krejums/c/SH-11-4",
        # ── Eggs ─────────────────────────────────────────────────────
        f"{_RIMI}/piena-produkti-un-olas/olas/c/SH-11-6",
        # ── Bread ────────────────────────────────────────────────────
        f"{_RIMI}/maize-un-konditoreja/maize/c/SH-7-2",
        # ── Meat & Chicken ───────────────────────────────────────────
        f"{_RIMI}/gala-zivis-un-gatava-kulinarija/svaiga-gala/c/SH-6-15",
        f"{_RIMI}/gala-zivis-un-gatava-kulinarija/desas-un-pastetes/c/SH-6-2",
        f"{_RIMI}/gala-zivis-un-gatava-kulinarija/cisini-sardeles-desinas/c/SH-6-1",
        # ── Pasta & Rice / Grains ────────────────────────────────────
        f"{_RIMI}/iepakota-partika/makaroni/c/SH-4-11",
        f"{_RIMI}/iepakota-partika/graudaugi-un-putraimi/c/SH-4-5",
        # ── Canned goods ─────────────────────────────────────────────
        f"{_RIMI}/iepakota-partika/konserveti-produkti/c/SH-4-10",
        # ── Snacks ───────────────────────────────────────────────────
        f"{_RIMI}/saldumi-un-uzkodas/cipsi-un-dip-merces/c/SH-13-4",
        # ── Drinks ───────────────────────────────────────────────────
        f"{_RIMI}/dzerieni/sulas-un-sulu-dzerieni/c/SH-5-8",
        f"{_RIMI}/dzerieni/udens/c/SH-5-12",
        # ── Frozen food ──────────────────────────────────────────────
        f"{_RIMI}/saldetie-edieni/saldeti-edieni-un-konditorejas-izstradajumi/c/SH-12-5",
    ],
    "maxima_lv": [],  # Barbora.lv — leaf categories auto-discovered via Playwright (no URLs needed)
    "top_lv": [],     # etop.lv JSON API — full promo list in one call (no URLs needed)
}
