"""Neon pooler: strip unsupported query params from DATABASE_URL in code."""

from app.core.config import _strip_neon_incompatible_query_params


def test_strip_options_param() -> None:
    url = (
        "postgresql+psycopg://u:p@ep-test.region.aws.neon.tech/db"
        "?sslmode=require&options=-c%20idle_in_transaction_session_timeout%3D900000"
    )
    out = _strip_neon_incompatible_query_params(url)
    assert "options=" not in out.lower()
    assert "sslmode=require" in out


def test_strip_idle_in_transaction_param() -> None:
    url = (
        "postgresql+psycopg://u:p@host/db"
        "?idle_in_transaction_session_timeout=900000&sslmode=require"
    )
    out = _strip_neon_incompatible_query_params(url)
    assert "idle_in_transaction" not in out.lower()
    assert "sslmode=require" in out


def test_sqlite_unchanged() -> None:
    assert _strip_neon_incompatible_query_params("sqlite:///prices.db") == "sqlite:///prices.db"
