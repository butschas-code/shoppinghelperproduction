"""Regression: metadata keys on ingest summary must not count as failed retailers."""

from app.services.ingest import is_retailer_ingest_key


def test_is_retailer_ingest_key_excludes_metadata() -> None:
    assert is_retailer_ingest_key("rimi_lv") is True
    assert is_retailer_ingest_key("maxima_lv") is True
    assert is_retailer_ingest_key("_anomalies") is False
    assert is_retailer_ingest_key("_health") is False
