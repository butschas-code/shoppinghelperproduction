"""Tests for query parsing, attribute scoring, and basket matching.

Covers the core fix: natural-language queries like "1 liter milk 2.0% fat"
must match actual milk products and reject derivative products (sausage,
chocolate, etc.) that happen to contain the word "piena".
"""

from __future__ import annotations

import pytest

from app.services.match import (
    CONFIDENCE_REJECT,
    CONFIDENCE_STRONG,
    CONFIDENCE_WEAK,
    match_product,
)
from app.services.query_parser import (
    ParsedQuery,
    attribute_boost,
    parse_grocery_query,
)


# ── query parser ────────────────────────────────────────────────────

class TestParseGroceryQuery:
    def test_milk_with_attributes(self) -> None:
        pq = parse_grocery_query("1 liter milk 2.0% fat")
        assert pq.core_terms == "milk"
        assert pq.fat_pct == pytest.approx(2.0)
        assert pq.volume_ml == pytest.approx(1000.0)
        assert "piens" in pq.expanded_core

    def test_latvian_milk(self) -> None:
        pq = parse_grocery_query("piens 2.5% 1l")
        assert pq.core_terms == "piens"
        assert pq.fat_pct == pytest.approx(2.5)
        assert pq.volume_ml == pytest.approx(1000.0)

    def test_weight_extraction(self) -> None:
        pq = parse_grocery_query("vistas fileja 1kg")
        assert "vistas" in pq.core_terms
        assert "fileja" in pq.core_terms
        assert pq.weight_g == pytest.approx(1000.0)

    def test_eggs(self) -> None:
        pq = parse_grocery_query("eggs")
        assert pq.core_terms == "eggs"
        assert "olas" in pq.expanded_core

    def test_bare_keyword(self) -> None:
        pq = parse_grocery_query("butter")
        assert pq.core_terms == "butter"
        assert pq.fat_pct is None
        assert pq.volume_ml is None

    def test_percent_before_keyword(self) -> None:
        pq = parse_grocery_query("2% milk")
        assert pq.core_terms == "milk"
        assert pq.fat_pct == pytest.approx(2.0)

    def test_empty_query(self) -> None:
        pq = parse_grocery_query("")
        assert pq.core_terms == ""


# ── attribute boost ─────────────────────────────────────────────────

class TestAttributeBoost:
    def _pq(self, fat: float | None = None, vol: float | None = None) -> ParsedQuery:
        return ParsedQuery(
            raw="test", core_terms="test", expanded_core="test",
            fat_pct=fat, volume_ml=vol,
        )

    def test_exact_fat_match(self) -> None:
        pq = self._pq(fat=2.0)
        assert attribute_boost(pq, "Piens 2% 1l") > 0.15

    def test_close_fat_match(self) -> None:
        pq = self._pq(fat=2.0)
        boost = attribute_boost(pq, "Piens 2.5% 1l")
        assert boost > 0.05

    def test_far_fat_mismatch(self) -> None:
        pq = self._pq(fat=2.0)
        boost = attribute_boost(pq, "Piens 9% 1l")
        assert boost < 0

    def test_volume_match(self) -> None:
        pq = self._pq(vol=1000.0)
        assert attribute_boost(pq, "Piens 2.5% 1l") > 0.05

    def test_no_attributes_no_boost(self) -> None:
        pq = self._pq()
        assert attribute_boost(pq, "Piens 2.5% 1l") == 0.0


# ── match_product (basket matching) ────────────────────────────────

class TestMatchProduct:
    def test_milk_sausage_is_weak(self) -> None:
        """'Piena desa' must be WEAK when searching for milk."""
        pq = parse_grocery_query("1 liter milk 2.0% fat")
        _, conf = match_product(pq.expanded_core, "Piena desa Ķekava cāļa 700g", parsed=pq)
        assert conf == CONFIDENCE_WEAK

    def test_milk_chocolate_is_weak(self) -> None:
        pq = parse_grocery_query("milk")
        _, conf = match_product(pq.expanded_core, "KRAUKŠĶI SMASH PIENA ŠOKOLĀDE 100G", parsed=pq)
        assert conf == CONFIDENCE_WEAK

    def test_real_milk_is_not_weak(self) -> None:
        pq = parse_grocery_query("1 liter milk 2.0% fat")
        score, conf = match_product(pq.expanded_core, "Piens 2% 1l", parsed=pq)
        assert conf != CONFIDENCE_WEAK
        assert conf != CONFIDENCE_REJECT

    def test_exact_milk_beats_wrong_fat(self) -> None:
        pq = parse_grocery_query("1 liter milk 2.0% fat")
        s_exact, _ = match_product(pq.expanded_core, "Piens 2% 1l", parsed=pq)
        s_wrong, _ = match_product(pq.expanded_core, "Piens FARM MILK 3.2% UHT 1l", parsed=pq)
        assert s_exact >= s_wrong

    def test_real_milk_beats_sausage(self) -> None:
        pq = parse_grocery_query("milk")
        s_milk, c_milk = match_product(pq.expanded_core, "Piens 2.5% 1l", parsed=pq)
        s_desa, c_desa = match_product(pq.expanded_core, "Piena desa Ķekava 700g", parsed=pq)
        assert c_desa == CONFIDENCE_WEAK
        assert c_milk != CONFIDENCE_WEAK

    def test_chicken_sausage_is_weak(self) -> None:
        pq = parse_grocery_query("chicken")
        _, conf = match_product(pq.expanded_core, "Vistas cīsiņi 300g", parsed=pq)
        assert conf == CONFIDENCE_WEAK

    def test_non_matching_product_rejected(self) -> None:
        pq = parse_grocery_query("milk")
        _, conf = match_product(pq.expanded_core, "Banāni 1kg", parsed=pq)
        assert conf == CONFIDENCE_REJECT

    def test_latvian_single_word_still_works(self) -> None:
        pq = parse_grocery_query("piens")
        score, conf = match_product(pq.expanded_core, "Piens 2.5% 1l", parsed=pq)
        assert conf in (CONFIDENCE_STRONG, "ok")
        assert score >= 0.50
