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
    passes_attribute_constraints,
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


# ── strict attribute filter (intent search) ─────────────────────────

class TestPassesAttributeConstraints:
    """Strict attribute gate — must work for ALL intents, not just milk."""

    # ── milk ────────────────────────────────────────────────────────

    def test_milk_2pct_1l_accepts_plain(self) -> None:
        pq = parse_grocery_query("1 liter milk 2.0% fat")
        assert passes_attribute_constraints(pq, "Piens 2% 1l", "milk")

    def test_milk_rejects_flavored_rasens(self) -> None:
        pq = parse_grocery_query("1 liter milk 2.0% fat")
        assert not passes_attribute_constraints(
            pq, "Piens RASĒNS zemeņu 1,5% 200ml", "milk",
        )

    def test_milk_rejects_wrong_volume(self) -> None:
        pq = parse_grocery_query("1 liter milk 2.0% fat")
        assert not passes_attribute_constraints(pq, "Piens 2% 200ml", "milk")

    def test_milk_rejects_griki(self) -> None:
        pq = parse_grocery_query("1 liter milk 2.0% fat")
        assert not passes_attribute_constraints(pq, "GOLDEN SUN Griķi 100 g", "milk")

    def test_milk_rejects_wrong_fat(self) -> None:
        pq = parse_grocery_query("1 liter milk 2.0% fat")
        assert not passes_attribute_constraints(pq, "PIENS TIP TOP 2.5% 1L", "milk")

    def test_milk_rejects_cottage_cheese(self) -> None:
        pq = parse_grocery_query("piens 2% 1l")
        assert not passes_attribute_constraints(pq, "Pilos Biezpiens 180g", "milk")

    def test_milk_rejects_dessert(self) -> None:
        pq = parse_grocery_query("piens 2% 1l")
        assert not passes_attribute_constraints(
            pq, "Piena deserts Monte šok. un lazdu riekstu 55g", "milk",
        )

    # ── chicken ─────────────────────────────────────────────────────

    def test_chicken_1kg_accepts_fillet(self) -> None:
        pq = parse_grocery_query("chicken 1kg")
        assert passes_attribute_constraints(pq, "Vistas fileja 1kg", "chicken")

    def test_chicken_rejects_wrong_weight(self) -> None:
        pq = parse_grocery_query("chicken 1kg")
        assert not passes_attribute_constraints(pq, "Vistas fileja 300g", "chicken")

    def test_chicken_rejects_sausage_with_weight(self) -> None:
        pq = parse_grocery_query("chicken 1kg")
        assert not passes_attribute_constraints(
            pq, "Vistas cīsiņi 1kg", "chicken",
            exclude_roots=["cisin", "desa", "pelmen"],
        )

    def test_chicken_rejects_chips(self) -> None:
        pq = parse_grocery_query("chicken 500g")
        assert not passes_attribute_constraints(
            pq, "Čipsi vistas garšas 200g", "chicken",
            exclude_roots=["cips", "krauksk"],
        )

    def test_chicken_rejects_no_weight_in_title(self) -> None:
        pq = parse_grocery_query("chicken 1kg")
        assert not passes_attribute_constraints(pq, "Vistas buljons", "chicken")

    # ── yogurt ──────────────────────────────────────────────────────

    def test_yogurt_500g_accepts_match(self) -> None:
        pq = parse_grocery_query("yogurt 2.5% 500g")
        assert passes_attribute_constraints(pq, "Jogurts dabīgais 2.5% 500g", "yogurt")

    def test_yogurt_rejects_wrong_fat(self) -> None:
        pq = parse_grocery_query("yogurt 2.5% 500g")
        assert not passes_attribute_constraints(pq, "Jogurts 0.1% 500g", "yogurt")

    def test_yogurt_rejects_wrong_weight(self) -> None:
        pq = parse_grocery_query("yogurt 2.5% 500g")
        assert not passes_attribute_constraints(pq, "Jogurts 2.5% 125g", "yogurt")

    # ── cheese ──────────────────────────────────────────────────────

    def test_cheese_rejects_chips_via_exclude(self) -> None:
        pq = parse_grocery_query("cheese 200g")
        assert not passes_attribute_constraints(
            pq, "Siera čipsi 200g", "cheese",
            exclude_roots=["cips", "krauksk"],
        )

    def test_cheese_accepts_plain(self) -> None:
        pq = parse_grocery_query("cheese 200g")
        assert passes_attribute_constraints(pq, "Siers Tilžas 200g", "cheese")

    # ── rice ────────────────────────────────────────────────────────

    def test_rice_1kg_accepts(self) -> None:
        pq = parse_grocery_query("rice 1kg")
        assert passes_attribute_constraints(pq, "Rīsi basmati 1kg", "rice")

    def test_rice_rejects_wrong_weight(self) -> None:
        pq = parse_grocery_query("rice 1kg")
        assert not passes_attribute_constraints(pq, "Rīsi 400g", "rice")

    # ── size_text fallback ──────────────────────────────────────────

    def test_accepts_via_size_text_when_title_lacks_volume(self) -> None:
        pq = parse_grocery_query("juice 1l")
        assert passes_attribute_constraints(
            pq, "Sula ābolu", "juice", size_text="1 l",
        )

    def test_rejects_when_neither_title_nor_size_text_has_volume(self) -> None:
        pq = parse_grocery_query("juice 1l")
        assert not passes_attribute_constraints(pq, "Sula ābolu", "juice")

    # ── no attributes = always pass ─────────────────────────────────

    def test_no_attributes_always_passes(self) -> None:
        pq = parse_grocery_query("milk")
        assert passes_attribute_constraints(pq, "Anything at all", "milk")

    # ── strict EXACT matching (no soft tolerance) ───────────────────

    def test_milk_1l_2pct_rejects_500ml(self) -> None:
        # "1l milk 2.0% fat" must NOT match 500ml bottle, even with correct fat.
        pq = parse_grocery_query("1l milk 2.0% fat")
        assert not passes_attribute_constraints(pq, "Piens 2% 500ml", "milk")

    def test_milk_1l_2pct_rejects_15pct(self) -> None:
        # 1.5% is close but not equal to 2.0% — must reject under strict mode.
        pq = parse_grocery_query("1l milk 2.0% fat")
        assert not passes_attribute_constraints(pq, "Piens 1.5% 1L", "milk")

    def test_milk_1l_2pct_accepts_exact_match_en(self) -> None:
        pq = parse_grocery_query("1l milk 2.0% fat")
        assert passes_attribute_constraints(pq, "Piens 2.0% 1L", "milk")

    def test_milk_exact_match_lv(self) -> None:
        # Latvian query form "piens 2,0% 1l" with LV fat label "tauku".
        pq = parse_grocery_query("piens 2,0% tauku 1l")
        assert passes_attribute_constraints(pq, "Piens 2.0% 1L", "milk")
        assert not passes_attribute_constraints(pq, "Piens 2.5% 1L", "milk")
        assert not passes_attribute_constraints(pq, "Piens 2.0% 500ml", "milk")

    def test_volume_unit_conversion_1l_equals_1000ml(self) -> None:
        # User types "1l", title says "1000ml" — still an exact volume match.
        pq = parse_grocery_query("1l milk 2.0% fat")
        assert passes_attribute_constraints(pq, "Piens 2.0% 1000ml", "milk")

    def test_count_exact_match_en(self) -> None:
        pq = parse_grocery_query("10 pcs eggs")
        assert pq.count == 10
        assert passes_attribute_constraints(pq, "Olas M 10 pcs", "eggs")
        assert not passes_attribute_constraints(pq, "Olas M 6 pcs", "eggs")

    def test_count_exact_match_lv(self) -> None:
        # Latvian form: "10 gab olas"
        pq = parse_grocery_query("10 gab olas")
        assert pq.count == 10
        assert passes_attribute_constraints(pq, "Olas L 10 gab", "eggs")
        assert not passes_attribute_constraints(pq, "Olas L 6 gab", "eggs")

    def test_count_without_unit_in_title_rejected(self) -> None:
        pq = parse_grocery_query("10 gab olas")
        # No explicit count in title — must reject under strict count mode.
        assert not passes_attribute_constraints(pq, "Olas svaigas", "eggs")

    # ── attribute gate applies without intent (fallback branch) ─────

    def test_attribute_gate_without_intent(self) -> None:
        # "coconut water" doesn't map to a built-in intent, but "1l" must
        # still filter candidates in the fuzzy fallback.
        pq = parse_grocery_query("coconut water 1l")
        assert pq.volume_ml == 1000
        assert not passes_attribute_constraints(pq, "Coconut water 330ml", None)
        assert passes_attribute_constraints(pq, "Coconut water 1L", None)


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
