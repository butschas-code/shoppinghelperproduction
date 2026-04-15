"""Logic tests for grocery search: detector, classifier, and search behavior.

Run: python -m pytest tests/test_grocery_search.py -v
"""

from __future__ import annotations

import unittest

from app.services.product_type_detector import detect_product_type
from app.services.product_classifier import detect_product_type_from_title
from app.services.normalize import normalize_text
from app.services.product_search import _score_offer, HARD_REJECT


class _Offer:
    """Minimal stand-in for ProductOffer used by _score_offer."""
    def __init__(self, title: str, product_type: str | None = None):
        self.title = title
        self.product_type = product_type


class TestProductTypeDetector(unittest.TestCase):
    def test_milk(self):
        self.assertEqual(detect_product_type("milk"), "milk")
        self.assertEqual(detect_product_type("Milk"), "milk")
        self.assertEqual(detect_product_type("piens"), "milk")

    def test_avocado(self):
        self.assertEqual(detect_product_type("avocado"), "avocado")
        self.assertEqual(detect_product_type("avokado"), "avocado")

    def test_yogurt(self):
        self.assertEqual(detect_product_type("jogurts"), "yogurt")

    def test_dish_soap(self):
        self.assertEqual(detect_product_type("dish soap"), "dish_soap")
        self.assertEqual(detect_product_type("trauku"), "dish_soap")

    def test_coffee(self):
        self.assertEqual(detect_product_type("coffee"), "coffee")
        self.assertEqual(detect_product_type("kafija"), "coffee")

    def test_unknown_returns_none(self):
        self.assertIsNone(detect_product_type("xyz random"))
        self.assertIsNone(detect_product_type(""))


class TestProductClassifier(unittest.TestCase):
    def test_milk_titles(self):
        self.assertEqual(detect_product_type_from_title("Piens 2.5% 1L"), "milk")
        self.assertEqual(detect_product_type_from_title("Milk UHT 1L"), "milk")

    def test_yogurt_titles(self):
        self.assertEqual(detect_product_type_from_title("Jogurts vaniļas"), "yogurt")
        self.assertEqual(detect_product_type_from_title("Jogurts 400g"), "yogurt")

    def test_avocado_real_fruit(self):
        self.assertEqual(detect_product_type_from_title("Avokado 1kg"), "avocado")
        self.assertEqual(detect_product_type_from_title("Avocado hass"), "avocado")

    def test_milk_must_not_include_chocolate_milk(self):
        """Milk search MUST NOT return chocolate milk."""
        self.assertNotEqual(detect_product_type_from_title("Šokolādes piens 1L"), "milk")
        self.assertNotEqual(detect_product_type_from_title("Chocolate milk"), "milk")

    def test_avocado_must_show_real_avocados_first(self):
        """Avocado oil/salsa must not classify as avocado fruit."""
        self.assertEqual(detect_product_type_from_title("Avokado 2 gab"), "avocado")
        self.assertNotEqual(detect_product_type_from_title("Avocado oil 250ml"), "avocado")
        self.assertNotEqual(detect_product_type_from_title("Avocado salsa"), "avocado")

    def test_dish_soap(self):
        self.assertEqual(detect_product_type_from_title("Trauku mazgāšanas līdzeklis"), "dish_soap")
        self.assertEqual(detect_product_type_from_title("Dish soap Fairy"), "dish_soap")

    def test_coffee(self):
        self.assertEqual(detect_product_type_from_title("Kafija melna"), "coffee")
        self.assertEqual(detect_product_type_from_title("Coffee beans 250g"), "coffee")


class TestScoreOfferRejects(unittest.TestCase):
    """Bad-result regressions — titles that used to leak into intent results."""

    def _score(self, title: str, intent: str) -> int:
        return _score_offer(_Offer(title), intent, normalize_text(title))

    def test_apple_candy_rejected_for_apple_intent(self):
        # "Ābolu konfekte" used to score +100 (starts "aboli") -80 (konfekt) = +20.
        self.assertEqual(self._score("Ābolu konfekte 100g", "apple"), HARD_REJECT)

    def test_apple_chips_rejected_for_apple_intent(self):
        self.assertEqual(self._score("Ābolu čipsi 50g", "apple"), HARD_REJECT)

    def test_pineapple_not_matched_as_apple(self):
        # "pineapple" contains "apple" — the old substring first-word check
        # would score +60. Classifier returns None for this title, so the
        # only safeguard is the prefix-only primary-root match.
        self.assertLessEqual(self._score("Pineapple 1kg", "apple"), 0)

    def test_milk_drink_rejected_for_milk_intent(self):
        # "Piena dzēriens" (flavoured milk drink) is not plain milk.
        self.assertEqual(self._score("Piena dzēriens šokolādes 1L", "milk"), HARD_REJECT)

    def test_chocolate_milk_rejected_for_milk_intent(self):
        self.assertEqual(self._score("Piena šokolāde 100g", "milk"), HARD_REJECT)

    def test_sausage_not_matched_as_beef(self):
        # "gala" is no longer a primary root for beef; a sausage must not
        # score as beef.
        self.assertLessEqual(self._score("Vistas gaļas desa 400g", "beef"), 0)

    def test_fish_fillet_not_matched_as_chicken(self):
        # "fileja" is no longer a chicken primary root.
        self.assertLessEqual(self._score("Laša fileja 200g", "chicken"), 0)

    def test_plain_milk_still_matches(self):
        self.assertGreater(self._score("Piens 2.5% 1L", "milk"), 0)

    def test_plain_apple_still_matches(self):
        self.assertGreater(self._score("Āboli 1kg", "apple"), 0)

    def test_plain_chicken_fillet_still_matches(self):
        # Chicken fillet should still match chicken intent via "vistas" prefix.
        self.assertGreater(self._score("Vistas fileja 500g", "chicken"), 0)


if __name__ == "__main__":
    unittest.main()
