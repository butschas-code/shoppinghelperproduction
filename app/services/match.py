"""Product matching with category-aware keyword filtering, product-type
penalty system, and structured-attribute scoring.

Two entry points:
- similarity_score()  – general fuzzy search (for /search)
- match_product()     – strict category-aware match (for /basket)

The penalty system prevents generic searches like "vista" (chicken) from
matching processed products like "vistas pastēte" (chicken pate) when
real/primary products (chicken fillets, legs, etc.) are available.

When the explicit KEYWORD_FILTER doesn't cover a query the system falls
back to product-intent detection so that ANY known product type (milk,
avocado, fish, toilet paper…) gets precision filtering automatically.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from app.services.normalize import (
    normalize_text,
    stem_latvian_token,
    tokenize,
    tokenize_for_match,
    trigrams,
)
from app.services.query_parser import ParsedQuery, attribute_boost, parse_grocery_query

# ---------------------------------------------------------------------------
# Grocery keyword filter
#
# Maps normalised (and optionally stemmed) query tokens → root prefixes that
# MUST appear at the START of a token in the product title.  Stems are used
# for lookup when tokenize_for_match is used (piens/piena → pien).
# ---------------------------------------------------------------------------
KEYWORD_FILTER: dict[str, list[str]] = {
    # ── dairy (stems + forms) ──
    "pien":      ["pien"],
    "piens":     ["pien"],
    "piena":     ["pien"],
    "pienu":     ["pien"],
    "milk":      ["pien", "milk"],
    "sier":      ["sier"],
    "siers":     ["sier"],
    "siera":     ["sier"],
    "sieru":     ["sier"],
    "cheese":    ["sier", "cheese"],
    "jogurt":    ["jogurt"],
    "jogurts":   ["jogurt"],
    "jogurta":   ["jogurt"],
    "jogurtu":   ["jogurt"],
    "yogurt":    ["jogurt", "yogurt", "yoghurt"],
    "yoghurt":   ["jogurt", "yogurt", "yoghurt"],
    "sviest":    ["sviest"],
    "sviests":   ["sviest"],
    "sviesta":   ["sviest"],
    "sviestu":   ["sviest"],
    "butter":    ["sviest", "butter"],
    "biezpien":  ["biezpien"],
    "biezpiens": ["biezpien"],
    "krejum":    ["krejum"],
    "krejums":   ["krejum"],
    "kefir":     ["kefir"],
    "kefirs":    ["kefir"],
    "kefira":    ["kefir"],
    # ── eggs ──
    "ola":   ["ola", "olu"],
    "olas":  ["ola", "olu"],
    "olu":   ["ola", "olu"],
    "eggs":  ["ola", "olu", "egg"],
    "egg":   ["ola", "olu", "egg"],
    # ── bread ──
    "maiz":   ["maiz"],
    "maize":  ["maiz"],
    "maizes": ["maiz"],
    "maizi":  ["maiz"],
    "bread":  ["maiz", "bread"],
    # ── meat / chicken ──
    "vist":     ["vist", "cal", "majputn"],
    "vista":    ["vist", "cal", "majputn"],
    "vistas":   ["vist", "cal", "majputn"],
    "vistiena": ["vist", "cal", "majputn"],
    "chicken":  ["vist", "chicken"],
    "gal":      ["gal", "cukg", "liellop", "vist", "cal", "jera", "trus"],
    "gala":     ["gal", "cukg", "liellop", "vist", "cal", "jera", "trus"],
    "galas":    ["gal", "cukg", "liellop", "vist", "cal", "jera", "trus"],
    "cukgala":  ["cukg"],
    "cukgal":   ["cukg"],
    "beef":     ["liellop", "gal", "beef"],
    "liellop":  ["liellop"],
    "pork":     ["cukg", "cuk", "pork"],
    "malt":     ["malt", "far"],
    "far":      ["malt", "far"],
    "minced":   ["malt", "minc"],
    # ── sausages ──
    "des":      ["des"],
    "desas":    ["des"],
    "desa":     ["des"],
    "cisin":    ["cisin", "sardel"],
    "cisini":   ["cisin", "sardel"],
    "sardel":   ["sardel", "cisin"],
    "sardeles": ["sardel", "cisin"],
    # ── fish ──
    "zivi":   ["ziv"],
    "ziv":    ["ziv"],
    "zivis":  ["ziv"],
    "fish":   ["ziv", "fish", "lasi", "salmon"],
    "lasi":   ["lasi", "salmon"],
    "salmon": ["lasi", "salmon"],
    # ── grains ──
    "ris":      ["ris"],
    "risi":     ["ris"],
    "risu":     ["ris"],
    "rice":     ["ris", "rice", "basmat", "jasmin"],
    "grik":     ["grik"],
    "griki":    ["grik"],
    "putraim":  ["putraim"],
    "putraimi": ["putraim"],
    # ── pasta ──
    "makaron":   ["makaron"],
    "makaroni":  ["makaron"],
    "makaronus": ["makaron"],
    "pasta":     ["makaron", "past", "spaget", "penne"],
    "spaget":    ["spaget"],
    # ── fruit ──
    "banan":  ["banan"],
    "banani": ["banan"],
    "bananu": ["banan"],
    "banana": ["banan"],
    "abol":   ["abol"],
    "aboli":  ["abol"],
    "apple":  ["abol", "apple"],
    "avokado":["avokado", "avocado"],
    "avocado":["avokado", "avocado"],
    "tomat":  ["tomat"],
    "tomati": ["tomat"],
    "tomato": ["tomat", "tomato"],
    # ── vegetables ──
    "kartupel":  ["kartupel"],
    "kartupeli": ["kartupel"],
    "kartupelu": ["kartupel"],
    "potato":    ["kartupel", "potato"],
    "sipol":     ["sipol"],
    "sipoli":    ["sipol"],
    "onion":     ["sipol", "onion"],
    "burkani":   ["burkan"],
    "burkan":    ["burkan"],
    "carrot":    ["burkan", "carrot"],
    "gurk":      ["gurk"],
    "gurki":     ["gurk"],
    "cucumber":  ["gurk", "cucumber"],
    # ── pantry / condiments ──
    "cukur":  ["cukur"],
    "cuk":    ["cuk"],
    "cukurs": ["cukur"],
    "cukura": ["cukur"],
    "sugar":  ["cukur", "sugar"],
    "milt":   ["milt"],
    "milti":  ["milt"],
    "flour":  ["milt", "flour"],
    "ell":    ["ell"],
    "ella":   ["ell"],
    "oil":    ["ell", "oil"],
    "sal":    ["sal"],
    "sals":   ["sal"],
    "salt":   ["sal", "salt"],
    "med":    ["med"],
    "medus":  ["med"],
    "honey":  ["med", "honey"],
    "kafij":  ["kafij"],
    "kafija": ["kafij"],
    "kafiju": ["kafij"],
    "coffee": ["kafij", "coffee"],
    "tej":    ["tej"],
    "teja":   ["tej"],
    "tea":    ["tej", "tea"],
    # ── sweets ──
    "sokolad":  ["sokolad", "chocolat"],
    "sokolade": ["sokolad", "chocolat"],
    "chocolate":["sokolad", "chocolat"],
    "cepum":    ["cepum"],
    "cepumi":   ["cepum"],
    "cookie":   ["cepum", "cookie", "biscuit"],
    "cookies":  ["cepum", "cookie", "biscuit"],
    # ── drinks ──
    "uden":  ["uden"],
    "udens": ["uden"],
    "water": ["uden", "water"],
    "sul":   ["sul"],
    "sula":  ["sul"],
    "sulas": ["sul"],
    "sulu":  ["sul"],
    "juice": ["sul", "juice"],
    "limonad":   ["limonad"],
    "limonade":  ["limonad"],
    "limonades": ["limonad"],
    # ── frozen ──
    "saldejum":  ["saldejum"],
    "saldejums": ["saldejum"],
    "pelmen":    ["pelmen"],
    "pelmeni":   ["pelmen"],
    # ── canned ──
    "konserv":  ["konserv"],
    "konservi": ["konserv"],
    # ── snacks ──
    "cips":  ["cips"],
    "cipsi": ["cips"],
    "cipsu": ["cips"],
    # ── non-food ──
    "sampun":  ["sampun", "shampoo"],
    "shampoo": ["sampun", "shampoo"],
    "trauk":   ["trauk"],
    "zobu":    ["zobu"],
}


# ---------------------------------------------------------------------------
# Product-type penalty system
#
# When a product title contains a penalty token the match is classified as
# CONFIDENCE_WEAK.  The basket engine then prefers primary (non-penalized)
# matches and rejects weak-only results entirely.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _CategoryProfile:
    penalty_roots: tuple[str, ...]


# Reusable penalty token groups
_SNACK_ROOTS = ("cips", "krauksk", "uzkod")
_SWEET_ROOTS = ("konfekt", "baton", "desert", "saldejum", "kuk", "cepum", "pudin")
_SAUCE_ROOTS = ("merce", "kecup", "sinep")
_PROCESSED_MEAT = ("des", "cisin", "pastet", "pelmen", "frikadel", "naget", "konserv")
_DRINK_ROOTS = ("dzerien", "kokteil", "sirup")

_PROFILES: dict[str, _CategoryProfile] = {
    # ── dairy ──
    "milk": _CategoryProfile(penalty_roots=(
        *_PROCESSED_MEAT, *_SNACK_ROOTS, *_SWEET_ROOTS, *_SAUCE_ROOTS,
        "sokolad", "kakao", "iebiezin", "kondenset",
        "cal", "zup", "sier",
    )),
    "yogurt": _CategoryProfile(penalty_roots=(
        *_SNACK_ROOTS, *_SWEET_ROOTS, *_SAUCE_ROOTS,
        "sokolad", "des", "zup",
    )),
    "butter": _CategoryProfile(penalty_roots=(
        "margarin", "ziepe",
    )),
    "cheese": _CategoryProfile(penalty_roots=(
        *_SNACK_ROOTS, *_PROCESSED_MEAT,
        "kukuruz", "longchip", "gars", "bumba", "plaksn", "nujin",
    )),
    # ── eggs ──
    "eggs": _CategoryProfile(penalty_roots=(
        "majonez", *_SWEET_ROOTS,
    )),
    # ── bread ──
    "bread": _CategoryProfile(penalty_roots=(
        "mikl", "margarin",
    )),
    # ── meat / poultry ──
    "chicken": _CategoryProfile(penalty_roots=(
        *_PROCESSED_MEAT, *_SNACK_ROOTS,
        "cepam", "nudel", "gars", "zup",
    )),
    "beef": _CategoryProfile(penalty_roots=(
        *_PROCESSED_MEAT, *_SNACK_ROOTS,
        "zup", "buljons", "gars",
    )),
    "pork": _CategoryProfile(penalty_roots=(
        *_PROCESSED_MEAT, *_SNACK_ROOTS,
        "zup", "buljons", "gars",
    )),
    "minced_meat": _CategoryProfile(penalty_roots=(
        *_SNACK_ROOTS, "zup", "buljons",
    )),
    # ── fish ──
    "fish": _CategoryProfile(penalty_roots=(
        *_SNACK_ROOTS, "ell", "konserv", "zup", "buljons",
    )),
    # ── grains / staples ──
    "rice": _CategoryProfile(penalty_roots=(
        *_PROCESSED_MEAT, *_SNACK_ROOTS,
    )),
    "pasta": _CategoryProfile(penalty_roots=(
        *_SAUCE_ROOTS, "zup",
    )),
    "flour": _CategoryProfile(penalty_roots=(
        *_SWEET_ROOTS, "maiz",
    )),
    # ── fruit ──
    "banana": _CategoryProfile(penalty_roots=(
        *_SWEET_ROOTS, *_SNACK_ROOTS, *_DRINK_ROOTS,
        "sokolad",
    )),
    "apple": _CategoryProfile(penalty_roots=(
        *_SWEET_ROOTS, *_SNACK_ROOTS, *_DRINK_ROOTS,
        "sul", "sokolad",
    )),
    "avocado": _CategoryProfile(penalty_roots=(
        *_SAUCE_ROOTS, "ell", "cips", "salsa",
    )),
    # ── vegetables ──
    "potatoes": _CategoryProfile(penalty_roots=(
        *_SNACK_ROOTS, "milt", "pire",
    )),
    "tomato": _CategoryProfile(penalty_roots=(
        *_SAUCE_ROOTS, "sul", "konserv", "past",
    )),
    "onion": _CategoryProfile(penalty_roots=(
        *_SNACK_ROOTS, *_SAUCE_ROOTS,
    )),
    # ── pantry / condiments ──
    "sugar": _CategoryProfile(penalty_roots=(
        "aizvietotaj", *_DRINK_ROOTS,
    )),
    "salt": _CategoryProfile(penalty_roots=(
        *_SNACK_ROOTS,
    )),
    "oil": _CategoryProfile(penalty_roots=(
        "filtr", "motor",
    )),
    "coffee": _CategoryProfile(penalty_roots=(
        *_SWEET_ROOTS, *_DRINK_ROOTS,
    )),
    "tea": _CategoryProfile(penalty_roots=(
        *_SWEET_ROOTS, *_DRINK_ROOTS,
    )),
    # ── sweets ──
    "chocolate": _CategoryProfile(penalty_roots=(
        *_DRINK_ROOTS, "saldejum",
    )),
    "cookies": _CategoryProfile(penalty_roots=()),
    "ice_cream": _CategoryProfile(penalty_roots=()),
    # ── drinks ──
    "water": _CategoryProfile(penalty_roots=(
        "sul",
    )),
    "juice": _CategoryProfile(penalty_roots=(
        *_SAUCE_ROOTS,
    )),
    # ── non-food ──
    "dish_soap": _CategoryProfile(penalty_roots=(
        "edien", "zup", "pien",
    )),
    "shampoo": _CategoryProfile(penalty_roots=(
        "edien", "zup",
    )),
    "toothpaste": _CategoryProfile(penalty_roots=(
        "makaron",
    )),
    "toilet_paper": _CategoryProfile(penalty_roots=()),
    "laundry_detergent": _CategoryProfile(penalty_roots=(
        "trauk", "edien",
    )),
}

# Stemmed query token → profile key.  Checked explicitly first; intent
# detection is used as fallback (see _get_penalty_profile).
_QUERY_TO_PROFILE: dict[str, str] = {
    # dairy
    "pien": "milk", "piens": "milk", "piena": "milk", "pienu": "milk", "milk": "milk",
    "jogurt": "yogurt", "jogurts": "yogurt", "jogurta": "yogurt", "jogurtu": "yogurt",
    "yogurt": "yogurt", "yoghurt": "yogurt",
    "sviest": "butter", "sviests": "butter", "sviesta": "butter", "sviestu": "butter",
    "butter": "butter",
    "sier": "cheese", "siers": "cheese", "siera": "cheese", "sieru": "cheese",
    "cheese": "cheese",
    # eggs
    "ola": "eggs", "olas": "eggs", "olu": "eggs", "eggs": "eggs", "egg": "eggs",
    # bread
    "maiz": "bread", "maize": "bread", "maizes": "bread", "maizi": "bread", "bread": "bread",
    # meat
    "vist": "chicken", "vista": "chicken", "vistas": "chicken", "chicken": "chicken",
    "liellop": "beef", "beef": "beef",
    "cukas": "pork", "cukgal": "pork", "pork": "pork",
    "malt": "minced_meat", "far": "minced_meat", "minced": "minced_meat",
    # fish
    "zivi": "fish", "ziv": "fish", "fish": "fish", "lasi": "fish", "salmon": "fish",
    # grains
    "ris": "rice", "risi": "rice", "risu": "rice", "rice": "rice",
    "makaron": "pasta", "pasta": "pasta", "spaget": "pasta",
    "milt": "flour", "flour": "flour",
    # fruit
    "banan": "banana", "banana": "banana",
    "abol": "apple", "apple": "apple",
    "avokado": "avocado", "avocado": "avocado",
    "tomat": "tomato", "tomato": "tomato",
    # vegetables
    "kartupel": "potatoes", "potato": "potatoes",
    "sipol": "onion", "onion": "onion",
    # pantry
    "cukur": "sugar", "sugar": "sugar",
    "sal": "salt", "salt": "salt",
    "ell": "oil", "oil": "oil",
    "kafij": "coffee", "coffee": "coffee",
    "tej": "tea", "tea": "tea",
    "med": "honey",
    # sweets
    "sokolad": "chocolate", "sokolade": "chocolate", "chocolate": "chocolate",
    "cepum": "cookies", "cookie": "cookies", "cookies": "cookies",
    "saldejum": "ice_cream",
    # drinks
    "uden": "water", "water": "water",
    "sul": "juice", "juice": "juice",
    # non-food
    "trauk": "dish_soap", "sampun": "shampoo", "shampoo": "shampoo",
    "zobu": "toothpaste",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _any_token_starts_with(tokens: set[str], prefix: str) -> bool:
    return any(t.startswith(prefix) for t in tokens)


def _get_required_roots(query: str) -> list[str] | None:
    """Return required title-token roots for *query*.

    First checks the explicit KEYWORD_FILTER (fast dict lookup), then falls
    back to product-intent detection so that every known product type gets
    precision filtering without manual KEYWORD_FILTER entries.
    """
    for token in tokenize_for_match(query):
        roots = KEYWORD_FILTER.get(token)
        if roots is not None:
            return roots

    # Fallback: derive roots from product-intent primary_roots
    from app.services.product_intent import detect_product_intent, get_intent_config

    intent = detect_product_intent(query)
    if intent:
        config = get_intent_config(intent)
        if config:
            roots = []
            for r in config["primary_roots"]:
                for tok in normalize_text(r).split():
                    stemmed = stem_latvian_token(tok)
                    if stemmed and len(stemmed) >= 3 and stemmed not in roots:
                        roots.append(stemmed)
            if roots:
                return roots
    return None


def _title_passes_filter(candidate_tokens: set[str], roots: list[str]) -> bool:
    """At least one candidate token must start with one of the roots."""
    return any(
        _any_token_starts_with(candidate_tokens, root)
        for root in roots
    )


def _get_penalty_profile(query: str) -> _CategoryProfile | None:
    """Return the penalty profile for *query*.

    Checks the explicit _QUERY_TO_PROFILE mapping first, then falls back to
    product-intent detection so every known category gets penalties applied.
    """
    for token in tokenize_for_match(query):
        profile_name = _QUERY_TO_PROFILE.get(token)
        if profile_name is not None:
            return _PROFILES.get(profile_name)

    from app.services.product_intent import detect_product_intent

    intent = detect_product_intent(query)
    if intent:
        return _PROFILES.get(intent)
    return None


def _is_penalized(candidate_tokens: set[str], profile: _CategoryProfile) -> bool:
    """True if any candidate token starts with a penalty root."""
    return any(
        _any_token_starts_with(candidate_tokens, root)
        for root in profile.penalty_roots
    )


# ---------------------------------------------------------------------------
# General similarity (used by /search)
# ---------------------------------------------------------------------------

def similarity_score(query: str, candidate: str) -> float:
    """Return a 0-1 score indicating how well *candidate* matches *query*.

    Uses normalized (lowercase, strip diacritics) and stemmed tokens so that
    piens/piena and banāns/banāni match. Token-start matching avoids compound
    false positives like "maize" inside "sviestmaizem".
    """
    q_norm = normalize_text(query)
    c_norm = normalize_text(candidate)

    if not q_norm or not c_norm:
        return 0.0

    if q_norm == c_norm:
        return 1.0

    q_tokens = set(tokenize_for_match(query))
    c_tokens = set(tokenize_for_match(candidate))

    # Single-token query that appears as prefix of a title token
    if len(q_tokens) == 1:
        (q_stem,) = q_tokens
        if any(t.startswith(q_stem) for t in c_tokens):
            return 0.9

    token_overlap = len(q_tokens & c_tokens) / len(q_tokens) if q_tokens else 0.0

    q_tri = trigrams(query)
    c_tri = trigrams(candidate)
    tri_union = q_tri | c_tri
    tri_sim = len(q_tri & c_tri) / len(tri_union) if tri_union else 0.0

    return 0.6 * token_overlap + 0.4 * tri_sim


# ---------------------------------------------------------------------------
# Strict basket matching (used by /basket)
# ---------------------------------------------------------------------------

CONFIDENCE_STRONG = "strong"   # >= 0.80, primary product
CONFIDENCE_OK     = "ok"       # >= 0.50, acceptable
CONFIDENCE_WEAK   = "weak"     # passed root filter but is a processed/derivative product
CONFIDENCE_REJECT = "reject"   # failed root filter or below threshold

BASKET_THRESHOLD = 0.50


def match_product(
    query: str,
    candidate: str,
    parsed: ParsedQuery | None = None,
) -> tuple[float, str]:
    """Category-aware matching for basket mode.

    Returns (score, confidence) where confidence is one of the CONFIDENCE_*
    constants.

    When *parsed* is provided the core product terms are used for similarity
    (so numbers/units in the raw query don't dilute the score) and attribute
    bonuses are applied for matching fat-%, volume, or weight.
    """
    if parsed is None:
        parsed = parse_grocery_query(query)

    core = parsed.expanded_core or parsed.core_terms or query

    c_tokens = set(tokenize_for_match(candidate))

    required_roots = _get_required_roots(core)

    if required_roots is not None:
        if not _title_passes_filter(c_tokens, required_roots):
            return 0.0, CONFIDENCE_REJECT

        score = similarity_score(core, candidate)
        score = max(score, 0.50)
        score += attribute_boost(parsed, candidate)
        score = min(score, 1.0)

        profile = _get_penalty_profile(core)
        if profile is not None and _is_penalized(c_tokens, profile):
            return score, CONFIDENCE_WEAK

        return score, _confidence(score)

    score = similarity_score(core, candidate)
    score += attribute_boost(parsed, candidate)
    score = min(score, 1.0)
    return score, _confidence(score)


def _confidence(score: float) -> str:
    if score >= 0.80:
        return CONFIDENCE_STRONG
    if score >= BASKET_THRESHOLD:
        return CONFIDENCE_OK
    return CONFIDENCE_REJECT
