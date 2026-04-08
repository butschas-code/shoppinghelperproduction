"""Parse structured attributes from free-text grocery queries.

Separates the product keyword(s) from size, volume, fat-%, and unit info
so the matching engine can score on the *product* keyword only (no dilution
from numbers / units) and then boost candidates whose attributes match.

Examples
--------
  "1 liter milk 2.0% fat"  → core="milk",  volume_ml=1000, fat_pct=2.0
  "piens 2.5% 1l"          → core="piens", volume_ml=1000, fat_pct=2.5
  "vistas fileja 1kg"      → core="vistas fileja", weight_g=1000
  "10 eggs"                → core="eggs",  count=10
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from app.services.normalize import normalize_text
from app.services.search_synonyms import expand_query_for_search

# ── regex ───────────────────────────────────────────────────────────

_FAT_RE = re.compile(
    r"(\d+[.,]?\d*)\s*%\s*(?:fat|tauk(?:u|vielu)?)?",
    re.IGNORECASE,
)

_SIZE_RE = re.compile(
    r"(\d+[.,]?\d*)\s*"
    r"(ml|l|liter|liters?|litres?|g|kg|grams?|gab|pcs|pieces?)\b",
    re.IGNORECASE,
)

_NOISE_WORDS = frozenset({
    "fat", "tauku", "taukvielu", "tauk",
    "of", "the", "a", "an", "with", "and", "ar",
    "liter", "liters", "litre", "litres",
    "ml", "l", "g", "kg", "gram", "grams",
    "gab", "pcs", "pieces", "piece",
})


def _num(s: str) -> float:
    return float(s.replace(",", "."))


# ── dataclass ───────────────────────────────────────────────────────

@dataclass(frozen=True)
class ParsedQuery:
    raw: str
    core_terms: str
    expanded_core: str
    volume_ml: float | None = None
    weight_g: float | None = None
    fat_pct: float | None = None
    count: int | None = None


# ── parse ───────────────────────────────────────────────────────────

def parse_grocery_query(query: str) -> ParsedQuery:
    """Parse free-text grocery query → structured attributes + core product keywords."""
    raw = (query or "").strip()
    if not raw:
        return ParsedQuery(raw="", core_terms="", expanded_core="")

    fat_pct: float | None = None
    m = _FAT_RE.search(raw)
    if m:
        fat_pct = _num(m.group(1))

    volume_ml: float | None = None
    weight_g: float | None = None
    count: int | None = None

    for val_s, unit in _SIZE_RE.findall(raw):
        u = unit.lower()
        v = _num(val_s)
        if u in ("l", "liter", "liters", "litre", "litres"):
            volume_ml = v * 1000
        elif u == "ml":
            volume_ml = v
        elif u == "kg":
            weight_g = v * 1000
        elif u in ("g", "gram", "grams"):
            weight_g = v
        elif u in ("gab", "pcs", "piece", "pieces"):
            count = int(v)

    cleaned = _FAT_RE.sub(" ", raw)
    cleaned = _SIZE_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\b\d+[.,]?\d*\b", " ", cleaned)
    cleaned = cleaned.replace("%", " ")

    norm = normalize_text(cleaned)
    tokens = [t for t in norm.split() if t not in _NOISE_WORDS and len(t) > 1]
    core = " ".join(tokens) if tokens else norm.strip()

    expanded = expand_query_for_search(core) if core else core

    return ParsedQuery(
        raw=raw,
        core_terms=core,
        expanded_core=expanded,
        volume_ml=volume_ml,
        weight_g=weight_g,
        fat_pct=fat_pct,
        count=count,
    )


# ── attribute extraction from product titles ────────────────────────

_TITLE_FAT_RE = re.compile(r"(\d+[.,]?\d*)\s*%")
_TITLE_VOL_RE = re.compile(r"(\d+[.,]?\d*)\s*(ml|l)\b", re.IGNORECASE)
_TITLE_WT_RE = re.compile(r"(\d+[.,]?\d*)\s*(g|kg)\b", re.IGNORECASE)


# Typical milk / cream fat % in titles (avoid matching "12%" nutrition noise).
_DAIRY_FAT_PCT_RANGE = (0.15, 7.5)


def extract_title_fat_pct(title: str) -> float | None:
    """First % in *dairy fat range* in the title, else first % overall."""
    in_range: list[float] = []
    for m in _TITLE_FAT_RE.finditer(title):
        v = _num(m.group(1))
        if _DAIRY_FAT_PCT_RANGE[0] <= v <= _DAIRY_FAT_PCT_RANGE[1]:
            in_range.append(v)
    if in_range:
        return in_range[0]
    m = _TITLE_FAT_RE.search(title)
    return _num(m.group(1)) if m else None


def extract_title_volume_ml(title: str) -> float | None:
    """Largest liquid volume in the title (pack size), in ml."""
    best: float | None = None
    for val_s, unit in _TITLE_VOL_RE.findall(title):
        v = _num(val_s)
        ml = v * 1000 if unit.lower() == "l" else v
        if best is None or ml > best:
            best = ml
    return best


def extract_title_weight_g(title: str) -> float | None:
    for val_s, unit in _TITLE_WT_RE.findall(title):
        v = _num(val_s)
        return v * 1000 if unit.lower() == "kg" else v
    return None


# ── strict attribute filter (intent search) ─────────────────────────
#
# When the user query contains fat %, volume, or weight, *every* candidate
# must demonstrably match those numbers.  Applies to ALL product intents
# (milk, yogurt, chicken, cheese, …) — not just milk.

_FAT_MATCH_TOLERANCE = 0.45   # 2.0% matches 1.55–2.45%
_SIZE_RATIO_MIN      = 0.85   # 1 L matches down to ~850 ml

# Extra normalized substrings that mark a "milk" title as *not* plain
# drinking milk.  Supplements the intent's own exclude_roots.
_MILK_DERIVATIVE_SUBS: frozenset[str] = frozenset({
    "dzerien",      # piens dzēriens / milk drink
    "zemen",        # strawberry
    "sokolad",      # chocolate
    "sokolade",
    "grik",         # buckwheat etc.
    "biezpien",     # cottage cheese
    "kefir",
    "jogurt",
    "jogurts",
    "monte",        # dessert brand
    "rasen",        # Rasēns flavored line
    "vanilla",
    "kakao",
    "karamel",
    "banan",
    "pudin",
    "desert",
})


def passes_attribute_constraints(
    pq: ParsedQuery,
    title: str,
    intent_key: str | None,
    *,
    size_text: str | None = None,
    exclude_roots: list[str] | None = None,
) -> bool:
    """Hard-gate: when *pq* specifies fat %, volume, or weight the product
    must match within tolerance **or be rejected**.

    Works for every intent — milk, yogurt, chicken, cheese, etc.

    Parameters
    ----------
    size_text : scraped size field (often contains volume / weight even when
        the title does not).
    exclude_roots : pre-normalised exclude roots from the intent config;
        treated as a **hard** reject (not just a scoring penalty) when
        attributes are present.
    """
    has_fat = pq.fat_pct is not None
    has_vol = pq.volume_ml is not None
    has_wt  = pq.weight_g is not None

    if not (has_fat or has_vol or has_wt):
        return True

    raw = title or ""
    tn  = normalize_text(raw)

    combined = f"{raw} {size_text}" if size_text else raw

    # ── 1. Hard reject on intent exclude_roots ──────────────────────
    if exclude_roots:
        for root in exclude_roots:
            if root and root in tn:
                return False

    # ── 2. Milk-specific derivative filter (supplements exclude_roots)
    if intent_key == "milk":
        for sub in _MILK_DERIVATIVE_SUBS:
            if sub in tn:
                return False

    # ── 3. Fat % — must be present and close ────────────────────────
    if has_fat:
        tf = extract_title_fat_pct(raw)
        if tf is None:
            return False
        if abs(pq.fat_pct - tf) > _FAT_MATCH_TOLERANCE:
            return False

    # ── 4. Volume — must be verifiable and within ratio ─────────────
    if has_vol:
        tv = extract_title_volume_ml(combined)
        if tv is None:
            return False
        hi, lo = max(pq.volume_ml, tv), min(pq.volume_ml, tv)
        ratio = lo / hi if hi else 0.0
        if ratio < _SIZE_RATIO_MIN:
            return False

    # ── 5. Weight — must be verifiable and within ratio ─────────────
    if has_wt:
        tw = extract_title_weight_g(combined)
        if tw is None:
            return False
        hi, lo = max(pq.weight_g, tw), min(pq.weight_g, tw)
        ratio = lo / hi if hi else 0.0
        if ratio < _SIZE_RATIO_MIN:
            return False

    return True


def attribute_boost(pq: ParsedQuery, title: str) -> float:
    """Return bonus (positive) or penalty (negative) for attribute match quality."""
    bonus = 0.0

    if pq.fat_pct is not None:
        title_fat = extract_title_fat_pct(title)
        if title_fat is not None:
            diff = abs(pq.fat_pct - title_fat)
            if diff < 0.05:
                bonus += 0.20
            elif diff <= 0.5:
                bonus += 0.10
            elif diff <= 1.5:
                bonus -= 0.03
            else:
                bonus -= 0.08

    if pq.volume_ml is not None:
        title_vol = extract_title_volume_ml(title)
        if title_vol is not None:
            hi, lo = max(pq.volume_ml, title_vol), min(pq.volume_ml, title_vol)
            ratio = lo / hi if hi else 0
            if ratio >= 0.95:
                bonus += 0.10
            elif ratio >= 0.5:
                bonus += 0.03

    if pq.weight_g is not None:
        title_wt = extract_title_weight_g(title)
        if title_wt is not None:
            hi, lo = max(pq.weight_g, title_wt), min(pq.weight_g, title_wt)
            ratio = lo / hi if hi else 0
            if ratio >= 0.95:
                bonus += 0.10
            elif ratio >= 0.5:
                bonus += 0.03

    return bonus
