"""Intelligent grocery search: product intent → scoring → group by retailer.

1. Detect product intent from query (product_intent).
2. If intent: score all offers (+100 start primary, +60 first word, +30 contains, -80 exclude, -40 category mismatch).
3. Sort by score DESC, price ASC. Group by retailer, take best 3 per retailer.
4. Return structured result (comparison + top per store + full_results) or fallback flat hits.
5. Cache latest offers 60s (no full DB on every keystroke).
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import ProductOffer, Retailer
from app.services.normalize import normalize_text, tokenize_for_match, trigrams
from app.services.query_parser import parse_grocery_query
from app.services.search_synonyms import expand_query_for_search
from app.services.product_intent import (
    detect_product_intent,
    get_intent_config,
)
from app.services.intent_category_map import (
    get_allowed_categories,
    offer_matches_category,
)
from app.services.product_type_detector import RELATED_PRODUCT_TYPES
from app.services.product_classifier import detect_product_type_from_title

LIMIT_PER_RETAILER = 50
TOP_PER_RETAILER = 3
SEARCH_THRESHOLD = 0.08
OFFERS_CACHE_TTL = 60.0

# Ranking scores
SCORE_TITLE_STARTS_PRIMARY = 100
SCORE_FIRST_WORD_PRIMARY = 60
SCORE_CONTAINS_PRIMARY = 30
SCORE_CONTAINS_EXCLUDE = -80
SCORE_CATEGORY_MISMATCH = -40

_offers_cache: list[ProductOffer] = []
_offers_cache_time: float = 0


@dataclass
class ProductSearchHit:
    product_id: int
    retailer_id: str
    retailer_name: str
    title: str
    price: float
    unit_price: float | None
    unit: str | None
    size: str | None
    image_url: str | None
    relevance: float
    url: str = ""
    product_type: str | None = None


@dataclass
class StructuredSearchResult:
    """Shopping-assistant result: comparison block + best per store + full list."""
    product_type: str
    cheapest: dict[str, Any]  # retailer_id, retailer_name, best_price, best_title, best_offer
    retailers: list[dict[str, Any]]  # retailer_id, retailer_name, best_price, best_title, best_offer, top_items
    full_results: list[ProductSearchHit]
    total_items: int = 0

    def __post_init__(self) -> None:
        if self.total_items == 0 and self.full_results:
            self.total_items = len(self.full_results)


def _get_latest_offers(db: Session) -> list[ProductOffer]:
    """Latest offers per retailer. Cached 60s."""
    global _offers_cache, _offers_cache_time
    now = time.monotonic()
    if _offers_cache and (now - _offers_cache_time) < OFFERS_CACHE_TTL:
        return _offers_cache
    latest_sub = (
        db.query(
            ProductOffer.retailer_id,
            func.max(ProductOffer.scraped_at).label("latest"),
        )
        .group_by(ProductOffer.retailer_id)
        .subquery()
    )
    result = (
        db.query(ProductOffer)
        .join(
            latest_sub,
            (ProductOffer.retailer_id == latest_sub.c.retailer_id)
            & (ProductOffer.scraped_at == latest_sub.c.latest),
        )
        .all()
    )
    _offers_cache = result
    _offers_cache_time = time.monotonic()
    return result


def _retailer_map(db: Session) -> dict[str, Retailer]:
    return {r.id: r for r in db.query(Retailer).all()}


def _searchable_text(offer: ProductOffer) -> str:
    parts = [offer.title]
    if offer.brand:
        parts.append(offer.brand)
    if offer.size_text:
        parts.append(offer.size_text)
    return " ".join(parts)


def _build_hit(
    offer: ProductOffer,
    retailers: dict[str, Retailer],
    relevance: float = 1.0,
    product_type: str | None = None,
) -> ProductSearchHit:
    r = retailers.get(offer.retailer_id)
    rname = r.name if r else offer.retailer_id
    return ProductSearchHit(
        product_id=offer.id,
        retailer_id=offer.retailer_id,
        retailer_name=rname,
        title=offer.title,
        price=offer.price,
        unit_price=offer.unit_price,
        unit=offer.unit,
        size=offer.size_text,
        image_url=None,
        relevance=round(relevance, 4),
        url=offer.url or "",
        product_type=product_type or offer.product_type,
    )


def _score_offer(
    offer: ProductOffer,
    intent_key: str,
    title_norm: str,
) -> int:
    """Score a single offer for the given intent. Higher = better match."""
    config = get_intent_config(intent_key)
    if not config:
        return 0
    primary = [normalize_text(r) for r in config["primary_roots"]]
    exclude = [normalize_text(r) for r in config["exclude_roots"]]
    score = 0

    first_word = title_norm.split()[0] if title_norm.split() else ""

    for root in primary:
        if not root:
            continue
        if title_norm.startswith(root + " ") or title_norm.startswith(root):
            score += SCORE_TITLE_STARTS_PRIMARY
            break
        if first_word.startswith(root) or root in first_word:
            score += SCORE_FIRST_WORD_PRIMARY
            break
    if score == 0:
        for root in primary:
            if root and re.search(r"\b" + re.escape(root) + r"\b", title_norm):
                score += SCORE_CONTAINS_PRIMARY
                break

    for root in exclude:
        if root and re.search(r"\b" + re.escape(root) + r"\b", title_norm):
            score += SCORE_CONTAINS_EXCLUDE
            break

    classified = detect_product_type_from_title(offer.title)
    if classified and classified != intent_key:
        related = set(RELATED_PRODUCT_TYPES.get(intent_key, []))
        if classified not in related:
            score += SCORE_CATEGORY_MISMATCH

    return score


def _intent_search(
    db: Session,
    query: str,
    retailers: dict[str, Retailer],
    offers: list[ProductOffer],
) -> StructuredSearchResult | None:
    """When product intent is detected: filter by category first, then score, then group."""
    intent_key = detect_product_intent(query)
    if not intent_key:
        return None

    allowed = get_allowed_categories(intent_key)
    if allowed:
        offers = [
            o for o in offers
            if offer_matches_category(o.category_path, o.category_root, allowed)
        ]
        if not offers:
            return None

    scored: list[tuple[ProductOffer, int]] = []
    for offer in offers:
        title_norm = normalize_text(offer.title)
        score = _score_offer(offer, intent_key, title_norm)
        if score < 0:
            continue
        scored.append((offer, score))

    if not scored:
        return None

    scored.sort(key=lambda x: (-x[1], x[0].price))
    by_retailer: dict[str, list[tuple[ProductOffer, int]]] = {}
    for offer, s in scored:
        rid = offer.retailer_id
        if rid not in by_retailer:
            by_retailer[rid] = []
        if len(by_retailer[rid]) < LIMIT_PER_RETAILER:
            by_retailer[rid].append((offer, s))

    retailers_list: list[dict[str, Any]] = []
    cheapest_rid: str | None = None
    cheapest_price: float | None = None
    cheapest_name: str | None = None
    full_hits: list[ProductSearchHit] = []

    for rid in sorted(by_retailer.keys()):
        items = by_retailer[rid]
        items_sorted = sorted(items, key=lambda x: (-x[1], x[0].price))
        top_items = items_sorted[:TOP_PER_RETAILER]
        meta = retailers.get(rid)
        rname = meta.name if meta else rid
        best_offer = top_items[0][0]
        best_price = best_offer.price
        best_title = best_offer.title

        top_hits = [
            _build_hit(offer, retailers, relevance=float(s), product_type=intent_key)
            for offer, s in top_items
        ]
        for offer, s in items_sorted:
            full_hits.append(_build_hit(offer, retailers, relevance=float(s), product_type=intent_key))

        retailers_list.append({
            "retailer_id": rid,
            "retailer_name": rname,
            "best_price": best_price,
            "best_title": best_title,
            "best_offer": top_hits[0] if top_hits else _build_hit(best_offer, retailers, product_type=intent_key),
            "top_items": top_hits,
            "all_items": [_build_hit(offer, retailers, relevance=float(s), product_type=intent_key) for offer, s in items_sorted],
        })
        if cheapest_price is None or best_price < cheapest_price:
            cheapest_price = best_price
            cheapest_rid = rid
            cheapest_name = rname

    if not retailers_list or cheapest_rid is None:
        return None

    cheapest_entry = retailers_list[0]
    for r in retailers_list:
        if r["retailer_id"] == cheapest_rid:
            cheapest_entry = r
            break
    cheapest = {
        "retailer_id": cheapest_rid,
        "retailer_name": cheapest_name,
        "best_price": cheapest_price,
        "best_title": cheapest_entry["best_title"],
        "best_offer": cheapest_entry["best_offer"],
    }

    return StructuredSearchResult(
        product_type=intent_key,
        cheapest=cheapest,
        retailers=retailers_list,
        full_results=full_hits,
        total_items=len(full_hits),
    )


def _fuzzy_score(query_expanded: str, offer: ProductOffer) -> float:
    """Score 0–1 for fuzzy matching (fallback when no intent)."""
    text = _searchable_text(offer)
    q_norm = normalize_text(query_expanded)
    c_norm = normalize_text(text)
    if not q_norm or not c_norm:
        return 0.0
    if q_norm in c_norm or c_norm in q_norm:
        return 0.95
    q_tokens = set(tokenize_for_match(query_expanded))
    c_tokens = set(tokenize_for_match(text))
    if not q_tokens:
        return 0.0
    overlap = len(q_tokens & c_tokens) / len(q_tokens)
    if overlap >= 1.0:
        return 0.9
    q_tri = trigrams(query_expanded)
    c_tri = trigrams(text)
    tri_union = q_tri | c_tri
    tri_sim = len(q_tri & c_tri) / len(tri_union) if tri_union else 0.0
    score = overlap * 0.6 + tri_sim * 0.4
    return min(score, 1.0)


def search_products_multi(
    db: Session,
    query: str,
    limit_per_retailer: int = LIMIT_PER_RETAILER,
    include_related: bool = False,
) -> tuple[list[ProductSearchHit], str | None, StructuredSearchResult | None]:
    """Intelligent grocery search.

    - If product intent detected: score offers, group by retailer, return structured result.
    - Else: fuzzy search, return flat hits and optional fallback message.

    Returns: (flat_hits, fallback_message, structured_result).
    When structured_result is not None, use it for comparison UI; flat_hits still populated for compatibility.
    """
    q = (query or "").strip()
    if not q:
        return [], None, None

    retailers = _retailer_map(db)
    offers = _get_latest_offers(db)
    fallback_message: str | None = None

    structured = _intent_search(db, q, retailers, offers)
    if structured is not None:
        flat = structured.full_results
        return flat, None, structured

    fallback_message = "fallback"
    pq = parse_grocery_query(q)
    search_text = pq.expanded_core or expand_query_for_search(q)
    scored: list[tuple[ProductOffer, float]] = []
    for offer in offers:
        s = _fuzzy_score(search_text, offer)
        if s < SEARCH_THRESHOLD:
            continue
        scored.append((offer, s))

    scored.sort(key=lambda x: (-x[1], x[0].price))
    by_retailer: dict[str, list[tuple[ProductOffer, float]]] = {}
    for offer, s in scored:
        rid = offer.retailer_id
        if rid not in by_retailer:
            by_retailer[rid] = []
        if len(by_retailer[rid]) < limit_per_retailer:
            by_retailer[rid].append((offer, s))

    out: list[ProductSearchHit] = []
    for rid in sorted(by_retailer.keys()):
        for offer, relevance in sorted(by_retailer[rid], key=lambda p: (-p[1], p[0].price)):
            out.append(_build_hit(offer, retailers, relevance=relevance))
    out.sort(key=lambda x: (-x.relevance, x.price))
    return out, fallback_message, None
