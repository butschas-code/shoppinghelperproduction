"""Search & basket-pricing logic."""

from __future__ import annotations

from datetime import timezone
from zoneinfo import ZoneInfo

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.retailer_meta import get_retailer_info
from app.db.models import ProductOffer, Retailer
from app.schemas.dto import BasketItem, BasketResult, SearchResult
from app.services.household import household_score
from app.services.match import (
    BASKET_THRESHOLD,
    CONFIDENCE_REJECT,
    CONFIDENCE_WEAK,
    match_product,
    similarity_score,
)
from app.services.normalize import tokenize
from app.services.query_parser import parse_grocery_query
from app.services.search_synonyms import expand_query_for_search

SEARCH_THRESHOLD = 0.25


def _get_latest_offers(db: Session) -> list[ProductOffer]:
    """Return all offers from the most recent scrape per retailer."""
    latest_sub = (
        db.query(
            ProductOffer.retailer_id,
            func.max(ProductOffer.scraped_at).label("latest"),
        )
        .group_by(ProductOffer.retailer_id)
        .subquery()
    )

    return (
        db.query(ProductOffer)
        .join(
            latest_sub,
            (ProductOffer.retailer_id == latest_sub.c.retailer_id)
            & (ProductOffer.scraped_at == latest_sub.c.latest),
        )
        .all()
    )


def _retailer_map(db: Session) -> dict[str, Retailer]:
    return {r.id: r for r in db.query(Retailer).all()}


def get_last_updated(db: Session) -> dict[str, str]:
    """Return {retailer_id: last scraped_at} for display purposes."""
    rows = (
        db.query(
            ProductOffer.retailer_id,
            func.max(ProductOffer.scraped_at),
        )
        .group_by(ProductOffer.retailer_id)
        .all()
    )
    return {rid: ts.strftime("%Y-%m-%d %H:%M") if ts else "never" for rid, ts in rows}


def get_latest_pricing_update_label(db: Session, lang: str) -> str | None:
    """Single line for UI: latest `scraped_at` in DB (Europe/Riga). None if empty."""
    from app.core.i18n import t

    ts = db.query(func.max(ProductOffer.scraped_at)).scalar()
    if not ts:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    local = ts.astimezone(ZoneInfo("Europe/Riga"))
    when = local.strftime("%Y-%m-%d %H:%M")
    return t("footer.last_prices_update", lang, when=when)


# ------------------------------------------------------------------
# Product search
# ------------------------------------------------------------------

def search_products(db: Session, query: str, limit: int = 60) -> list[SearchResult]:
    expanded = expand_query_for_search(query)
    offers = _get_latest_offers(db)
    retailers = _retailer_map(db)

    scored: list[SearchResult] = []
    for offer in offers:
        score = similarity_score(expanded, offer.title)
        if score < SEARCH_THRESHOLD:
            continue
        r = retailers.get(offer.retailer_id)
        rmeta = get_retailer_info(offer.retailer_id)
        scored.append(
            SearchResult(
                retailer_id=offer.retailer_id,
                retailer_name=r.name if r else offer.retailer_id,
                title=offer.title,
                price=offer.price,
                unit_price=offer.unit_price,
                unit=offer.unit,
                size_text=offer.size_text,
                url=offer.url,
                scraped_at=offer.scraped_at,
                similarity=score,
                catalog_type=rmeta.catalog_type.value,
                catalog_description=rmeta.description,
            )
        )

    scored.sort(key=lambda x: (-x.similarity, x.price))
    return scored[:limit]


# ------------------------------------------------------------------
# Basket comparison
# ------------------------------------------------------------------

def compute_basket(db: Session, items: list[str]) -> list[BasketResult]:
    offers = _get_latest_offers(db)
    retailers = _retailer_map(db)

    offers_by_retailer: dict[str, list[ProductOffer]] = {}
    for o in offers:
        offers_by_retailer.setdefault(o.retailer_id, []).append(o)

    results: list[BasketResult] = []
    for retailer_id, r_offers in offers_by_retailer.items():
        r = retailers.get(retailer_id)
        rmeta = get_retailer_info(retailer_id)
        result = BasketResult(
            retailer_id=retailer_id,
            retailer_name=r.name if r else retailer_id,
            catalog_type=rmeta.catalog_type.value,
            catalog_description=rmeta.description,
            basket_eligible=rmeta.basket_eligible,
        )

        for item_query in items:
            pq = parse_grocery_query(item_query)
            core_for_match = pq.expanded_core or pq.core_terms or item_query
            primary: list[tuple[ProductOffer, float, float, str]] = []
            weak: list[tuple[ProductOffer, float, float, str]] = []

            single_word_core = len(tokenize(pq.core_terms)) <= 1

            for offer in r_offers:
                score, confidence = match_product(
                    core_for_match, offer.title, parsed=pq,
                )
                if confidence == CONFIDENCE_REJECT:
                    continue
                if score < BASKET_THRESHOLD:
                    continue

                if single_word_core:
                    h_score = household_score(core_for_match, offer.title)
                    rank_score = score * (0.6 + 0.4 * h_score)
                else:
                    rank_score = score

                entry = (offer, rank_score, score, confidence)
                if confidence == CONFIDENCE_WEAK:
                    weak.append(entry)
                else:
                    primary.append(entry)

            # Two-pass: prefer primary (real/fresh) products.
            # If only processed/derivative matches exist, reject entirely.
            chosen = primary or None
            if chosen:
                best_offer, _, best_score, best_confidence = max(
                    chosen, key=lambda x: x[1],
                )
                result.items.append(
                    BasketItem(
                        query=item_query,
                        title=best_offer.title,
                        price=best_offer.price,
                        score=round(best_score, 2),
                        confidence=best_confidence,
                    )
                )
                result.total += best_offer.price
                result.found_count += 1
            else:
                result.missing.append(item_query)

        result.total = round(result.total, 2)
        results.append(result)

    # Eligible retailers first (for basket total comparison), then by found/total
    results.sort(key=lambda r: (not r.basket_eligible, -r.found_count, r.total))
    return results
