"""FastAPI application – serves the API and Jinja2 UI."""

from __future__ import annotations

import pathlib
from urllib.parse import urlencode

from fastapi import Depends, FastAPI, Form, Path, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core import config
from app.core.admin_auth import require_admin
from app.core.retailer_meta import get_all_retailer_info, get_retailer_info
from app.core.i18n import SUPPORTED_LANGS, DEFAULT_LANG, t as t_func
from app.db.migrate import create_tables
from app.db.models import ProductOffer
from app.db.session import get_db
from app.services.basket_index import (
    get_cheapest_retailer_of_day,
    get_price_index_history,
    get_today_basket_for_home,
)
from app.services.history import get_price_history, get_price_stats
from app.services.ingest import run_full_ingest
from app.services.ingest_display_time import get_hero_refresh_display
from app.services.newsletter import confirm_subscriber, get_newsletter_admin_stats, subscribe as newsletter_subscribe
from app.services.pricing import (
    compute_basket,
    get_last_updated,
    get_latest_pricing_update_label,
    search_products,
)
from app.services.product_search import search_products_multi
from app.services.search_synonyms import get_search_suggestions
from app.services.search_intent import classify_query
from app.services.product_type_detector import detect_product_type as detect_query_product_type
from app.services.product_type_detector import get_product_type_display

BASE_DIR = pathlib.Path(__file__).resolve().parent


class NewsletterSubscribeBody(BaseModel):
    email: str
    source: str = "homepage"  # homepage | basket | popup
    preferences: dict[str, bool] | None = None  # weekly_report, price_alerts, big_price_drops


app = FastAPI(title="CartWise", version="0.1.0", debug=config.APP_DEBUG)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "web" / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "web" / "templates"))


def get_lang_from_request(request: Request) -> str:
    """Language from URL path (/lv/..., /en/...), else cookie or query, else default."""
    path = request.url.path
    if path.startswith("/lv/") or path == "/lv":
        return "lv"
    if path.startswith("/en/") or path == "/en":
        return "en"
    lang = request.query_params.get("lang") or request.cookies.get("lang") or DEFAULT_LANG
    return lang if lang in SUPPORTED_LANGS else DEFAULT_LANG


def path_without_lang(path: str) -> str:
    """Strip /lv or /en prefix from path. Used for alternates and lang switch."""
    path = path.rstrip("/") or "/"
    if path.startswith("/lv"):
        return path[3:] or "/"
    if path.startswith("/en"):
        return path[3:] or "/"
    return path


def template_context(
    request: Request,
    lang: str | None = None,
    path_without_lang_override: str | None = None,
    **kwargs: object,
) -> dict:
    """Base context for all templates: request, lang, t, lang_prefix, hreflang, etc."""
    if lang is None:
        lang = get_lang_from_request(request)
    lang_prefix = "/" + lang
    pwl = path_without_lang_override if path_without_lang_override is not None else path_without_lang(request.url.path)
    query = request.url.query
    query_string = ("?" + query) if query else ""

    base = config.BASE_URL
    path_part = pwl if pwl != "/" else "/"
    url_lv = base + "/lv" + path_part + query_string
    url_en = base + "/en" + path_part + query_string

    def t(key: str, **kw: object) -> str:
        return t_func(key, lang, **{k: v for k, v in kw.items() if v is not None and isinstance(v, (str, int, float))})

    kwargs.setdefault("nav_active", None)
    return {
        "request": request,
        "lang": lang,
        "t": t,
        "lang_prefix": lang_prefix,
        "path_without_lang": pwl,
        "query_string": query_string,
        "base_url": base,
        "hreflang_lv_url": url_lv,
        "hreflang_en_url": url_en,
        **kwargs,
    }


@app.on_event("startup")
def _startup() -> None:
    create_tables()


# ------------------------------------------------------------------
# Language (cookie + redirect; does not change URL routing)
# ------------------------------------------------------------------

@app.get("/set-lang")
def set_lang(
    lang: str = Query("lv"),
    next_url: str = Query("/", alias="next"),
) -> RedirectResponse:
    """Set language cookie and redirect to language-specific URL."""
    if lang not in SUPPORTED_LANGS:
        lang = DEFAULT_LANG
    # Redirect to /lv/... or /en/... if next is root or a path
    if next_url == "/" or not next_url.startswith("/"):
        next_url = "/"
    if next_url == "/":
        target = f"/{lang}/"
    else:
        target = f"/{lang}{next_url}" if next_url.startswith("/") else f"/{lang}/"
    return RedirectResponse(url=target, status_code=303)


# ------------------------------------------------------------------
# Root redirect → default language
# ------------------------------------------------------------------

@app.get("/")
def root_redirect() -> RedirectResponse:
    """Redirect root to default language home."""
    return RedirectResponse(url="/lv/", status_code=302)


# ------------------------------------------------------------------
# Health
# ------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


# ------------------------------------------------------------------
# UI routes (language-prefixed: /lv/..., /en/...)
# ------------------------------------------------------------------

@app.get("/{lang}/", response_class=HTMLResponse)
def home(
    request: Request,
    lang: str = Path(..., regex="^(lv|en)$"),
    db: Session = Depends(get_db),
    newsletter: str | None = Query(None),
) -> HTMLResponse:
    from sqlalchemy import func

    updated = get_last_updated(db)
    all_meta = {info.retailer_id: info for info in get_all_retailer_info()}

    # Hero: today's basket totals and cheapest
    today_totals_raw, cheapest_pair, savings = get_today_basket_for_home(db)
    today_totals = [
        (rid, all_meta.get(rid, get_retailer_info(rid)).display_name, total)
        for rid, _, total in today_totals_raw
    ]
    cheapest_retailer_id = cheapest_pair[0] if cheapest_pair else None
    cheapest_retailer_name = (
        all_meta.get(cheapest_retailer_id, get_retailer_info(cheapest_retailer_id)).display_name
        if cheapest_retailer_id else None
    )
    cheapest_total = cheapest_pair[1] if cheapest_pair else None

    # Total product count (latest scrape only)
    latest_sub = (
        db.query(
            ProductOffer.retailer_id,
            func.max(ProductOffer.scraped_at).label("latest"),
        )
        .group_by(ProductOffer.retailer_id)
        .subquery()
    )
    total_products = (
        db.query(func.count(ProductOffer.id))
        .join(
            latest_sub,
            (ProductOffer.retailer_id == latest_sub.c.retailer_id)
            & (ProductOffer.scraped_at == latest_sub.c.latest),
        )
        .scalar()
        or 0
    )

    # Hero: last ingest time — GitHub Actions daily-ingest when configured, else latest DB scrape (Riga)
    updated_today_label, updated_date_is_today, _ingest_time_source = get_hero_refresh_display(db)

    return templates.TemplateResponse(
        "index.html",
        template_context(
            request,
            lang=lang,
            path_without_lang_override="/",
            latest_pricing_update=get_latest_pricing_update_label(db, lang),
            nav_active="compare",
            last_updated=updated,
            retailer_meta=all_meta,
            today_basket_totals=today_totals,
            cheapest_retailer_id=cheapest_retailer_id,
            cheapest_retailer_name=cheapest_retailer_name,
            cheapest_total=cheapest_total,
            savings=savings,
            total_products=total_products,
            updated_today_label=updated_today_label,
            updated_date_is_today=updated_date_is_today,
            newsletter_flash=newsletter,
        ),
    )


@app.post("/{lang}/newsletter")
def newsletter_signup(
    lang: str = Path(..., regex="^(lv|en)$"),
    db: Session = Depends(get_db),
    email: str = Form(..., alias="newsletter_email"),
    weekly_report: str | None = Form(None),
    price_alerts: str | None = Form(None),
    big_price_drops: str | None = Form(None),
):
    """Form POST from homepage newsletter section. Stores to newsletter_subscribers, redirects with flash."""
    preferences = {
        "weekly_report": weekly_report is not None,
        "price_alerts": price_alerts is not None,
        "big_price_drops": big_price_drops is not None,
    }
    result = newsletter_subscribe(db, email.strip(), "homepage", preferences)
    status = result.get("status", "error")
    if status == "subscribed":
        return RedirectResponse(url=f"/{lang}/?newsletter=subscribed", status_code=303)
    if status == "already_subscribed":
        return RedirectResponse(url=f"/{lang}/?newsletter=already", status_code=303)
    return RedirectResponse(url=f"/{lang}/?newsletter=error", status_code=303)


@app.get("/{lang}/compare")
def compare_redirect(lang: str = Path(..., regex="^(lv|en)$")) -> RedirectResponse:
    """Compare is the home experience; keep URL for nav parity."""
    return RedirectResponse(url=f"/{lang}/", status_code=302)


@app.get("/{lang}/history", response_class=HTMLResponse)
def history_page(
    request: Request,
    lang: str = Path(..., regex="^(lv|en)$"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return templates.TemplateResponse(
        "history.html",
        template_context(
            request,
            lang=lang,
            path_without_lang_override="/history",
            nav_active="history",
            latest_pricing_update=get_latest_pricing_update_label(db, lang),
        ),
    )


@app.get("/{lang}/alerts", response_class=HTMLResponse)
def alerts_page(
    request: Request,
    lang: str = Path(..., regex="^(lv|en)$"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return templates.TemplateResponse(
        "alerts.html",
        template_context(
            request,
            lang=lang,
            path_without_lang_override="/alerts",
            nav_active="alerts",
            latest_pricing_update=get_latest_pricing_update_label(db, lang),
        ),
    )


@app.get("/{lang}/settings", response_class=HTMLResponse)
def settings_page(
    request: Request,
    lang: str = Path(..., regex="^(lv|en)$"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    return templates.TemplateResponse(
        "settings.html",
        template_context(
            request,
            lang=lang,
            path_without_lang_override="/settings",
            nav_active="settings",
            latest_pricing_update=get_latest_pricing_update_label(db, lang),
        ),
    )


@app.get("/{lang}/newsletter/confirm")
def newsletter_confirm(
    lang: str = Path(..., regex="^(lv|en)$"),
    token: str = Query(..., alias="token"),
    db: Session = Depends(get_db),
):
    """Confirm subscription via email link. Sets confirmed=True and redirects to home."""
    if confirm_subscriber(db, token):
        return RedirectResponse(url=f"/{lang}/?newsletter=confirmed", status_code=303)
    return RedirectResponse(url=f"/{lang}/?newsletter=confirm_invalid", status_code=303)


@app.get("/{lang}/search", response_class=HTMLResponse)
def search(
    request: Request,
    lang: str = Path(..., regex="^(lv|en)$"),
    q: str = Query(""),
    sort: str = Query("relevance", regex="^(relevance|price)$"),
    mode: str = Query("", regex="^(strict|all)?$"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    from collections import OrderedDict
    query_product_type = detect_query_product_type(q) if q.strip() else None
    intent = classify_query(q) if q.strip() else None
    search_mode = mode if mode else ("strict" if query_product_type else "all")
    include_related = search_mode == "all"
    hits, fallback_message, structured = search_products_multi(db, q, include_related=include_related) if q.strip() else ([], None, None)
    all_meta = {info.retailer_id: info for info in get_all_retailer_info()}

    if structured is not None:
        ordered = OrderedDict()
        for r in structured.retailers:
            ordered[r["retailer_id"]] = r["all_items"]
        total_results = structured.total_items
        retailer_count = len(structured.retailers)
        all_hits_sorted = structured.full_results
        cheapest_per_retailer = {r["retailer_id"]: r["best_price"] for r in structured.retailers}
        comparison_rows = [
            {"retailer_id": r["retailer_id"], "retailer_name": r["retailer_name"], "price": r["best_price"], "is_cheapest": r["retailer_id"] == structured.cheapest["retailer_id"]}
            for r in structured.retailers
        ]
        best_per_retailer = {r["retailer_id"]: r["top_items"] for r in structured.retailers}
        comparison_data = {
            "product_type": structured.product_type,
            "summary": {
                "cheapest_retailer_id": structured.cheapest["retailer_id"],
                "cheapest_retailer_name": structured.cheapest["retailer_name"],
                "cheapest_price": structured.cheapest["best_price"],
            },
            "retailers": structured.retailers,
            "total_items": structured.total_items,
        }
        query_product_type = structured.product_type
    else:
        by_retailer: dict[str, list] = OrderedDict()
        for h in hits:
            by_retailer.setdefault(h.retailer_id, []).append(h)
        retailer_order = [info.retailer_id for info in get_all_retailer_info()]
        ordered = OrderedDict()
        for rid in retailer_order:
            if rid in by_retailer:
                ordered[rid] = by_retailer[rid]
        for rid, items in by_retailer.items():
            if rid not in ordered:
                ordered[rid] = items
        if sort == "price":
            for rid in ordered:
                ordered[rid] = sorted(ordered[rid], key=lambda h: (h.price, h.title))
        else:
            for rid in ordered:
                ordered[rid] = sorted(ordered[rid], key=lambda h: (-h.relevance, h.price))
        total_results = sum(len(items) for items in ordered.values())
        retailer_count = len(ordered)
        all_hits_sorted = sorted(
            (h for items in ordered.values() for h in items),
            key=lambda h: (h.price, h.title),
        )
        cheapest_per_retailer = {
            rid: min((h.price for h in items), default=None)
            for rid, items in ordered.items()
        }
        comparison_rows = []
        if cheapest_per_retailer and query_product_type:
            sorted_retailers_by_price = sorted(
                [(rid, p) for rid, p in cheapest_per_retailer.items() if p is not None],
                key=lambda x: x[1],
            )
            min_price = sorted_retailers_by_price[0][1] if sorted_retailers_by_price else None
            for rid, price in sorted_retailers_by_price:
                meta = all_meta.get(rid)
                comparison_rows.append({
                    "retailer_id": rid,
                    "retailer_name": meta.display_name if meta else rid,
                    "price": price,
                    "is_cheapest": price == min_price,
                })
        best_per_retailer = {rid: items[:3] for rid, items in ordered.items()}
        comparison_data = None

    updated = get_last_updated(db)
    suggestions = get_search_suggestions(q) if q.strip() and not hits else []
    return templates.TemplateResponse(
        "results.html",
        template_context(
            request,
            lang=lang,
            path_without_lang_override="/search",
            latest_pricing_update=get_latest_pricing_update_label(db, lang),
            query=q,
            results_by_retailer=ordered,
            sort_mode=sort,
            search_mode=search_mode,
            search_intent=intent,
            query_product_type=query_product_type,
            query_product_type_display=get_product_type_display(query_product_type) if query_product_type else None,
            search_fallback_message=fallback_message,
            total_results=total_results,
            retailer_count=retailer_count,
            last_updated=updated,
            retailer_meta=all_meta,
            suggestions=suggestions,
            all_hits_sorted=all_hits_sorted,
            cheapest_per_retailer=cheapest_per_retailer,
            comparison_rows=comparison_rows,
            best_per_retailer=best_per_retailer,
            comparison_data=comparison_data,
        ),
    )


@app.post("/{lang}/basket", response_class=HTMLResponse)
def basket(
    request: Request,
    lang: str = Path(..., regex="^(lv|en)$"),
    items: str = Form(""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    item_list = [line.strip() for line in items.splitlines() if line.strip()]
    results = compute_basket(db, item_list) if item_list else []
    updated = get_last_updated(db)
    all_meta = {info.retailer_id: info for info in get_all_retailer_info()}
    return templates.TemplateResponse(
        "basket.html",
        template_context(
            request,
            lang=lang,
            path_without_lang_override="/basket",
            latest_pricing_update=get_latest_pricing_update_label(db, lang),
            items=items,
            item_list=item_list,
            results=results,
            last_updated=updated,
            retailer_meta=all_meta,
        ),
    )


# ------------------------------------------------------------------
# Admin API
# ------------------------------------------------------------------

@app.post("/admin/run-ingest", dependencies=[Depends(require_admin)])
def admin_run_ingest(db: Session = Depends(get_db)) -> dict:
    summary = run_full_ingest(db)
    return {"status": "ok", "summary": summary}


@app.get("/{lang}/admin", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
def admin_dashboard(
    request: Request,
    lang: str = Path(..., regex="^(lv|en)$"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    from sqlalchemy import func as sqlfunc

    all_meta = get_all_retailer_info()
    updated = get_last_updated(db)

    # Product counts per retailer (latest scrape only)
    latest_sub = (
        db.query(
            ProductOffer.retailer_id,
            sqlfunc.max(ProductOffer.scraped_at).label("latest"),
        )
        .group_by(ProductOffer.retailer_id)
        .subquery()
    )
    count_rows = (
        db.query(ProductOffer.retailer_id, sqlfunc.count(ProductOffer.id))
        .join(
            latest_sub,
            (ProductOffer.retailer_id == latest_sub.c.retailer_id)
            & (ProductOffer.scraped_at == latest_sub.c.latest),
        )
        .group_by(ProductOffer.retailer_id)
        .all()
    )
    product_counts = dict(count_rows)

    retailer_rows = []
    for info in all_meta:
        retailer_rows.append({
            "retailer_id": info.retailer_id,
            "display_name": info.display_name,
            "catalog_type": info.catalog_type.value,
            "description": info.description,
            "basket_eligible": info.basket_eligible,
            "product_count": product_counts.get(info.retailer_id, 0),
            "last_updated": updated.get(info.retailer_id, "never"),
        })

    return templates.TemplateResponse(
        "admin.html",
        template_context(request, lang=lang, path_without_lang_override="/admin", retailers=retailer_rows),
    )


@app.get("/{lang}/admin/newsletter", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
def admin_newsletter(
    request: Request,
    lang: str = Path(..., regex="^(lv|en)$"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    stats = get_newsletter_admin_stats(db)
    return templates.TemplateResponse(
        "admin_newsletter.html",
        template_context(request, lang=lang, path_without_lang_override="/admin/newsletter", **stats),
    )


# ------------------------------------------------------------------
# JSON API mirrors (for programmatic use)
# ------------------------------------------------------------------

@app.get("/api/products/search")
def api_products_search(
    q: str = Query(""),
    include_related: bool = Query(False, description="Include related product types (e.g. yogurt, cheese when searching milk)"),
    db: Session = Depends(get_db),
) -> dict:
    """Product search by type (supermarket-style). Grouped by retailer, sorted by price."""
    if not (q or "").strip():
        return {"query": "", "product_type": None, "include_related": False, "fallback": False, "groups": []}

    intent = classify_query(q)
    hits, fallback_message, structured = search_products_multi(db, q, include_related=include_related)
    product_type = structured.product_type if structured else (intent.category.key if (intent.type == "product_type" and intent.category) else None)

    # Group by retailer, sort products by price within each group
    from collections import OrderedDict
    by_retailer: dict[str, list] = OrderedDict()
    for h in hits:
        by_retailer.setdefault(h.retailer_id, []).append(h)
    for rid in by_retailer:
        by_retailer[rid] = sorted(by_retailer[rid], key=lambda x: (x.price, x.title))

    groups = [
        {
            "retailer_id": rid,
            "retailer_name": by_retailer[rid][0].retailer_name if by_retailer[rid] else "",
            "products": [
                {
                    "product_id": h.product_id,
                    "title": h.title,
                    "price": h.price,
                    "unit_price": h.unit_price,
                    "unit": h.unit,
                    "size": h.size,
                    "image_url": h.image_url,
                    "url": h.url,
                    "product_type": h.product_type,
                }
                for h in by_retailer[rid]
            ],
        }
        for rid in sorted(by_retailer.keys())
    ]

    return {
        "query": q.strip(),
        "product_type": product_type,
        "include_related": include_related,
        "fallback": fallback_message == "fallback",
        "groups": groups,
    }


@app.get("/api/search")
def api_search(q: str = Query(""), db: Session = Depends(get_db)) -> list[dict]:
    results = search_products(db, q) if q.strip() else []
    return [
        {
            "retailer": r.retailer_name,
            "retailer_id": r.retailer_id,
            "title": r.title,
            "price": r.price,
            "unit_price": r.unit_price,
            "unit": r.unit,
            "size_text": r.size_text,
            "url": r.url,
            "similarity": r.similarity,
            "catalog_type": r.catalog_type,
            "catalog_description": r.catalog_description,
        }
        for r in results
    ]


@app.post("/api/basket")
def api_basket(payload: dict, db: Session = Depends(get_db)) -> list[dict]:
    item_list = payload.get("items", [])
    results = compute_basket(db, item_list) if item_list else []
    return [
        {
            "retailer": r.retailer_name,
            "retailer_id": r.retailer_id,
            "total": r.total,
            "found": r.found_count,
            "missing": r.missing,
            "catalog_type": r.catalog_type,
            "catalog_description": r.catalog_description,
            "basket_eligible": r.basket_eligible,
            "items": [
                {"query": i.query, "matched": i.title, "price": i.price, "score": i.score, "confidence": i.confidence}
                for i in r.items
            ],
        }
        for r in results
    ]


# ------------------------------------------------------------------
# Price history API
# ------------------------------------------------------------------

@app.get("/api/history/{offer_id}")
def api_price_history(offer_id: int, db: Session = Depends(get_db)) -> dict:
    stats = get_price_stats(db, offer_id)
    if not stats:
        return {"error": "product not found"}
    return {
        "offer_id": stats.offer_id,
        "retailer": stats.retailer_name,
        "title": stats.title,
        "current_price": stats.current_price,
        "lowest_price": stats.lowest_price,
        "highest_price": stats.highest_price,
        "avg_price_30d": stats.avg_price_30d,
        "price_range": stats.price_range,
        "observation_count": stats.observation_count,
        "first_seen": stats.first_seen,
        "last_seen": stats.last_seen,
        "last_price_change": stats.last_price_change,
        "price_trend": stats.price_trend,
        "history": [
            {"date": p.date, "price": p.price, "unit_price": p.unit_price}
            for p in stats.history
        ],
    }


# ------------------------------------------------------------------
# Basket price index API
# ------------------------------------------------------------------

@app.get("/api/basket-index")
def api_basket_index(days: int = Query(90), db: Session = Depends(get_db)) -> dict:
    history = get_price_index_history(db, days=days)
    cheapest = get_cheapest_retailer_of_day(db)
    retailer_meta = {
        rid: {
            "display_name": get_retailer_info(rid).display_name,
            "catalog_type": get_retailer_info(rid).catalog_type.value,
            "description": get_retailer_info(rid).description,
        }
        for rid in history.series
    }
    return {
        "basket_items": history.basket_items,
        "days": history.days,
        "series": history.series,
        "retailer_meta": retailer_meta,
        "cheapest_by_day": history.cheapest_by_day,
        "cheapest_today": {
            "retailer_id": cheapest[0],
            "basket_total": cheapest[1],
        } if cheapest else None,
    }


# ------------------------------------------------------------------
# Newsletter (email capture only, no account)
# ------------------------------------------------------------------


@app.post("/api/newsletter/subscribe")
def api_newsletter_subscribe(
    body: NewsletterSubscribeBody,
    db: Session = Depends(get_db),
) -> dict[str, str]:
    """Subscribe an email for price drop alerts, weekly basket, savings insights. No login."""
    result = newsletter_subscribe(db, body.email, body.source, body.preferences)
    return result
