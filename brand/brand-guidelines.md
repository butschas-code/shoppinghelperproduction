# CartWise — Brand & design system

## Brand (fixed)

| Field | Value |
|--------|--------|
| **Name** | CartWise |
| **Product** | Realtime grocery price comparison for a full basket across stores. |
| **Promise** | Know the best basket price — instantly. |
| **Tone** | Friendly, calm, premium utility. Not “discount/cheap.” |
| **Visual** | apple.com-like: whitespace, subtle depth, soft gradients optional, refined type, micro-motion, glass surfaces optional. No loud colors or heavy shadows. |

## Color system

- **Primary (Pulse Green):** `#16A34A` — primary CTAs (white text on green).
- **Ink:** `#0B1220` — primary text.
- **Accent (Savings Amber):** `#F59E0B` — “Best basket” / best-price badges with **dark text** `#0B1220` (never neon green for “best”).
- **Background:** `#F7F8FA` — page background.
- **Surface:** `#FFFFFF` — cards/panels.
- **Border:** `#E6E8EE`
- **Link Blue:** `#2563EB` — links only.

### Neutrals

N0 `#FFFFFF` · N50 `#F7F8FA` · N100 `#EEF1F6` · N200 `#E6E8EE` · N300 `#CBD5E1` · N500 `#64748B` · N700 `#334155` · N900 `#0B1220`

### Semantic

- Success `#16A34A` — e.g. “price dropped” (subtle 5–8% green tint bg).
- Warn `#F59E0B`
- Error `#EF4444` — “price increased” (subtle red tint bg).
- Info `#2563EB`

## Usage rules

1. **Best price / best basket chip:** Accent `#F59E0B`, text `#0B1220`.
2. **Price dropped:** Success green + small ↓, subtle success background tint.
3. **Price increased:** Error red + small ↑, subtle error background tint.
4. **Primary CTA:** Green `#16A34A`, white label.
5. **Secondary CTA:** White surface, border `#E6E8EE`, Ink text.
6. **Links:** Link Blue only.
7. **Charts (future):** Ink/N500 lines; best store line Primary; “you save” Accent.

## Typography

- **Font:** Inter (Google Fonts), weights 400 / 500 / 600.
- **Prices / numbers:** `font-variant-numeric: tabular-nums; font-feature-settings: "tnum" 1, "ss01" 1;`

| Role | Size / line | Weight |
|------|-------------|--------|
| H1 | 44px / 52px | 600 |
| H2 | 32px / 40px | 600 |
| H3 | 22px / 30px | 600 |
| Body | 16px / 24px | 400 |
| Small | 13px / 18px | 400 |
| Label | 12px / 16px | 500 |

## Components

- **Radius:** cards 18px · inputs 14px · pills 999px.
- **Border:** 1px solid `#E6E8EE`.
- **Shadow:** `0 1px 1px rgba(15,23,42,.04), 0 12px 30px rgba(15,23,42,.06)`.
- **Glass (optional):** `background: rgba(255,255,255,.72); backdrop-filter: blur(14px); border: 1px solid rgba(230,232,238,.9)`.

## Motion

- Transitions: **180ms ease-out** for hover/focus.
- Cards: hover `translateY(-2px)` + slightly stronger shadow.
- Buttons: gentle highlight on hover; no bounce.

## Logo

- **Icon:** Shopping basket outline + three vertical “signal” bars inside; rounded stroke; works at 24px; flat single-color variants.
- **Wordmark:** CartWise (Inter SemiBold, ~−1% letter-spacing). No dots/gimmicks on the *i*.
- **Files:** `brand/logo/cartpulse-icon.svg`, `cartpulse-logo.svg`, `cartpulse-appicon.svg` (+ `cartwise-mark-source.png` from product art).

## Repo implementation

- **Tokens:** `brand/tokens.json`
- **UI/CSS:** `app/web/static/ui.css` (variables + component classes)
- **Legacy page styles:** `app/web/static/style.css` (loads after `ui.css`)
