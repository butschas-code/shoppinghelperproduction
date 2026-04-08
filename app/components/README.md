# UI components (CartWise)

This project is **server-rendered (Jinja2)**, not React. Reusable UI is implemented as **CSS classes** in [`app/web/static/ui.css`](../web/static/ui.css):

| Concept | Classes |
|--------|---------|
| Button | `.cw-btn`, `.cw-btn--primary`, `.cw-btn--secondary` |
| Card | `.cw-card`, `.cw-card--interactive` |
| Badge | `.cw-badge`, `.cw-badge--best`, `.cw-badge--live` |
| Skeleton | `.cw-skeleton`, `.cw-skeleton--block` |
| Nav | `.cw-nav`, `.cw-nav__link` |
| Price text | `.cw-price` (tabular numerals) |

Shared chrome: [`app/web/templates/_chrome_header.html`](../web/templates/_chrome_header.html), [`_site_nav.html`](../web/templates/_site_nav.html).
