"""Bilingual search: expand English query terms to Latvian equivalents before matching.

Expansion runs before token scoring and does not change fingerprint logic.
Lookup is case-insensitive and diacritics-insensitive (via normalize_text).
"""

from __future__ import annotations

from app.services.normalize import normalize_text, tokenize

# Phrase (normalized, lowercase) -> extra terms to add for matching.
PHRASE_SYNONYMS: dict[str, list[str]] = {
    "red lentils": ["sarkanas lēcas"],
    "greek yogurt": ["grieķu jogurts"],
    "greek yoghurt": ["grieķu jogurts"],
    "dish soap": ["trauku mazgāšanas līdzeklis", "trauku", "mazgāšanas"],
    "olive oil": ["olīveļļa", "eļļa"],
    "ice cream": ["saldējums"],
    "sour cream": ["krējums"],
    "cottage cheese": ["biezpiens"],
    "toilet paper": ["tualetes papīrs"],
    "ground meat": ["malta gaļa"],
    "minced meat": ["malta gaļa"],
    "whipped cream": ["putukrējums"],
    "cream cheese": ["krēmsiers"],
    "peanut butter": ["zemesriekstu sviests"],
    "orange juice": ["apelsīnu sula"],
    "apple juice": ["ābolu sula"],
    "sparkling water": ["gāzēts ūdens"],
    "washing powder": ["veļas pulveris"],
    "laundry detergent": ["veļas mazgāšanas līdzeklis", "veļas"],
}

# English (normalized lowercase) -> Latvian terms to add to the query.
# Short/partial forms also map to full terms for prefix matching.
search_synonyms: dict[str, list[str]] = {
    # ── dairy ──
    "milk": ["piens"],
    "butter": ["sviests"],
    "cheese": ["siers"],
    "yogurt": ["jogurts"],
    "yogurts": ["jogurts"],
    "yoghurt": ["jogurts"],
    "cream": ["krējums"],
    "kefir": ["kefīrs"],
    # ── eggs ──
    "eggs": ["olas"],
    "egg": ["olas"],
    # ── bread / bakery ──
    "bread": ["maize"],
    "baguette": ["bagete"],
    # ── meat / poultry ──
    "chicken": ["vista"],
    "beef": ["liellopu gaļa", "liellops"],
    "pork": ["cūkgaļa", "cūkas"],
    "minced": ["malta gaļa"],
    "mince": ["malta gaļa"],
    "sausage": ["desa"],
    "sausages": ["desas"],
    "ham": ["šķiņķis"],
    "bacon": ["bekons"],
    # ── fish ──
    "fish": ["zivis"],
    "salmon": ["lasis"],
    "tuna": ["tuncis"],
    "shrimp": ["garneles"],
    "shrimps": ["garneles"],
    # ── fruit ──
    "banana": ["banāni"],
    "bananas": ["banāni"],
    "apple": ["āboli"],
    "apples": ["āboli"],
    "orange": ["apelsīni"],
    "oranges": ["apelsīni"],
    "lemon": ["citroni"],
    "lemons": ["citroni"],
    "strawberry": ["zemenes"],
    "strawberries": ["zemenes"],
    "grapes": ["vīnogas"],
    "pear": ["bumbieri"],
    "pears": ["bumbieri"],
    "avocado": ["avokado"],
    "avocados": ["avokado"],
    "avokado": ["avokado"],
    "mango": ["mango"],
    "watermelon": ["arbūzs"],
    "blueberry": ["mellenes"],
    "blueberries": ["mellenes"],
    # ── vegetables ──
    "potatoes": ["kartupeļi"],
    "potato": ["kartupeļi"],
    "tomato": ["tomāti"],
    "tomatoes": ["tomāti"],
    "onion": ["sīpoli"],
    "onions": ["sīpoli"],
    "garlic": ["ķiploki"],
    "carrot": ["burkāni"],
    "carrots": ["burkāni"],
    "cucumber": ["gurķi"],
    "cucumbers": ["gurķi"],
    "pepper": ["pipari"],
    "peppers": ["pipari"],
    "cabbage": ["kāposti"],
    "broccoli": ["brokoļi"],
    "mushroom": ["sēnes"],
    "mushrooms": ["sēnes"],
    "lettuce": ["salāti"],
    "spinach": ["spināti"],
    "corn": ["kukurūza"],
    "zucchini": ["cukini"],
    # ── grains / staples ──
    "rice": ["rīsi"],
    "pasta": ["makaroni"],
    "flour": ["milti"],
    "oats": ["auzu pārslas"],
    "oatmeal": ["auzu pārslas"],
    # ── condiments / pantry ──
    "sugar": ["cukurs"],
    "salt": ["sāls"],
    "oil": ["eļļa"],
    "vinegar": ["etiķis"],
    "honey": ["medus"],
    "ketchup": ["kečups"],
    "mustard": ["sinepes"],
    "mayonnaise": ["majonēze"],
    "soy": ["sojas"],
    # ── drinks ──
    "coffee": ["kafija"],
    "tea": ["tēja"],
    "water": ["ūdens"],
    "juice": ["sula"],
    # ── sweets / snacks ──
    "chocolate": ["šokolāde"],
    "cookies": ["cepumi"],
    "biscuits": ["cepumi"],
    "chips": ["čipsi"],
    "crackers": ["krekeri"],
    "candy": ["konfektes"],
    # ── frozen ──
    "frozen": ["saldēti"],
    # ── legumes ──
    "lentils": ["lēcas"],
    "lentil": ["lēcas"],
    "lecas": ["lēcas"],
    "beans": ["pupiņas"],
    "chickpeas": ["aunazirņi"],
    "red": ["sarkanas"],
    # ── household / non-food ──
    "shampoo": ["šampūns"],
    "toothpaste": ["zobu pasta"],
    "soap": ["ziepes"],
    "detergent": ["mazgāšanas līdzeklis"],
    # ── partial / short query expansion ──
    "avo": ["avocado", "avokado"],
    "jogurt": ["jogurts"],
    "piens": ["piens"],
    "vista": ["vista"],
    "siers": ["siers"],
    "maize": ["maize"],
    "olas": ["olas"],
    "zivis": ["zivis"],
    "sviests": ["sviests"],
    "kafija": ["kafija"],
    "teja": ["tēja"],
    "sula": ["sula"],
    "milti": ["milti"],
    "cukurs": ["cukurs"],
    "ella": ["eļļa"],
}


def expand_query_for_search(query: str) -> str:
    """Expand English terms in query with Latvian synonyms before matching.

    Runs phrase expansion first (e.g. red lentils -> sarkanas lecas), then
    token expansion. Original query is preserved; synonyms are appended.
    """
    if not (query or "").strip():
        return query
    q_norm = normalize_text(query)
    extra: list[str] = []
    for phrase, syns in PHRASE_SYNONYMS.items():
        if phrase in q_norm:
            extra.extend(syns)
    tokens = tokenize(query)
    for t in tokens:
        syns = search_synonyms.get(t)
        if syns:
            extra.extend(syns)
    if not extra:
        return query
    return query.strip() + " " + " ".join(extra)


def get_search_suggestions(query: str) -> list[str]:
    """Return suggested alternative search terms when no results (e.g. avocado -> avokado)."""
    if not (query or "").strip():
        return []
    tokens = tokenize(query)
    out: list[str] = []
    seen: set[str] = set()
    for t in tokens:
        syns = search_synonyms.get(t)
        if syns:
            for s in syns:
                if s not in seen and s != t:
                    seen.add(s)
                    out.append(s)
    return out[:5]
