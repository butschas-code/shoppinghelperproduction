"""Canonical product intents for the shopping assistant.

Maps user query (EN + LV) to a product intent key and provides
primary_roots (for ranking) and exclude_roots (to demote off-topic products).

Multi-language: English queries map to Latvian product titles via primary_roots
(e.g. milk → piens, yogurt → jogurts, chicken → vista, dish soap → trauku).
"""

from __future__ import annotations

from app.services.normalize import normalize_text

# Explicit EN → LV (and key LV) for search expansion / display. Used by fallback fuzzy search.
QUERY_TO_LV: dict[str, str] = {
    "milk": "piens",
    "yogurt": "jogurts",
    "yoghurt": "jogurts",
    "chicken": "vista",
    "dish soap": "trauku",
    "avocado": "avokado",
    "cheese": "siers",
    "butter": "sviests",
    "bread": "maize",
    "eggs": "olas",
    "rice": "risi",
    "banana": "banani",
    "apple": "aboli",
    "coffee": "kafija",
    "tea": "teja",
    "water": "udens",
    "juice": "sula",
    "pasta": "makaroni",
    "potatoes": "kartupeli",
    "fish": "zivis",
    "beef": "gala",
    "pork": "cukas",
    "minced": "malta gala",
    "toothpaste": "zobu pasta",
    "shampoo": "sampuns",
    "toilet paper": "tualetes papirs",
    "laundry": "velas",
    "washing": "velas",
}

# Intent key -> { terms (query triggers), primary_roots (title matching), exclude_roots (penalise) }
PRODUCT_INTENTS: dict[str, dict[str, list[str]]] = {
    "milk": {
        "terms": ["milk", "piens", "piena"],
        "primary_roots": ["piens", "piena", "milk"],
        "exclude_roots": [
            "sierin", "desert", "baton", "konfekt",
            "sokolad", "cepum", "sald", "krem", "kakao",
            "iebiezinat", "kondenset", "jogurt", "sviest",
            "desa", "desin", "des",
            "krauksk", "cips",
            "pudin", "merce", "kokteil",
            "cal",
            # Flavoured milk drinks ("piena dzēriens", "piens kakao") are not plain milk.
            "dzerien",
            "biezpien",  # cottage cheese
            "kefir",
        ],
    },
    "yogurt": {
        "terms": ["yogurt", "yoghurt", "jogurts", "jogurti"],
        "primary_roots": ["jogurt", "jogurts", "yogurt", "yoghurt"],
        "exclude_roots": [
            "sokolad", "baton", "konfekt", "saldējums", "saldejums", "zupa",
            "desa", "cips", "krauksk",
        ],
    },
    "avocado": {
        "terms": ["avocado", "avokado", "avo"],
        "primary_roots": ["avokado", "avocado"],
        "exclude_roots": ["ella", "merce", "salsa", "cips", "krauksk"],
    },
    "chicken": {
        "terms": ["chicken", "vista", "vistas", "fileja"],
        # "fileja" (fillet) is ambiguous — fish fillets match too. Match chicken
        # via "vista*" stem; fish fillets will be handled by the fish intent.
        "primary_roots": ["vista", "vistas", "chicken"],
        "exclude_roots": [
            "cips", "zupa", "garsa", "buljons", "saldējums", "saldejums",
            "desa", "desin", "cisin", "pastet", "pelmen", "frikadel",
            "naget", "konserv", "krauksk",
        ],
    },
    "cheese": {
        "terms": ["cheese", "siers", "sieri"],
        "primary_roots": ["siers", "sieri", "cheese"],
        "exclude_roots": [
            "sierins", "ziepes", "biezpiens",
            "cips", "krauksk", "desa", "pelmen", "longchip",
        ],
    },
    "eggs": {
        "terms": ["eggs", "egg", "olas", "ola"],
        "primary_roots": ["olas", "ola", "eggs", "egg"],
        "exclude_roots": ["majonez", "cepum", "desert", "pudin"],
    },
    "bread": {
        "terms": ["bread", "maize", "maizite", "rupjmaize"],
        "primary_roots": ["maize", "maizite", "rupjmaize", "bread"],
        "exclude_roots": ["flakes", "ziepes", "milti", "mikla"],
    },
    "rice": {
        "terms": ["rice", "rīsi", "risi", "basmati", "jasmin"],
        "primary_roots": ["risi", "rice", "basmati", "jasmin"],
        "exclude_roots": ["kuka", "pudins", "flakes", "desa", "cips"],
    },
    "butter": {
        "terms": ["butter", "sviests"],
        "primary_roots": ["sviests", "butter"],
        "exclude_roots": ["margarin", "ziepes"],
    },
    "banana": {
        "terms": ["banana", "bananas", "banāni", "banani"],
        "primary_roots": ["banani", "banana", "banans"],
        "exclude_roots": [
            "kuka", "maize", "dzeriens", "cipsi", "krauksk",
            "sokolad", "saldejums", "konfekt", "baton",
        ],
    },
    "apple": {
        "terms": ["apple", "apples", "āboli", "aboli"],
        "primary_roots": ["aboli", "apple"],
        "exclude_roots": [
            "sula", "dzeriens", "cipsi", "krauksk",
            "kuka", "saldejums", "konfekt", "baton",
        ],
    },
    "dish_soap": {
        "terms": ["dish soap", "trauku", "trauku mazg", "trauku mazgāšanas"],
        "primary_roots": ["trauku", "trauku mazg"],
        "exclude_roots": ["edien", "zupa", "piens"],
    },
    "shampoo": {
        "terms": ["shampoo", "šampūns", "sampuns"],
        "primary_roots": ["sampuns", "shampoo"],
        "exclude_roots": ["edien", "zupa"],
    },
    "toothpaste": {
        "terms": ["toothpaste", "zobu pasta", "zobu ziepes"],
        "primary_roots": ["zobu pasta", "toothpaste"],
        "exclude_roots": ["makaroni", "pasta"],
    },
    "coffee": {
        "terms": ["coffee", "kafija"],
        "primary_roots": ["kafija", "coffee", "graudi", "malts"],
        "exclude_roots": [
            "dzeriens", "gatavs", "ledus",
            "konfekt", "cepum", "saldejums", "baton",
        ],
    },
    "tea": {
        "terms": ["tea", "teja", "tēja"],
        "primary_roots": ["teja", "tea"],
        "exclude_roots": [
            "dzeriens", "ledus",
            "konfekt", "cepum", "saldejums",
        ],
    },
    "water": {
        "terms": ["water", "ūdens", "udens"],
        "primary_roots": ["udens", "water"],
        "exclude_roots": ["gaze", "sula"],
    },
    "juice": {
        "terms": ["juice", "sula"],
        "primary_roots": ["sula", "juice"],
        "exclude_roots": ["desert", "merce"],
    },
    "pasta": {
        "terms": ["pasta", "makaroni", "spageti", "penne"],
        "primary_roots": ["makaroni", "spageti", "penne", "pasta"],
        "exclude_roots": ["merce", "sauce", "zupa"],
    },
    "potatoes": {
        "terms": ["potatoes", "potato", "kartupeļi", "kartupeli"],
        "primary_roots": ["kartupeli", "potato", "potatoes"],
        "exclude_roots": ["cipsi", "chips", "milti", "krauksk", "pire"],
    },
    "tomato": {
        "terms": ["tomato", "tomatoes", "tomāti", "tomati"],
        "primary_roots": ["tomati", "tomato"],
        "exclude_roots": ["merce", "kecup", "sula", "konserv", "pasta"],
    },
    "onion": {
        "terms": ["onion", "onions", "sīpoli", "sipoli"],
        "primary_roots": ["sipoli", "onion"],
        "exclude_roots": ["cips", "krauksk", "merce"],
    },
    "flour": {
        "terms": ["flour", "milti"],
        "primary_roots": ["milti", "flour"],
        "exclude_roots": ["kuka", "maize", "cepum"],
    },
    "oil": {
        "terms": ["oil", "eļļa", "ella"],
        "primary_roots": ["ella", "oil"],
        "exclude_roots": ["filtr", "motor"],
    },
    "salt": {
        "terms": ["salt", "sāls", "sals"],
        "primary_roots": ["sals", "salt"],
        "exclude_roots": ["desert", "cips", "krauksk"],
    },
    "sugar": {
        "terms": ["sugar", "cukurs"],
        "primary_roots": ["cukurs", "sugar"],
        "exclude_roots": ["aizvietotaj", "saldejums", "dzeriens", "sirup"],
    },
    "chocolate": {
        "terms": ["chocolate", "šokolāde", "sokolade"],
        "primary_roots": ["sokolade", "chocolate"],
        "exclude_roots": ["merce", "sula", "dzeriens", "saldejums"],
    },
    "cookies": {
        "terms": ["cookies", "biscuits", "cepumi"],
        "primary_roots": ["cepumi", "cookies", "biscuits"],
        "exclude_roots": [],
    },
    "ice_cream": {
        "terms": ["ice cream", "saldējums", "saldejums"],
        "primary_roots": ["saldejums", "ice cream"],
        "exclude_roots": ["merce", "zupa"],
    },
    "toilet_paper": {
        "terms": ["toilet paper", "tualetes papīrs", "tualetes papirs"],
        "primary_roots": ["tualetes papirs", "toilet paper"],
        "exclude_roots": ["edien", "maize"],
    },
    "laundry_detergent": {
        "terms": ["laundry", "washing", "veļas", "velas"],
        "primary_roots": ["velas", "laundry", "washing"],
        "exclude_roots": ["trauku", "edien"],
    },
    "fish": {
        "terms": ["fish", "zivis", "zivs"],
        "primary_roots": ["zivis", "zivs", "fish", "lasi", "salmon"],
        "exclude_roots": ["ella", "desert", "konserv", "zupa", "buljons", "cips"],
    },
    "beef": {
        "terms": ["beef", "liellops", "gala"],
        # "gala" alone just means "meat" in Latvian — it matches pork, chicken, fish,
        # sausages, everything. Anchor beef on "liellop*" (beef-specific root).
        "primary_roots": ["liellop", "liellops", "liellopa", "beef"],
        "exclude_roots": [
            "vista", "cuka", "zivis",
            "desa", "cisin", "pastet", "pelmen", "konserv",
            "cips", "krauksk", "buljons",
        ],
    },
    "pork": {
        "terms": ["pork", "cūka", "cukas"],
        "primary_roots": ["cukas", "cūka", "pork"],
        "exclude_roots": [
            "vista", "zivis",
            "desa", "cisin", "pastet", "pelmen", "konserv",
            "cips", "krauksk", "buljons",
        ],
    },
    "minced_meat": {
        "terms": ["minced", "ground meat", "malta gaļa", "malta gala", "farš", "fars"],
        # "gala" alone is just "meat" — anchor minced on "malt*" / "fars".
        "primary_roots": ["malta gala", "malta", "maltas", "minced", "fars", "farš"],
        "exclude_roots": ["desert", "zupa", "buljons", "konserv"],
    },
}

# Normalised term -> intent key (for detection)
_TERM_TO_INTENT: dict[str, str] = {}
for intent_key, data in PRODUCT_INTENTS.items():
    for term in data["terms"]:
        norm = normalize_text(term)
        if norm and norm not in _TERM_TO_INTENT:
            _TERM_TO_INTENT[norm] = intent_key
    type_norm = normalize_text(intent_key.replace("_", " "))
    if type_norm and type_norm not in _TERM_TO_INTENT:
        _TERM_TO_INTENT[type_norm] = intent_key


def get_intent_config(intent_key: str) -> dict[str, list[str]] | None:
    """Return config for an intent (primary_roots, exclude_roots)."""
    return PRODUCT_INTENTS.get(intent_key)


def detect_product_intent(query: str) -> str | None:
    """Detect product intent from search query.

    Normalises query and checks against known terms.
    Returns canonical intent key (e.g. 'milk', 'yogurt') or None.
    """
    if not (query or "").strip():
        return None
    norm = normalize_text(query.strip())
    if not norm:
        return None
    if norm in _TERM_TO_INTENT:
        return _TERM_TO_INTENT[norm]
    for token in norm.split():
        if len(token) >= 2 and token in _TERM_TO_INTENT:
            return _TERM_TO_INTENT[token]
    return None
