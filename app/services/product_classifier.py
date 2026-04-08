"""Classify product title into product type (in-memory only, no DB).

Rules: title starts with "piens" -> milk; contains "jogurts" -> yogurt;
excludes so "chocolate milk" is not classified as milk. Used to filter
search results by type when detector has identified query type.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from app.services.normalize import normalize_text


@dataclass
class _Rule:
    type_key: str
    include: list[str]
    exclude: list[str]
    starts_only: bool = False


def _r(key: str, include: list[str], exclude: list[str], starts_only: bool = False) -> _Rule:
    return _Rule(
        type_key=key,
        include=[normalize_text(x) for x in include],
        exclude=[normalize_text(x) for x in exclude],
        starts_only=starts_only,
    )


# More specific types first (yogurt, cheese before milk; avocado before oil).
RULES: list[_Rule] = [
    # Non-food first (most specific)
    _r("dish_soap", ["trauku", "dish soap", "trauku mazg"], ["ēdiens", "zupa", "piens"], False),
    _r("toothpaste", ["zobu pasta", "toothpaste"], ["makaroni", "pasta"], False),
    _r("shampoo", ["sampuns", "šampūns", "shampoo"], ["ēdiens", "zupa"], False),
    _r("toilet_paper", ["tualetes papirs", "toilet paper"], ["ēdiens", "maize"], False),
    _r("laundry_detergent", ["velas", "veļas", "laundry", "washing"], ["trauku", "ēdiens"], False),
    # Dairy (specific before generic)
    _r("yogurt", ["jogurts", "jogurti", "yoghurt", "yogurt"],
        ["saldējums", "deserts", "zupa", "desa", "čipsi", "kraukšķi", "šokolāde"], False),
    _r("cheese", ["siers", "sieri", "cheese", "mozzarella", "cheddar"],
        ["sieriņš", "ziepes", "biezpiens", "čipsi", "kraukšķi", "desa", "pelmeņi", "longchips"], False),
    _r("butter", ["sviests", "butter"], ["margarīns", "ziepes"], False),
    _r("avocado", ["avokado", "avocado"],
        ["mērce", "eļļa", "salsas", "salsa", "čipsi", "oil", "kraukšķi"], False),
    _r("milk", ["piens", "piena", "milk"],
        ["jogurts", "siers", "sviests", "šokolāde", "sokolade", "chocolate",
         "kakao", "cepumi", "iebiezināts", "kondensētais",
         "desa", "desiņas", "desinas", "kraukšķi", "kraukski",
         "čipsi", "cipsi", "pudiņš", "pudins", "mērce", "merce",
         "konfekte", "batons", "deserts"], True),
    # Eggs
    _r("eggs", ["olas", "ola", "eggs", "egg"],
        ["majonēze", "cepumi", "deserts", "pudiņš"], False),
    # Meat / poultry (specific before generic)
    _r("chicken", ["vista", "vistas", "chicken", "fileja"],
        ["zupa", "buljons", "saldējums", "desa", "desiņas", "cīsiņi",
         "pastēte", "pelmeņi", "frikadeles", "nageti", "konservs",
         "čipsi", "kraukšķi"], False),
    _r("beef", ["liellops", "gala", "beef"],
        ["vista", "cūka", "zivis", "desa", "cīsiņi", "pastēte",
         "pelmeņi", "konservs", "čipsi", "kraukšķi", "buljons"], False),
    _r("pork", ["cūka", "cukas", "pork"],
        ["vista", "zivis", "desa", "cīsiņi", "pastēte",
         "pelmeņi", "konservs", "čipsi", "kraukšķi", "buljons"], False),
    _r("minced_meat", ["malta gala", "malta gaļa", "minced", "farš"],
        ["vista", "deserts", "čipsi"], False),
    # Fish
    _r("fish", ["zivis", "zivs", "fish", "lasi", "salmon"],
        ["eļļa", "deserts", "konservs", "zupa", "buljons", "čipsi"], False),
    # Grains
    _r("rice", ["rīsi", "risi", "rice", "basmati", "jasmin"],
        ["kūka", "pudins", "flakes", "desa", "čipsi"], False),
    _r("bread", ["maize", "maizite", "rupjmaize", "bread"],
        ["flakes", "ziepes", "milti", "mīkla"], False),
    _r("pasta", ["makaroni", "spageti", "penne", "pasta"],
        ["mērce", "sauce", "zupa"], False),
    _r("flour", ["milti", "flour"], ["kūka", "maize", "cepumi"], False),
    # Fruit
    _r("banana", ["banāni", "banani", "banana"],
        ["kūka", "maize", "dzeriens", "čipsi", "kraukšķi",
         "šokolāde", "saldējums", "konfekte", "batons"], False),
    _r("apple", ["āboli", "aboli", "apple"],
        ["sula", "dzeriens", "čipsi", "kraukšķi",
         "kūka", "saldējums", "konfekte", "batons"], False),
    # Vegetables
    _r("potatoes", ["kartupeli", "kartupeļi", "potatoes"],
        ["čipsi", "chips", "milti", "kraukšķi", "biezenis"], False),
    _r("tomato", ["tomati", "tomāti", "tomato"],
        ["mērce", "kečups", "sula", "konservs", "pasta"], False),
    _r("onion", ["sipoli", "sīpoli", "onion"],
        ["čipsi", "kraukšķi", "mērce"], False),
    # Drinks
    _r("coffee", ["kafija", "coffee", "graudi", "malts"],
        ["dzeriens", "gatavs", "ledus", "konfekte", "cepumi", "saldējums"], False),
    _r("tea", ["teja", "tēja", "tea"],
        ["dzeriens", "ledus", "konfekte", "cepumi", "saldējums"], False),
    _r("water", ["udens", "ūdens", "water"], ["gāze", "sula"], False),
    _r("juice", ["sula", "juice"], ["deserts", "mērce"], False),
    # Sweets / frozen
    _r("ice_cream", ["saldējums", "saldejums", "ice cream"], ["mērce", "zupa"], False),
    _r("chocolate", ["sokolade", "šokolāde", "chocolate"],
        ["mērce", "sula", "dzeriens", "saldējums"], False),
    # Pantry
    _r("oil", ["ella", "eļļa", "oil"], ["filtrs", "motors"], False),
    _r("salt", ["sals", "sāls", "salt"], ["deserts", "čipsi", "kraukšķi"], False),
    _r("sugar", ["cukurs", "sugar"], ["aizvietotājs", "saldējums", "dzeriens"], False),
]


def _contains_word(text_norm: str, token: str) -> bool:
    if not token:
        return False
    return bool(re.search(r"\b" + re.escape(token) + r"\b", text_norm))


def _starts_with_any(text_norm: str, tokens: list[str]) -> bool:
    parts = text_norm.split()
    first_word = parts[0] if parts else ""
    for t in tokens:
        if not t:
            continue
        if first_word.startswith(t) or t in first_word:
            return True
        if text_norm.startswith(t + " ") or text_norm.startswith(t):
            return True
    return False


def detect_product_type_from_title(title: str) -> str | None:
    """Classify product into one type from its title. In-memory only."""
    if not (title or "").strip():
        return None
    norm = normalize_text(title.strip())
    if not norm:
        return None

    for rule in RULES:
        if any(_contains_word(norm, ex) for ex in rule.exclude):
            continue
        if rule.starts_only:
            if _starts_with_any(norm, rule.include):
                return rule.type_key
        else:
            for inc in rule.include:
                if _contains_word(norm, inc) or _starts_with_any(norm, [inc]):
                    return rule.type_key
    return None
