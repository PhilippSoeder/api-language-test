import json
import os
import time
from typing import Optional

DATA_PREFIX = os.environ.get("DATA_PREFIX", "data")
I18N_PREFIX = os.environ.get("I18N_PREFIX", "i18n")
DEFAULT_LANG = os.environ.get("DEFAULT_LANG", "en")
DESC_CACHE_TTL_SECONDS = int(
    os.environ.get("DESC_CACHE_TTL_SECONDS", "1200")
)  # 20 min default
DATA_CACHE_TTL_SECONDS = int(
    os.environ.get("DATA_CACHE_TTL_SECONDS", "600")
)  # 10 min default
SUPPORTED_LANGS = set((os.environ.get("SUPPORTED_LANGS", "en,de,fr")).split(","))


# In-memory cache
# lang -> {"data": dict, "loaded_at": epoch}
_DESC_CACHE = {}


def negotiate_language(accept_language_header: str) -> str:
    """
    RFC 9110-ish parsing with q-weights; we only serve base langs, so try:
    exact -> base -> next candidate -> DEFAULT_LANG
    """
    if not accept_language_header:
        return DEFAULT_LANG

    # Parse q-values
    parts = [p.strip() for p in accept_language_header.split(",") if p.strip()]
    parsed = []
    for p in parts:
        if ";q=" in p:
            lang, q = p.split(";q=", 1)
            try:
                qv = float(q)
            except ValueError:
                qv = 1.0
        else:
            lang, qv = p, 1.0
        parsed.append((lang.lower(), qv))
    parsed.sort(key=lambda x: x[1], reverse=True)

    # Try candidates in order
    for lang, _ in parsed:
        cand = lang
        if cand in SUPPORTED_LANGS:
            return cand
        # Reduce region, e.g. "de-ch" -> "de"
        if "-" in cand:
            base = cand.split("-")[0]
            if base in SUPPORTED_LANGS:
                return base

    return DEFAULT_LANG


def _path_for_i18n(lang: str) -> str:
    return f"{I18N_PREFIX}/{lang}.json"


def _load_i18n_from_file(lang: str):
    key = _path_for_i18n(lang)
    with open(key) as f:
        data = json.load(f)
    return data


def get_descriptions(lang: str) -> dict:
    """
    Lazy-load with cache. Fallback order: requested -> base -> default.
    Cache by concrete lang actually loaded.
    """
    now = time.time()

    def cached_ok(entry):
        return entry and (now - entry["loaded_at"] < DESC_CACHE_TTL_SECONDS)

    tried = []

    # Try exact
    for candidate in (
        lang,
        lang.split("-")[0] if "-" in lang else None,
        DEFAULT_LANG,
    ):
        if not candidate or candidate in tried:
            continue
        tried.append(candidate)

        entry = _DESC_CACHE.get(candidate)
        if cached_ok(entry):
            return entry["data"]

        # Attempt to load from file
        try:
            data = _load_i18n_from_file(candidate)
            _DESC_CACHE[candidate] = {"data": data, "loaded_at": now}
            return data
        except FileNotFoundError:
            continue
        except Exception as e:
            # 404 or access issue; try next candidate
            if isinstance(e, FileNotFoundError):
                continue
            # For transient errors, don't poison cache; raise to surface 5xx
            raise

    # If we got here, nothing was available
    raise RuntimeError("No available descriptions for requested/default languages")


def _path_for_data(id: str) -> str:
    return f"{DATA_PREFIX}/{id}.json"


def _load_data_from_file(id: str):
    key = _path_for_data(id)
    with open(key) as f:
        data = json.load(f)
    return data


def get_data(id: str) -> Optional[dict]:
    """
    Lazy-load with cache.
    """
    now = time.time()

    def cached_ok(entry):
        return entry and (now - entry["loaded_at"] < DESC_CACHE_TTL_SECONDS)

    entry = _DESC_CACHE.get(id)
    if cached_ok(entry):
        return entry["data"]

    # Attempt to load from file
    try:
        data = _load_data_from_file(id)
        _DESC_CACHE[id] = {"data": data, "loaded_at": now}
        return data
    except FileNotFoundError:
        return None
    except Exception:
        # For transient errors, don't poison cache; raise to surface 5xx
        raise


def main(item_id, accept_language):
    if not item_id:
        return {"statusCode": 400, "body": json.dumps({"error": "missing id"})}

    lang = negotiate_language(accept_language)

    descriptions = get_descriptions(lang)
    data = get_data(item_id)

    if data is None:
        return {
            "statusCode": 404,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"code": 404, "message": "not found"}),
        }

    list_attr = []
    list_elements = data.get("data-list", [])
    for element in list_elements:
        if "key" not in element:
            continue
        key = element["key"]
        description = descriptions.get(key, "")
        value = element.get("value", "")
        list_attr.append({"key": key, "description": description, "value": value})

    response = {
        "id": item_id,
        "language": lang.split("-")[0] if "-" in lang else lang,
        "list-attribute": list_attr,
    }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(response, ensure_ascii=False),
    }


if __name__ == "__main__":
    # For local testing
    test_id = "id01"
    test_lang = "de-DE"
    result = main(test_id, test_lang)
    print(result)
