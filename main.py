import json
import os
import time
import re
import threading
import uuid
from typing import Optional, Dict, Any, List, Tuple

# --- Config ---
# Directories for data and i18n files (relative to working dir)
DATA_PREFIX = os.environ.get("DATA_PREFIX", "data")
I18N_PREFIX = os.environ.get("I18N_PREFIX", "i18n")

# Default language (if no Accept-Language or no match)
DEFAULT_LANG = os.environ.get("DEFAULT_LANG", "en")
# Supported languages (comma-separated, e.g., "en,de,fr")
# Note: DEFAULT_LANG should be in this set.
SUPPORTED_LANGS = set((os.environ.get("SUPPORTED_LANGS", "en,de,fr")).split(","))

# Optional canonical ordering for output; comma-separated keys in desired order
# example: "key2,key1,key3"
CANON_KEYS = [k for k in os.environ.get("CANON_KEYS", "").split(",") if k]

# Units file (single global file, language-agnostic)
UNITS_FILE = os.environ.get("UNITS_FILE", "units/units.json")

# --- Limits ---
# Limits for Accept-Language parsing
MAX_LANG_HEADER_LEN = int(os.environ.get("MAX_LANG_HEADER_LEN", "512"))
MAX_LANG_ENTRIES = int(os.environ.get("MAX_LANG_ENTRIES", "12"))
# Cache TTLs (in seconds)
DESC_CACHE_TTL_SECONDS = int(os.environ.get("DESC_CACHE_TTL_SECONDS", "1200"))  # 20 min
DATA_CACHE_TTL_SECONDS = int(os.environ.get("DATA_CACHE_TTL_SECONDS", "600"))  # 10 min
UNITS_CACHE_TTL_SECONDS = int(os.environ.get("UNITS_CACHE_TTL_SECONDS", "86400"))  # 24 h
# Bounded cache sizes (reasonable defaults)
MAX_DATA_CACHE_ENTRIES = int(os.environ.get("MAX_DATA_CACHE_ENTRIES", "1000"))
MAX_MISS_CACHE_ENTRIES = int(os.environ.get("MAX_MISS_CACHE_ENTRIES", "5000"))

# --- Caches (survive warm invocations) ---
# We store loaded_at_mono from time.monotonic() for robust TTL math.
# lang -> {"data": dict, "loaded_at_mono": float}
_DESC_CACHE: Dict[str, Dict[str, Any]] = {}
# id -> {"data_map": dict, "loaded_at_mono": float}
_DATA_CACHE: Dict[str, Dict[str, Any]] = {}
# singleton -> {"units": dict, "loaded_at_mono": float}
_UNITS_CACHE: Dict[str, Any] = {}
# negative cache for missing IDs: id -> expires_at_mono (float)
_DATA_MISS_CACHE: Dict[str, float] = {}

# --- Locks (thread-safety for cache writes) ---
# Lambda typically runs single-threaded per runtime, but web servers / tests might not.
_DESC_LOCK = threading.Lock()
_DATA_LOCK = threading.Lock()
_UNITS_LOCK = threading.Lock()
_MISS_LOCK = threading.Lock()

# Response caching control
# e.g., "no-store", or "public, max-age=300"
RESPONSE_CACHE_CONTROL = os.environ.get("RESPONSE_CACHE_CONTROL", "public, max-age=300")

# --- ID validation ---
# exactly 4 characters, lowercase a-z or digits 0-9
_ID_RE = re.compile(r"^[a-z0-9]{4}$")


def _validate_item_id(item_id: str) -> None:
    """
    Enforce security + correctness:
      - exactly 4 chars
      - only [a-z0-9]
    Blocks path traversal and malformed names by construction.
    """
    if not isinstance(item_id, str) or not _ID_RE.fullmatch(item_id):
        raise ValueError("invalid id: must be exactly 4 chars [a-z0-9]")


# --- Shared cache helper ---
def cache_fresh(entry: Optional[dict], ttl_seconds: int, now_mono: float) -> bool:
    """
    Return True if cache entry exists and is still fresh using a monotonic clock.
    Why monotonic? Wall clocks can jump (NTP/DST/manual changes); monotonic only increases,
    so TTL comparisons remain correct.
    """
    return bool(entry) and (now_mono - entry.get("loaded_at_mono", -1e18) < ttl_seconds)


def _bounded_put(
    cache: Dict[str, Any], key: str, value: Any, max_entries: int, lock: threading.Lock
):
    """
    Insert into a dict-bounded cache; evict an arbitrary oldest entry when full.
    This avoids unbounded growth in long-lived Lambda execution environments.
    """
    with lock:
        if len(cache) >= max_entries:
            cache.pop(next(iter(cache)))
        cache[key] = value


def negotiate_language(accept_language_header: str) -> str:
    """
    Parse Accept-Language with q-values; try exact -> base -> DEFAULT_LANG.
    Keep client order for equal q-values.
    Honor q=0 as 'unacceptable' and skip such entries (RFC-consistent).
    """
    if not accept_language_header:
        return DEFAULT_LANG

    h = accept_language_header[:MAX_LANG_HEADER_LEN]
    parts = [p.strip() for p in h.split(",") if p.strip()][:MAX_LANG_ENTRIES]

    parsed: List[Tuple[str, float, int]] = []
    for idx, p in enumerate(parts):
        if ";q=" in p:
            lang, q = p.split(";q=", 1)
            try:
                qv = float(q)
            except ValueError:
                qv = 1.0
        else:
            lang, qv = p, 1.0
        parsed.append((lang.lower(), qv, idx))

    # sort by q DESC, then original position ASC (stable tie-break)
    parsed.sort(key=lambda x: (-x[1], x[2]))

    for lang, qv, _ in parsed:
        if qv <= 0.0:
            # q=0 means the client explicitly *does not* accept this language.
            continue
        if lang in SUPPORTED_LANGS:
            return lang
        if "-" in lang:
            base = lang.split("-")[0]
            if base in SUPPORTED_LANGS:
                return base
    return DEFAULT_LANG


def _normalize_lang_for_header(lang: str) -> str:
    """Return the language code suitable for Content-Language (prefer base)."""
    return lang.split("-")[0] if "-" in lang else lang


def _std_headers(lang: str) -> Dict[str, str]:
    """
    Standard headers for all responses:
      - Content-Type: JSON
      - Content-Language: the language chosen (for clients/CDNs)
      - Vary: Accept-Language (so caches don't mix languages)
      - Cache-Control: configurable (default no-store)
    """
    return {
        "Content-Type": "application/json",
        "Content-Language": _normalize_lang_for_header(lang),
        "Vary": "Accept-Language",
        "Cache-Control": RESPONSE_CACHE_CONTROL,
    }


def _path_for_i18n(lang: str) -> str:
    return f"{I18N_PREFIX}/{lang}.json"


def _path_for_data(item_id: str) -> str:
    # ID validation blocks traversal and malformed names
    _validate_item_id(item_id)
    return f"{DATA_PREFIX}/{item_id}.json"


def _load_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# --- I18N (descriptions) ---
def get_descriptions(lang: str) -> dict:
    """
    Lazy-load descriptions with cache. Fallback: requested -> base -> default.
    Uses thread-safe writes and monotonic TTL checks.
    """
    now_mono = time.monotonic()

    tried = set()
    for candidate in (lang, lang.split("-")[0] if "-" in lang else None, DEFAULT_LANG):
        if not candidate or candidate in tried:
            continue
        tried.add(candidate)

        entry = _DESC_CACHE.get(candidate)
        if cache_fresh(entry, DESC_CACHE_TTL_SECONDS, now_mono):
            return entry["data"]

        try:
            data = _load_json_file(_path_for_i18n(candidate))
            if not isinstance(data, dict):
                raise ValueError(f"i18n file must be an object/map, got {type(data)}")
            # Lock around writes to avoid torn updates under concurrent access.
            with _DESC_LOCK:
                _DESC_CACHE[candidate] = {"data": data, "loaded_at_mono": now_mono}
            return data
        except FileNotFoundError:
            continue

    raise RuntimeError("No available descriptions for requested/default languages")


# --- Units (global, language-agnostic) ---
def get_units_map() -> dict:
    """
    Load units from a single JSON file, cache with its own TTL.
    File shape: { "key1": "m", "key2": "kg", ... }
    """
    now_mono = time.monotonic()
    entry = _UNITS_CACHE.get("units")
    if cache_fresh(entry, UNITS_CACHE_TTL_SECONDS, now_mono):
        return entry["units"]

    try:
        units = _load_json_file(UNITS_FILE)
        if not isinstance(units, dict):
            raise ValueError(f"units file must be an object/map, got {type(units)}")
        with _UNITS_LOCK:  # thread-safe write
            _UNITS_CACHE["units"] = {"units": units, "loaded_at_mono": now_mono}
        return units
    except FileNotFoundError:
        # No units file -> still respond; unit fields will be null
        with _UNITS_LOCK:
            _UNITS_CACHE["units"] = {"units": {}, "loaded_at_mono": now_mono}
        return {}


# --- Data per ID (with negative cache + bounded caches) ---
def get_data_map(item_id: str) -> Optional[dict]:
    """
    Load per-ID data as a map { key: value }.
    File shape: { "id": "...", "data": { ... } }
    Negative-cache 404s to avoid repeated disk hits.
    """
    now_mono = time.monotonic()

    # Negative cache: if we recently saw a miss for this id, return fast
    miss_exp = _DATA_MISS_CACHE.get(item_id)
    if miss_exp and miss_exp > now_mono:
        return None

    entry = _DATA_CACHE.get(item_id)
    if cache_fresh(entry, DATA_CACHE_TTL_SECONDS, now_mono):
        return entry["data_map"]

    try:
        raw = _load_json_file(_path_for_data(item_id))
        if not isinstance(raw, dict):
            raise ValueError(f"data file must be an object/map, got {type(raw)}")

        data_map = raw.get("data") or {}
        if not isinstance(data_map, dict):
            raise ValueError(f"'data' must be an object/map, got {type(data_map)}")

        _bounded_put(
            _DATA_CACHE,
            item_id,
            {"data_map": data_map, "loaded_at_mono": now_mono},
            MAX_DATA_CACHE_ENTRIES,
            _DATA_LOCK,
        )
        # clear negative cache on hit
        if item_id in _DATA_MISS_CACHE:
            with _MISS_LOCK:
                _DATA_MISS_CACHE.pop(item_id, None)
        return data_map

    except FileNotFoundError:
        # remember miss briefly (min(60s, DATA_CACHE_TTL_SECONDS))
        _bounded_put(
            _DATA_MISS_CACHE,
            item_id,
            now_mono + min(60, DATA_CACHE_TTL_SECONDS),
            MAX_MISS_CACHE_ENTRIES,
            _MISS_LOCK,
        )
        return None


def _determine_output_keys(
    descriptions: dict, values_map: dict, units_map: dict
) -> List[str]:
    """
    Decide which keys to output and in what order.
    Priority:
      1) CANON_KEYS (if provided)
      2) All description keys, then any extra keys from values_map, then any extra from units_map
         (stable, alphabetical within each group)
    """
    if CANON_KEYS:
        return CANON_KEYS

    desc_keys = set(descriptions.keys())
    val_keys = set(values_map.keys())
    unit_keys = set(units_map.keys())
    first = sorted(desc_keys)
    second = sorted(val_keys - desc_keys)
    third = sorted(unit_keys - desc_keys - val_keys)
    return first + second + third


def _error_response(
    status: int,
    lang: str,
    code: int,
    message: str,
    correlation_id: Optional[str] = None,
):
    payload = {"code": code, "message": message}
    if correlation_id:
        payload["correlation_id"] = correlation_id
    return {
        "statusCode": status,
        "headers": _std_headers(lang),
        "body": json.dumps(payload, ensure_ascii=False),
    }


def main(item_id: str, accept_language: str):
    # Choose a language early (for headers even on errors)
    lang = negotiate_language(accept_language)

    # 500 guard: catch-all to avoid leaking stack traces to clients
    try:
        # Validate ID; return 400 on invalid
        try:
            _validate_item_id(item_id)
        except ValueError as e:
            return _error_response(400, lang, 400, str(e))

        descriptions = get_descriptions(lang)
        units_map = get_units_map()
        values_map = get_data_map(item_id)

        if values_map is None:
            return _error_response(404, lang, 404, "not found")

        keys = _determine_output_keys(descriptions, values_map, units_map)

        list_attr = []
        for k in keys:
            list_attr.append(
                {
                    "key": k,
                    "description": descriptions.get(k),  # None if missing in language
                    "value": values_map.get(k),  # None if missing for this id
                    "unit": units_map.get(k),  # None if no unit defined
                }
            )

        response = {
            "id": item_id,
            "language": _normalize_lang_for_header(lang),
            "list-attribute": list_attr,
        }

        return {
            "statusCode": 200,
            "headers": _std_headers(lang),
            "body": json.dumps(response, ensure_ascii=False),
        }

    except Exception as ex:
        # Log the exception server-side (visible in CloudWatch for Lambda)
        correlation_id = str(uuid.uuid4())
        print(f"[ERROR] correlation_id={correlation_id} item_id={item_id} lang={lang} error={ex}")
        # Generic 500 to client with correlation id
        return _error_response(500, lang, 500, "internal server error", correlation_id)


if __name__ == "__main__":
    # Local test
    print(main("id01", "de-DE,de;q=0.9,en;q=0.8"))
