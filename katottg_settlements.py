import json
import os
import re
from threading import Lock


def _clean(value):
    if value is None:
        return ""
    return str(value).strip()


def _norm_text(value):
    text = _clean(value).lower()
    if not text:
        return ""
    text = re.sub(r"^[\s,.-]*(м\.|с\.|смт|місто|село|селище)\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _build_label(item):
    name = _clean(item.get("n"))
    district = _clean(item.get("d"))
    region = _clean(item.get("r"))
    tail = ", ".join([part for part in [district, region] if part])
    return f"{name}, {tail}" if tail else name


class KatottgSettlementsIndex:
    def __init__(self, items):
        self._items = []
        for raw in items:
            if not isinstance(raw, dict):
                continue
            name = _clean(raw.get("n"))
            katottg = _clean(raw.get("k"))
            if not name or not katottg:
                continue
            normalized = {
                "label": _build_label(raw),
                "name": name,
                "type": _clean(raw.get("t")),
                "katottg": katottg,
                "region": _clean(raw.get("r")),
                "district": _clean(raw.get("d")),
                "community": _clean(raw.get("g")),
            }
            normalized["_name_norm"] = _norm_text(normalized["name"])
            self._items.append(normalized)

    def search(self, query, limit=10, min_query_len=1):
        q = _clean(query)
        if len(q) < max(int(min_query_len), 1):
            return []
        norm_q = _norm_text(q)
        if not norm_q:
            return []

        ranked = []
        for item in self._items:
            name_n = item["_name_norm"]
            if name_n.startswith(norm_q):
                ranked.append(item)
        ranked.sort(key=lambda row: (_norm_text(row["name"]), _norm_text(row["district"]), _norm_text(row["region"])))

        out = []
        for item in ranked[: max(int(limit), 1)]:
            out.append(
                {
                    "label": item["label"],
                    "name": item["name"],
                    "type": item["type"],
                    "katottg": item["katottg"],
                    "region": item["region"],
                    "district": item["district"],
                    "community": item["community"],
                }
            )
        return out


_dataset_lock = Lock()
_dataset_index = None
_dataset_path = None


def _resolve_dataset_path():
    env_path = _clean(os.environ.get("KATOTTG_SETTLEMENTS_PATH"))
    if env_path:
        return env_path
    return os.path.join(os.path.dirname(__file__), "data", "ukraine_settlements_compact_2026-05-05.json")


def get_settlements_index(force_reload=False):
    global _dataset_index, _dataset_path
    with _dataset_lock:
        path = _resolve_dataset_path()
        if force_reload:
            _dataset_index = None
        if _dataset_index is not None and _dataset_path == path:
            return _dataset_index

        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, list):
            raise ValueError("KATOTTG settlements JSON must be an array")

        _dataset_index = KatottgSettlementsIndex(data)
        _dataset_path = path
        return _dataset_index


def search_settlements(query, limit=10, min_query_len=1):
    index = get_settlements_index()
    return index.search(query=query, limit=limit, min_query_len=min_query_len)


def normalize_ukrainian_text(value):
    return _norm_text(value)
