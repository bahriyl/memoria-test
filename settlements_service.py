import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from location_schema import clean_str, parse_float


DEFAULT_SETTLEMENTS_DATASET_PATH = os.path.join(
    os.path.dirname(__file__), "data", "ukraine_settlements_with_population_latlon_2026-05-05.json"
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def norm_text(value: Any) -> str:
    text = clean_str(value).lower()
    if not text:
        return ""
    text = re.sub(r"^[\s,.-]*(м\.|с\.|смт|місто|село|селище)\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def build_search_text(payload: Dict[str, Any]) -> str:
    parts = [
        payload.get("name"),
        payload.get("type"),
        payload.get("region"),
        payload.get("district"),
        payload.get("hromada"),
        payload.get("katottg_code"),
    ]
    return " ".join([clean_str(item) for item in parts if clean_str(item)])


def normalize_source_row(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    katottg = clean_str(raw.get("k"))
    name = clean_str(raw.get("n"))
    if not katottg or not name:
        return None

    lat = parse_float(raw.get("lat"))
    lon = parse_float(raw.get("lon"))
    location = None
    if lat is not None and lon is not None:
        location = {"type": "Point", "coordinates": [lon, lat]}

    payload = {
        "katottg_code": katottg,
        "name": name,
        "type": clean_str(raw.get("t")),
        "region": clean_str(raw.get("r")),
        "district": clean_str(raw.get("d")),
        "hromada": clean_str(raw.get("g")),
        "population": raw.get("population"),
        "population_source": clean_str(raw.get("population_source")) or None,
        "lat": lat,
        "lon": lon,
        "location": location,
        "osm_id": clean_str(raw.get("osm_id")) or None,
        "coord_source": clean_str(raw.get("coord_source")) or None,
        "coord_updated_at": clean_str(raw.get("coord_updated_at")) or None,
        "normalized_name": norm_text(name),
        "search_text": "",
        "source_compact": {
            "n": clean_str(raw.get("n")),
            "t": clean_str(raw.get("t")),
            "k": katottg,
            "r": clean_str(raw.get("r")),
            "d": clean_str(raw.get("d")),
            "g": clean_str(raw.get("g")),
        },
    }
    payload["search_text"] = build_search_text(payload)
    return payload


def serialize_settlement(doc: Dict[str, Any]) -> Dict[str, Any]:
    location = doc.get("location") if isinstance(doc.get("location"), dict) else None
    coords = location.get("coordinates") if isinstance(location, dict) else None
    lon = parse_float(doc.get("lon"))
    lat = parse_float(doc.get("lat"))
    if (lat is None or lon is None) and isinstance(coords, list) and len(coords) == 2:
        lon = parse_float(coords[0])
        lat = parse_float(coords[1])

    return {
        "katottg_code": clean_str(doc.get("katottg_code")),
        "name": clean_str(doc.get("name")),
        "type": clean_str(doc.get("type")),
        "region": clean_str(doc.get("region")),
        "district": clean_str(doc.get("district")),
        "hromada": clean_str(doc.get("hromada")),
        "population": doc.get("population"),
        "population_source": clean_str(doc.get("population_source")) or None,
        "lat": lat,
        "lon": lon,
        "location": {"type": "Point", "coordinates": [lon, lat]} if lat is not None and lon is not None else None,
        "osm_id": clean_str(doc.get("osm_id")) or None,
        "coord_source": clean_str(doc.get("coord_source")) or None,
        "coord_updated_at": doc.get("coord_updated_at"),
        "source_compact": doc.get("source_compact") if isinstance(doc.get("source_compact"), dict) else None,
    }


def load_source_dataset(path: Optional[str] = None) -> List[Dict[str, Any]]:
    source_path = clean_str(path) or clean_str(os.environ.get("SETTLEMENTS_DATASET_PATH")) or DEFAULT_SETTLEMENTS_DATASET_PATH
    with open(source_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ValueError("Settlements dataset must be an array")
    rows: List[Dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        normalized = normalize_source_row(item)
        if normalized:
            rows.append(normalized)
    return rows
