import re
from datetime import datetime, timezone


def clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


def clean_str_list(value):
    if not isinstance(value, list):
        return []
    return [clean_str(item) for item in value if clean_str(item)]


def parse_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def build_geo_point(lat, lng, precision="exact"):
    lat_val = parse_float(lat)
    lng_val = parse_float(lng)
    if lat_val is None or lng_val is None:
        return None
    if not (-90 <= lat_val <= 90 and -180 <= lng_val <= 180):
        return None
    return {
        "type": "Point",
        "coordinates": [lng_val, lat_val],
        "precision": clean_str(precision) or "exact",
    }


def parse_geo_point(raw):
    if isinstance(raw, dict):
        if raw.get("type") == "Point" and isinstance(raw.get("coordinates"), list) and len(raw.get("coordinates")) == 2:
            lng = parse_float(raw["coordinates"][0])
            lat = parse_float(raw["coordinates"][1])
            if lat is None or lng is None:
                return None
            return {
                "type": "Point",
                "coordinates": [lng, lat],
                "precision": clean_str(raw.get("precision")) or "exact",
            }
        lat = parse_float(raw.get("lat"))
        lng = parse_float(raw.get("lng"))
        if lat is not None and lng is not None:
            return build_geo_point(lat, lng, raw.get("precision") or "exact")
    if isinstance(raw, (list, tuple)) and len(raw) == 2:
        lat = parse_float(raw[0])
        lng = parse_float(raw[1])
        if lat is not None and lng is not None:
            return build_geo_point(lat, lng)
    if isinstance(raw, str):
        m = re.match(r"^\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*$", raw)
        if m:
            lat = parse_float(m.group(1))
            lng = parse_float(m.group(2))
            return build_geo_point(lat, lng)
    return None


def geo_to_coords_text(geo):
    parsed = parse_geo_point(geo)
    if not parsed:
        return ""
    lng, lat = parsed["coordinates"]
    return f"{lat:.6f}, {lng:.6f}"


def normalize_area_ref(raw):
    if raw is None:
        return None
    if isinstance(raw, str):
        text = clean_str(raw)
        if not text:
            return None
        return {
            "id": text if text.isdigit() else None,
            "source": "manual",
            "city": "",
            "region": "",
            "countryCode": "UA",
            "display": text,
        }
    if not isinstance(raw, dict):
        return None

    area_id = clean_str(raw.get("id") or raw.get("areaId") or raw.get("geonameId"))
    city = clean_str(raw.get("city") or raw.get("name"))
    region = clean_str(raw.get("region") or raw.get("adminName1"))
    country_code = clean_str(raw.get("countryCode") or raw.get("country") or "UA") or "UA"
    display = clean_str(raw.get("display"))
    if not display:
        parts = [part for part in [city, region, "Україна" if country_code == "UA" else country_code] if part]
        display = ", ".join(parts)
    inferred_source = "manual"
    if area_id:
        if re.match(r"^\d+$", area_id):
            inferred_source = "geonames"
        elif re.match(r"^(node|way|relation):\d+$", area_id, flags=re.IGNORECASE):
            inferred_source = "locationiq"
    source = clean_str(raw.get("source") or inferred_source) or "manual"

    if not any([area_id, city, region, display]):
        return None

    return {
        "id": area_id or None,
        "source": source,
        "city": city,
        "region": region,
        "countryCode": country_code,
        "display": display,
    }


def normalize_address_ref(raw):
    if raw is None:
        return None
    if isinstance(raw, str):
        text = clean_str(raw)
        if not text:
            return None
        return {
            "raw": text,
            "display": text,
            "placeId": None,
            "provider": "manual",
        }
    if not isinstance(raw, dict):
        return None

    raw_text = clean_str(raw.get("raw") or raw.get("address"))
    display = clean_str(raw.get("display") or raw_text)
    place_id = clean_str(raw.get("placeId") or raw.get("id")) or None
    provider = clean_str(raw.get("provider") or "manual") or "manual"

    if not raw_text and not display:
        return None

    return {
        "raw": raw_text or display,
        "display": display or raw_text,
        "placeId": place_id,
        "provider": provider,
    }


def _tokenize(text):
    if not text:
        return []
    found = re.findall(r"[\w\-']+", text.lower(), flags=re.UNICODE)
    seen = set()
    out = []
    for token in found:
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def build_search_tokens(*parts):
    tokens = []
    seen = set()
    for part in parts:
        for token in _tokenize(clean_str(part)):
            if token in seen:
                continue
            seen.add(token)
            tokens.append(token)
    return tokens


def infer_quality(area, address, geo):
    if area and address and geo:
        return "full"
    if area and not address and not geo:
        return "area_only"
    if address and not area and not geo:
        return "address_only"
    if geo and not area and not address:
        return "geo_only"
    if area or address or geo:
        return "full"
    return "unknown"


def make_display(area, address, geo):
    area_display = clean_str((area or {}).get("display")) if isinstance(area, dict) else ""
    address_display = clean_str((address or {}).get("display")) if isinstance(address, dict) else ""

    if area_display and address_display:
        if address_display.lower() in area_display.lower() or area_display.lower() in address_display.lower():
            return address_display if len(address_display) >= len(area_display) else area_display
        return f"{area_display}, {address_display}"
    if address_display:
        return address_display
    if area_display:
        return area_display

    geo_parsed = parse_geo_point(geo)
    if geo_parsed:
        lng, lat = geo_parsed["coordinates"]
        return f"{lat:.6f}, {lng:.6f}"
    return ""


def normalize_location_core(raw):
    if not isinstance(raw, dict):
        return {
            "area": None,
            "address": None,
            "geo": None,
            "display": "",
            "quality": "unknown",
            "searchTokens": [],
            "updatedAt": now_iso(),
        }

    area = normalize_area_ref(raw.get("area"))
    address = normalize_address_ref(raw.get("address"))
    geo = parse_geo_point(raw.get("geo"))

    # Accept fallback top-level fields.
    if not area:
        area = normalize_area_ref({
            "id": raw.get("areaId"),
            "display": raw.get("area"),
            "city": raw.get("city"),
            "region": raw.get("region"),
            "source": raw.get("source") or raw.get("areaSource"),
        })
    if not address:
        address = normalize_address_ref(raw.get("addressLine") or raw.get("addressText") or raw.get("addressRaw"))
    if not geo:
        geo = parse_geo_point(raw.get("coords") or raw.get("coordinates") or raw.get("location"))

    display = clean_str(raw.get("display")) or make_display(area, address, geo)
    quality = clean_str(raw.get("quality")) or infer_quality(area, address, geo)
    search_tokens = raw.get("searchTokens") if isinstance(raw.get("searchTokens"), list) else None
    if search_tokens is None:
        search_tokens = build_search_tokens(
            display,
            (area or {}).get("city") if area else "",
            (area or {}).get("region") if area else "",
            (address or {}).get("raw") if address else "",
            (address or {}).get("display") if address else "",
        )

    updated_at = clean_str(raw.get("updatedAt")) or now_iso()

    return {
        "area": area,
        "address": address,
        "geo": geo,
        "display": display,
        "quality": quality,
        "searchTokens": search_tokens,
        "updatedAt": updated_at,
    }
