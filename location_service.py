import math
import re
import threading
import time

import requests

from location_mapper import normalize_cemetery_location
from location_schema import clean_str, normalize_area_ref, normalize_location_core, parse_float, parse_geo_point

_SETTLEMENT_TYPES = {"city", "town", "village", "hamlet", "municipality"}

_QUERY_CACHE = {}
_QUERY_CACHE_LOCK = threading.Lock()


def _cache_get(key):
    now = time.time()
    with _QUERY_CACHE_LOCK:
        item = _QUERY_CACHE.get(key)
        if not item:
            return None
        expires_at, value = item
        if expires_at < now:
            _QUERY_CACHE.pop(key, None)
            return None
        return value


def _cache_set(key, value, ttl_seconds):
    with _QUERY_CACHE_LOCK:
        _QUERY_CACHE[key] = (time.time() + max(int(ttl_seconds), 1), value)


def _to_osm_ref(osm_type, osm_id):
    item_type = clean_str(osm_type).lower()
    item_id = clean_str(osm_id)
    if not item_type or not item_id:
        return ""
    if item_type not in {"node", "way", "relation"}:
        return ""
    return f"{item_type}:{item_id}"


def _infer_area_source(area_id):
    value = clean_str(area_id)
    if not value:
        return "manual"
    if re.match(r"^\d+$", value):
        return "geonames"
    if re.match(r"^(node|way|relation):\d+$", value, flags=re.IGNORECASE):
        return "locationiq"
    return "manual"


def _extract_city(address):
    if not isinstance(address, dict):
        return ""
    for key in (
        "city",
        "city_district",
        "locality",
        "town",
        "borough",
        "municipality",
        "village",
        "hamlet",
        "quarter",
        "neighbourhood",
        "suburb",
    ):
        value = clean_str(address.get(key))
        if value:
            return value
    return ""


def _extract_region(address):
    if not isinstance(address, dict):
        return ""
    for key in ("state", "county", "region"):
        value = clean_str(address.get(key))
        if value:
            return value
    return ""


def _extract_country_code(address, default_country="UA"):
    if isinstance(address, dict):
        code = clean_str(address.get("country_code") or address.get("countryCode") or address.get("country"))
        if code:
            return code.upper()
    default_clean = clean_str(default_country)
    return default_clean.upper() if default_clean else "UA"


def _display_country_name(country_code):
    return "Україна" if clean_str(country_code).upper() == "UA" else clean_str(country_code).upper()


def _build_area_ref(city, region, country_code, area_id, source="locationiq"):
    return normalize_area_ref({
        "id": area_id,
        "source": source,
        "city": clean_str(city),
        "region": clean_str(region),
        "countryCode": _extract_country_code({"country_code": country_code}),
        "display": ", ".join(
            [
                part
                for part in [
                    clean_str(city),
                    clean_str(region),
                    _display_country_name(country_code),
                ]
                if part
            ]
        ),
    })


def _to_country_codes_param(country_codes):
    if isinstance(country_codes, str):
        values = [part.strip().lower() for part in country_codes.split(",") if part.strip()]
    elif isinstance(country_codes, (list, tuple, set)):
        values = [clean_str(part).lower() for part in country_codes if clean_str(part)]
    else:
        values = []
    if not values:
        return "ua"
    seen = set()
    unique = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return ",".join(unique)


def _locationiq_base(region):
    region_clean = clean_str(region).lower()
    if region_clean in {"eu", "eu1"}:
        return "https://eu1.locationiq.com/v1"
    if region_clean in {"us", "us1"}:
        return "https://us1.locationiq.com/v1"
    return "https://api.locationiq.com/v1"


def _http_get_json(url, params, timeout=5, retries=1):
    params = params if isinstance(params, dict) else {}
    last_error = None
    for attempt in range(max(int(retries), 0) + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            if response.status_code in {429, 500, 502, 503, 504} and attempt < retries:
                time.sleep(0.2 * (attempt + 1))
                continue
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(0.2 * (attempt + 1))
                continue
            break
    if last_error:
        return None
    return None


def _locationiq_request(path, params, api_key, region="eu1", timeout=5, retries=1):
    token = clean_str(api_key)
    if not token:
        return None
    query = dict(params or {})
    query["key"] = token
    url = f"{_locationiq_base(region).rstrip('/')}/{clean_str(path).lstrip('/')}"
    return _http_get_json(url, query, timeout=timeout, retries=retries)


def _locationiq_geo(item):
    if not isinstance(item, dict):
        return None
    return parse_geo_point({
        "lat": item.get("lat"),
        "lng": item.get("lon") if item.get("lon") is not None else item.get("lng"),
    })


def _is_settlement_item(item):
    if not isinstance(item, dict):
        return False
    item_type = clean_str(item.get("type")).lower()
    item_class = clean_str(item.get("class")).lower()
    if item_type in _SETTLEMENT_TYPES:
        return True
    if item_class == "place" and item_type in {"city", "town", "village", "hamlet", "municipality", "county"}:
        return True
    return False


def _build_area_from_item(item, fallback_city="", fallback_region="", fallback_country="UA"):
    if not isinstance(item, dict):
        return None
    address = item.get("address") if isinstance(item.get("address"), dict) else {}
    city = clean_str(fallback_city) or _extract_city(address)
    region = clean_str(fallback_region) or _extract_region(address)
    country_code = clean_str(fallback_country) or _extract_country_code(address, fallback_country)
    area_id = _to_osm_ref(item.get("osm_type"), item.get("osm_id"))
    if not area_id:
        return None
    return _build_area_ref(city, region, country_code, area_id, source="locationiq")


def _pick_settlement_candidate(candidates, city, country_code):
    target_city = clean_str(city).lower()
    target_country = clean_str(country_code).lower()
    best = None
    best_score = -1
    for item in candidates if isinstance(candidates, list) else []:
        if not isinstance(item, dict):
            continue
        area_id = _to_osm_ref(item.get("osm_type"), item.get("osm_id"))
        if not area_id:
            continue
        address = item.get("address") if isinstance(item.get("address"), dict) else {}
        item_city = _extract_city(address)
        item_country = _extract_country_code(address, "").lower()
        score = 0
        if _is_settlement_item(item):
            score += 2
        if target_city and clean_str(item_city).lower() == target_city:
            score += 3
        if target_country and item_country == target_country:
            score += 1
        if score > best_score:
            best = item
            best_score = score
    return best


def _search_settlement_by_city(city, region_name, country_code, api_key, region, language):
    city_clean = clean_str(city)
    if not city_clean:
        return None

    cache_key = f"city::{city_clean.lower()}::{clean_str(region_name).lower()}::{clean_str(country_code).lower()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    q_parts = [city_clean]
    if clean_str(region_name):
        q_parts.append(clean_str(region_name))
    if clean_str(country_code):
        q_parts.append(clean_str(country_code).upper())

    params = {
        "q": ", ".join(q_parts),
        "format": "json",
        "addressdetails": 1,
        "normalizeaddress": 1,
        "limit": 6,
        "countrycodes": _to_country_codes_param(country_code or "UA"),
    }
    if clean_str(language):
        params["accept-language"] = clean_str(language)

    data = _locationiq_request("search", params, api_key=api_key, region=region, timeout=5, retries=1)
    candidate = _pick_settlement_candidate(data, city_clean, country_code)
    area = None
    if candidate:
        address = candidate.get("address") if isinstance(candidate.get("address"), dict) else {}
        area = _build_area_ref(
            _extract_city(address) or city_clean,
            _extract_region(address) or clean_str(region_name),
            _extract_country_code(address, country_code or "UA"),
            _to_osm_ref(candidate.get("osm_type"), candidate.get("osm_id")),
            source="locationiq",
        )

    _cache_set(cache_key, area, ttl_seconds=3600)
    return area


def _reverse_settlement_by_geo(lat, lng, api_key, region, language, country_code):
    geo_key = f"reverse::{round(float(lat), 4)}::{round(float(lng), 4)}"
    cached = _cache_get(geo_key)
    if cached is not None:
        return cached

    params = {
        "lat": lat,
        "lon": lng,
        "format": "json",
        "addressdetails": 1,
        "normalizeaddress": 1,
        "zoom": 10,
    }
    if clean_str(language):
        params["accept-language"] = clean_str(language)

    data = _locationiq_request("reverse", params, api_key=api_key, region=region, timeout=5, retries=1)
    area = None
    if isinstance(data, dict):
        address = data.get("address") if isinstance(data.get("address"), dict) else {}
        area_id = _to_osm_ref(data.get("osm_type"), data.get("osm_id"))
        if area_id and _is_settlement_item(data):
            area = _build_area_ref(
                _extract_city(address),
                _extract_region(address),
                _extract_country_code(address, country_code),
                area_id,
                source="locationiq",
            )
        if not area:
            city = _extract_city(address)
            if city:
                area = _search_settlement_by_city(
                    city,
                    _extract_region(address),
                    _extract_country_code(address, country_code),
                    api_key=api_key,
                    region=region,
                    language=language,
                )

    _cache_set(geo_key, area, ttl_seconds=900)
    return area


def _resolve_settlement_area(item, api_key, region, language, country_codes):
    if not isinstance(item, dict):
        return None
    address = item.get("address") if isinstance(item.get("address"), dict) else {}
    country_code = _extract_country_code(address, clean_str(country_codes).split(",")[0] if clean_str(country_codes) else "UA")

    if _is_settlement_item(item):
        area = _build_area_from_item(item, fallback_country=country_code)
        if area:
            return area

    city = _extract_city(address)
    region_name = _extract_region(address)
    if city:
        by_city = _search_settlement_by_city(
            city,
            region_name,
            country_code,
            api_key=api_key,
            region=region,
            language=language,
        )
        if by_city:
            return by_city

    geo = _locationiq_geo(item)
    if geo and isinstance(geo.get("coordinates"), list) and len(geo.get("coordinates")) == 2:
        lng, lat = geo["coordinates"]
        by_reverse = _reverse_settlement_by_geo(
            lat,
            lng,
            api_key=api_key,
            region=region,
            language=language,
            country_code=country_code,
        )
        if by_reverse:
            return by_reverse

    return None


def _dedupe_by_key(items, key_fn):
    out = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        key = key_fn(item)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def geonames_search_areas(search, geonames_user, geonames_lang="uk", country="UA", max_rows=10):
    search = clean_str(search)
    if not search:
        return []

    try:
        url = (
            "https://secure.geonames.org/searchJSON?"
            f"name_startsWith={requests.utils.quote(search)}"
            f"&country={country}"
            "&featureClass=P"
            f"&maxRows={int(max_rows)}"
            f"&lang={geonames_lang}"
            f"&username={geonames_user}"
        )
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        geonames = data.get("geonames") or []
    except Exception:
        return []

    seen = set()
    out = []
    for place in geonames:
        geoname_id = clean_str(place.get("geonameId"))
        if not geoname_id or geoname_id in seen:
            continue
        seen.add(geoname_id)

        area = normalize_area_ref({
            "id": geoname_id,
            "city": clean_str(place.get("name")),
            "region": clean_str(place.get("adminName1")),
            "countryCode": clean_str(place.get("countryCode")) or country,
            "display": ", ".join(
                [
                    part
                    for part in [
                        clean_str(place.get("name")),
                        clean_str(place.get("adminName1")),
                        clean_str(place.get("countryName")),
                    ]
                    if part
                ]
            ),
            "source": "geonames",
        })
        geo = parse_geo_point({"lat": place.get("lat"), "lng": place.get("lng")})

        out.append({
            "id": geoname_id,
            "display": (area or {}).get("display") or "",
            "lat": (geo or {}).get("coordinates", [None, None])[1] if geo else None,
            "lng": (geo or {}).get("coordinates", [None, None])[0] if geo else None,
            "area": area,
            "source": "geonames",
        })
    return out


def locationiq_search_areas(search, api_key, region="eu1", language="uk", country_codes="UA", max_rows=10):
    search = clean_str(search)
    if not search or not clean_str(api_key):
        return []

    params = {
        "q": search,
        "limit": max(int(max_rows), 1),
        "countrycodes": _to_country_codes_param(country_codes),
        "normalizecity": 1,
        "layers": "city",
    }
    if clean_str(language):
        params["accept-language"] = clean_str(language)

    items = _locationiq_request("autocomplete", params, api_key=api_key, region=region, timeout=5, retries=1)
    if not isinstance(items, list):
        items = []

    out = []
    for item in items:
        area = _build_area_from_item(item, fallback_country=clean_str(country_codes).split(",")[0] or "UA")
        geo = _locationiq_geo(item)
        if not area or not area.get("id") or not geo:
            continue
        out.append({
            "id": clean_str(area.get("id")),
            "display": clean_str(area.get("display")),
            "lat": geo.get("coordinates", [None, None])[1],
            "lng": geo.get("coordinates", [None, None])[0],
            "area": area,
            "source": "locationiq",
        })

    out = _dedupe_by_key(out, lambda x: x.get("id") or x.get("display"))
    return out[: max(int(max_rows), 1)]


def locationiq_search_addresses(search, api_key, region="eu1", language="uk", country_codes="UA", max_rows=10):
    search = clean_str(search)
    if not search or not clean_str(api_key):
        return []

    params = {
        "q": search,
        "limit": max(int(max_rows), 1),
        "countrycodes": _to_country_codes_param(country_codes),
        "normalizecity": 1,
        "layers": "road,neighbourhood,suburb,city,county,state,postcode",
    }
    if clean_str(language):
        params["accept-language"] = clean_str(language)

    items = _locationiq_request("autocomplete", params, api_key=api_key, region=region, timeout=5, retries=1)
    if not isinstance(items, list):
        items = []

    out = []
    for item in items:
        geo = _locationiq_geo(item)
        if not geo:
            continue

        area = _resolve_settlement_area(
            item,
            api_key=api_key,
            region=region,
            language=language,
            country_codes=country_codes,
        )
        if not area or not area.get("id"):
            continue

        selected_ref = _to_osm_ref(item.get("osm_type"), item.get("osm_id")) or clean_str(item.get("place_id"))
        address_display = clean_str(item.get("display_address") or item.get("display_name"))
        if not address_display:
            address = item.get("address") if isinstance(item.get("address"), dict) else {}
            address_display = ", ".join(
                [
                    part
                    for part in [
                        clean_str(address.get("house_number")),
                        clean_str(address.get("road")),
                        clean_str(address.get("city") or address.get("town") or address.get("village")),
                        clean_str(address.get("state")),
                        _display_country_name(_extract_country_code(address, "UA")),
                    ]
                    if part
                ]
            )
        display = address_display or clean_str(item.get("display_name")) or clean_str(area.get("display"))

        out.append({
            "id": clean_str(area.get("id")),
            "display": display,
            "lat": geo.get("coordinates", [None, None])[1],
            "lng": geo.get("coordinates", [None, None])[0],
            "area": area,
            "address": {
                "raw": address_display or display,
                "display": address_display or display,
                "placeId": selected_ref or None,
                "provider": "locationiq",
            },
            "placeId": selected_ref or None,
            "provider": "locationiq",
            "source": "locationiq",
        })

    out = _dedupe_by_key(out, lambda x: (x.get("placeId") or "", x.get("display") or "", x.get("id") or ""))
    return out[: max(int(max_rows), 1)]


def search_location_areas(
    search,
    provider="locationiq",
    locationiq_api_key="",
    locationiq_region="eu1",
    locationiq_language="uk",
    country_codes="UA",
    geonames_user="",
    geonames_lang="uk",
    max_rows=10,
):
    provider_name = clean_str(provider).lower() or "locationiq"

    if provider_name == "locationiq" and clean_str(locationiq_api_key):
        items = locationiq_search_areas(
            search,
            api_key=locationiq_api_key,
            region=locationiq_region,
            language=locationiq_language,
            country_codes=country_codes,
            max_rows=max_rows,
        )
        if items:
            return items

    return geonames_search_areas(
        search,
        geonames_user=geonames_user,
        geonames_lang=geonames_lang,
        country=clean_str(country_codes).split(",")[0].upper() or "UA",
        max_rows=max_rows,
    )


def search_location_addresses(
    search,
    provider="locationiq",
    locationiq_api_key="",
    locationiq_region="eu1",
    locationiq_language="uk",
    country_codes="UA",
    geonames_user="",
    geonames_lang="uk",
    max_rows=10,
):
    provider_name = clean_str(provider).lower() or "locationiq"

    if provider_name == "locationiq" and clean_str(locationiq_api_key):
        items = locationiq_search_addresses(
            search,
            api_key=locationiq_api_key,
            region=locationiq_region,
            language=locationiq_language,
            country_codes=country_codes,
            max_rows=max_rows,
        )
        if items:
            return items

    # Compatibility fallback when LocationIQ is unavailable.
    areas = geonames_search_areas(
        search,
        geonames_user=geonames_user,
        geonames_lang=geonames_lang,
        country=clean_str(country_codes).split(",")[0].upper() or "UA",
        max_rows=max_rows,
    )
    out = []
    for item in areas:
        display = clean_str(item.get("display"))
        area = normalize_area_ref(item.get("area") if isinstance(item.get("area"), dict) else {})
        out.append({
            "id": clean_str(item.get("id")),
            "display": display,
            "lat": item.get("lat"),
            "lng": item.get("lng"),
            "area": area,
            "address": {
                "raw": display,
                "display": display,
                "placeId": clean_str(item.get("id")) or None,
                "provider": "geonames",
            },
            "placeId": clean_str(item.get("id")) or None,
            "provider": "geonames",
            "source": "geonames",
        })
    return out


def normalize_location_input(payload):
    payload = payload if isinstance(payload, dict) else {}

    area_id = payload.get("areaId")
    area_payload = payload.get("area") if isinstance(payload.get("area"), dict) else {
        "id": area_id,
        "display": payload.get("areaDisplay") or payload.get("areaText") or payload.get("area"),
        "city": payload.get("city"),
        "region": payload.get("region"),
        "source": payload.get("areaSource") or _infer_area_source(area_id),
    }

    address_payload = payload.get("address") if isinstance(payload.get("address"), dict) else {
        "raw": payload.get("addressRaw") or payload.get("address") or payload.get("addressLine"),
        "display": payload.get("addressDisplay") or payload.get("address") or payload.get("addressLine"),
        "placeId": payload.get("placeId"),
        "provider": payload.get("provider") or "manual",
    }

    geo_payload = payload.get("geo")
    if geo_payload is None:
        geo_payload = {
            "lat": payload.get("lat") if payload.get("lat") is not None else payload.get("latitude"),
            "lng": payload.get("lng") if payload.get("lng") is not None else payload.get("longitude"),
        }

    return normalize_location_core({
        "area": normalize_area_ref(area_payload),
        "address": address_payload,
        "geo": geo_payload,
        "display": payload.get("display"),
        "quality": payload.get("quality"),
    })


def haversine_km(lat1, lng1, lat2, lng2):
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)

    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lng / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def filter_docs_by_radius(docs, origin_lat, origin_lng, radius_km, location_getter):
    lat0 = parse_float(origin_lat)
    lng0 = parse_float(origin_lng)
    radius = parse_float(radius_km)
    if lat0 is None or lng0 is None or radius is None:
        return docs

    out = []
    for doc in docs:
        location = location_getter(doc)
        geo = (location or {}).get("geo") if isinstance(location, dict) else None
        parsed_geo = parse_geo_point(geo)
        if not parsed_geo:
            continue
        lng, lat = parsed_geo["coordinates"]
        distance = haversine_km(lat0, lng0, lat, lng)
        if distance <= radius:
            out.append(doc)
    return out


def cemetery_option_from_doc(doc):
    location = normalize_cemetery_location(doc)
    area = location.get("area") or {}
    return {
        "id": str(doc.get("_id")),
        "name": clean_str(doc.get("name")),
        "area": clean_str(area.get("display") or area.get("city") or doc.get("locality")),
        "areaId": clean_str(area.get("id")),
        "location": location,
    }
