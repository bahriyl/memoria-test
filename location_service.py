import math
import re
import threading
import time

import requests

from location_mapper import normalize_cemetery_location
from location_schema import clean_str, normalize_area_ref, normalize_location_core, parse_float, parse_geo_point

_SETTLEMENT_TYPES = {"city", "town", "village", "hamlet", "municipality"}
_DEFAULT_GEOCODING_USER_AGENT = "memoria-geocoder/1.0 (+https://memoria.com.ua)"

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

    # Photon/Nominatim can return short types (N/W/R).
    short_map = {"n": "node", "w": "way", "r": "relation"}
    if item_type in short_map:
        item_type = short_map[item_type]

    if item_type not in {"node", "way", "relation"}:
        return ""
    return f"{item_type}:{item_id}"


def _parse_osm_ref(value):
    raw = clean_str(value)
    match = re.match(r"^(node|way|relation):(\d+)$", raw, flags=re.IGNORECASE)
    if not match:
        return None, None, ""
    item_type = match.group(1).lower()
    item_id = match.group(2)
    nominatim_type = {"node": "N", "way": "W", "relation": "R"}.get(item_type, "")
    return item_type, item_id, nominatim_type


def _infer_area_source(area_id):
    value = clean_str(area_id)
    if not value:
        return "manual"
    if re.match(r"^\d+$", value):
        return "geonames"
    if re.match(r"^(node|way|relation):\d+$", value, flags=re.IGNORECASE):
        return "photon"
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


def _extract_road(address):
    if not isinstance(address, dict):
        return ""
    for key in ("road", "street", "pedestrian", "residential", "footway", "path"):
        value = clean_str(address.get(key))
        if value:
            return value
    return ""


def _extract_house_number(address):
    if not isinstance(address, dict):
        return ""
    for key in ("house_number", "houseNumber", "housenumber"):
        value = clean_str(address.get(key))
        if value:
            return value
    return ""


def _display_country_name(country_code):
    return "Україна" if clean_str(country_code).upper() == "UA" else clean_str(country_code).upper()


def _build_area_ref(city, region, country_code, area_id, source="photon"):
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


def _to_country_codes(country_codes):
    if isinstance(country_codes, str):
        values = [part.strip().upper() for part in country_codes.split(",") if part.strip()]
    elif isinstance(country_codes, (list, tuple, set)):
        values = [clean_str(part).upper() for part in country_codes if clean_str(part)]
    else:
        values = []
    if not values:
        return ["UA"]
    seen = set()
    unique = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return unique


def _http_get_json(url, params=None, headers=None, timeout=5, retries=1):
    query = params if isinstance(params, dict) else {}
    req_headers = headers if isinstance(headers, dict) else {}
    last_error = None
    for attempt in range(max(int(retries), 0) + 1):
        try:
            response = requests.get(url, params=query, headers=req_headers, timeout=timeout)
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


def _resolve_user_agent(user_agent):
    return clean_str(user_agent) or _DEFAULT_GEOCODING_USER_AGENT


def _photon_request(base_url, params, user_agent="", timeout=5, retries=1):
    base = clean_str(base_url).rstrip("/")
    if not base:
        return None
    headers = {"User-Agent": _resolve_user_agent(user_agent)}
    url = f"{base}/api"
    query = dict(params or {})
    payload = _http_get_json(url, params=query, headers=headers, timeout=timeout, retries=retries)

    # Public Photon rejects unsupported `lang` values (e.g. `uk`) with an error payload.
    # Retry once without language to preserve availability.
    if query.get("lang") and (
        payload is None or (isinstance(payload, dict) and isinstance(payload.get("lang"), list))
    ):
        fallback_query = dict(query)
        fallback_query.pop("lang", None)
        fallback_payload = _http_get_json(url, params=fallback_query, headers=headers, timeout=timeout, retries=retries)
        if fallback_payload is not None:
            return fallback_payload

    return payload


def _nominatim_request(base_url, path, params, user_agent="", language="", timeout=5, retries=1):
    base = clean_str(base_url).rstrip("/")
    if not base:
        return None

    headers = {"User-Agent": _resolve_user_agent(user_agent)}

    query = dict(params or {})
    query.setdefault("format", "jsonv2")
    if clean_str(language):
        query.setdefault("accept-language", clean_str(language))

    url = f"{base}/{clean_str(path).lstrip('/')}"
    return _http_get_json(url, params=query, headers=headers, timeout=timeout, retries=retries)


def _is_country_allowed(country_code, allowed_codes):
    code = clean_str(country_code).upper()
    if not code:
        return False
    return code in set(_to_country_codes(allowed_codes))


def _build_display(*parts):
    return ", ".join([clean_str(part) for part in parts if clean_str(part)])


def _feature_geo(feature):
    if not isinstance(feature, dict):
        return None

    geometry = feature.get("geometry") if isinstance(feature.get("geometry"), dict) else {}
    coords = geometry.get("coordinates") if isinstance(geometry.get("coordinates"), list) else []
    if len(coords) == 2:
        return parse_geo_point({"lng": coords[0], "lat": coords[1]})

    props = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
    return parse_geo_point({"lat": props.get("lat"), "lng": props.get("lon")})


def _extract_photon_address_parts(feature):
    props = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}

    road = clean_str(props.get("street") or props.get("name"))
    house_number = clean_str(props.get("housenumber"))
    city = clean_str(props.get("city") or props.get("town") or props.get("village") or props.get("locality"))
    region = clean_str(props.get("state") or props.get("county"))
    postcode = clean_str(props.get("postcode"))
    country_code = clean_str(props.get("countrycode") or "UA").upper()

    return {
        "road": road,
        "house_number": house_number,
        "houseNumber": house_number,
        "city": city,
        "region": region,
        "postcode": postcode,
        "countryCode": country_code,
    }


def _is_settlement_feature(feature):
    props = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
    osm_key = clean_str(props.get("osm_key")).lower()
    osm_value = clean_str(props.get("osm_value")).lower()
    if osm_key == "place" and osm_value in _SETTLEMENT_TYPES:
        return True
    return osm_value in _SETTLEMENT_TYPES


def _photon_area_suggestion_from_feature(feature, country_codes):
    if not isinstance(feature, dict):
        return None
    if not _is_settlement_feature(feature):
        return None

    props = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
    country_code = clean_str(props.get("countrycode") or "UA").upper()
    if not _is_country_allowed(country_code, country_codes):
        return None

    area_id = _to_osm_ref(props.get("osm_type"), props.get("osm_id"))
    if not area_id:
        return None

    city = clean_str(props.get("name") or props.get("city") or props.get("town") or props.get("village"))
    region = clean_str(props.get("state") or props.get("county"))
    area = _build_area_ref(city, region, country_code, area_id, source="photon")
    geo = _feature_geo(feature)
    if not area or not geo:
        return None

    display = clean_str(area.get("display") or _build_display(city, region, _display_country_name(country_code)))
    return {
        "id": clean_str(area.get("id")),
        "display": display,
        "lat": geo.get("coordinates", [None, None])[1],
        "lng": geo.get("coordinates", [None, None])[0],
        "area": area,
        "source": "photon",
    }


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


def _lookup_nominatim_by_place(place_id, nominatim_base_url, user_agent, language, timeout_seconds, retries):
    _, item_id, nominatim_type = _parse_osm_ref(place_id)
    if not item_id or not nominatim_type:
        return None

    cache_key = f"nominatim:lookup:{nominatim_type}{item_id}:{clean_str(language).lower()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    payload = _nominatim_request(
        nominatim_base_url,
        "lookup",
        {
            "osm_ids": f"{nominatim_type}{item_id}",
            "addressdetails": 1,
            "namedetails": 1,
            "extratags": 1,
            "limit": 1,
        },
        user_agent=user_agent,
        language=language,
        timeout=timeout_seconds,
        retries=retries,
    )

    item = payload[0] if isinstance(payload, list) and payload else None
    _cache_set(cache_key, item, ttl_seconds=3600)
    return item


def _nominatim_reverse(lat, lng, nominatim_base_url, user_agent, language, timeout_seconds, retries, zoom=18):
    cache_key = f"nominatim:reverse:{round(float(lat), 5)}:{round(float(lng), 5)}:{int(zoom)}:{clean_str(language).lower()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    payload = _nominatim_request(
        nominatim_base_url,
        "reverse",
        {
            "lat": lat,
            "lon": lng,
            "addressdetails": 1,
            "zoom": int(zoom),
        },
        user_agent=user_agent,
        language=language,
        timeout=timeout_seconds,
        retries=retries,
    )

    item = payload if isinstance(payload, dict) else None
    _cache_set(cache_key, item, ttl_seconds=600)
    return item


def _nominatim_search(query, nominatim_base_url, user_agent, language, country_codes, timeout_seconds, retries, limit=1):
    query_clean = clean_str(query)
    if not query_clean:
        return []

    cache_key = f"nominatim:search:{query_clean.lower()}:{clean_str(language).lower()}:{','.join(_to_country_codes(country_codes))}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    payload = _nominatim_request(
        nominatim_base_url,
        "search",
        {
            "q": query_clean,
            "addressdetails": 1,
            "limit": max(int(limit), 1),
            "countrycodes": ",".join(code.lower() for code in _to_country_codes(country_codes)),
        },
        user_agent=user_agent,
        language=language,
        timeout=timeout_seconds,
        retries=retries,
    )

    items = payload if isinstance(payload, list) else []
    _cache_set(cache_key, items, ttl_seconds=900)
    return items


def _extract_address_parts_from_nominatim(item):
    address = item.get("address") if isinstance(item, dict) and isinstance(item.get("address"), dict) else {}
    road = _extract_road(address)
    house_number = _extract_house_number(address)
    city = _extract_city(address)
    region = _extract_region(address)
    postcode = clean_str(address.get("postcode"))
    country_code = _extract_country_code(address, "UA")
    return {
        "road": road,
        "house_number": house_number,
        "houseNumber": house_number,
        "city": city,
        "region": region,
        "postcode": postcode,
        "countryCode": country_code,
    }


def _location_from_nominatim_item(item, area_seed=None):
    if not isinstance(item, dict):
        return normalize_location_core({})

    geo = parse_geo_point({"lat": item.get("lat"), "lng": item.get("lon")})
    address_parts = _extract_address_parts_from_nominatim(item)
    address_display = clean_str(item.get("display_name")) or _build_display(
        address_parts.get("house_number"),
        address_parts.get("road"),
        address_parts.get("city"),
        address_parts.get("region"),
        _display_country_name(address_parts.get("countryCode")),
    )

    place_id = _to_osm_ref(item.get("osm_type"), item.get("osm_id")) or clean_str(item.get("place_id"))
    area = normalize_area_ref(area_seed if isinstance(area_seed, dict) else {}) if area_seed else None

    if not area:
        if _is_settlement_feature({"properties": {"osm_key": item.get("class"), "osm_value": item.get("type")}}):
            area = _build_area_ref(
                address_parts.get("city"),
                address_parts.get("region"),
                address_parts.get("countryCode"),
                _to_osm_ref(item.get("osm_type"), item.get("osm_id")),
                source="nominatim",
            )

    return normalize_location_core({
        "area": area,
        "address": {
            "raw": address_display,
            "display": address_display,
            "placeId": place_id or None,
            "provider": "nominatim",
            "road": clean_str(address_parts.get("road")),
            "houseNumber": clean_str(address_parts.get("house_number")),
            "house_number": clean_str(address_parts.get("house_number")),
            "city": clean_str(address_parts.get("city")),
            "region": clean_str(address_parts.get("region")),
            "postcode": clean_str(address_parts.get("postcode")),
            "countryCode": clean_str(address_parts.get("countryCode")),
        },
        "geo": geo,
        "display": address_display,
        "quality": "full" if area and geo else "address_only",
    })


def _resolve_area_from_geo(lat, lng, photon_base_url, nominatim_base_url, user_agent, language, country_codes, timeout_seconds, retries):
    reverse_settlement = _nominatim_reverse(
        lat,
        lng,
        nominatim_base_url=nominatim_base_url,
        user_agent=user_agent,
        language=language,
        timeout_seconds=timeout_seconds,
        retries=retries,
        zoom=10,
    )
    if not isinstance(reverse_settlement, dict):
        return None

    address = reverse_settlement.get("address") if isinstance(reverse_settlement.get("address"), dict) else {}
    city = _extract_city(address)
    region = _extract_region(address)
    country_code = _extract_country_code(address, _to_country_codes(country_codes)[0])

    area_id = _to_osm_ref(reverse_settlement.get("osm_type"), reverse_settlement.get("osm_id"))
    if area_id:
        return _build_area_ref(city, region, country_code, area_id, source="nominatim")

    # Fallback: query Photon by resolved city name.
    if not city:
        return None

    areas = search_location_areas(
        city,
        photon_base_url=photon_base_url,
        nominatim_base_url=nominatim_base_url,
        user_agent=user_agent,
        language=language,
        country_codes=country_codes,
        max_rows=5,
        timeout_ms=int(timeout_seconds * 1000),
        retries=retries,
    )
    if not areas:
        return None
    return normalize_area_ref((areas[0] or {}).get("area") if isinstance(areas[0], dict) else {})


def search_area_suggestions(search, photon_base_url, user_agent="", language="uk", country_codes="UA", max_rows=10, timeout_seconds=5, retries=1):
    search_clean = clean_str(search)
    if not search_clean:
        return []

    cache_key = f"photon:areas:{search_clean.lower()}:{clean_str(language).lower()}:{','.join(_to_country_codes(country_codes))}:{int(max_rows)}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    payload = _photon_request(
        photon_base_url,
        {
            "q": search_clean,
            "limit": max(int(max_rows), 1),
            "lang": clean_str(language) or "uk",
        },
        user_agent=user_agent,
        timeout=timeout_seconds,
        retries=retries,
    )
    features = payload.get("features") if isinstance(payload, dict) and isinstance(payload.get("features"), list) else []

    out = []
    for feature in features:
        item = _photon_area_suggestion_from_feature(feature, country_codes=country_codes)
        if not item:
            continue
        out.append(item)

    out = _dedupe_by_key(out, lambda x: x.get("id") or x.get("display"))
    out = out[: max(int(max_rows), 1)]
    _cache_set(cache_key, out, ttl_seconds=300)
    return out


def search_address_suggestions(
    search,
    photon_base_url,
    user_agent="",
    language="uk",
    country_codes="UA",
    max_rows=10,
    area_id="",
    city="",
    timeout_seconds=5,
    retries=1,
):
    search_clean = clean_str(search)
    area_id_clean = clean_str(area_id)
    city_clean = clean_str(city)
    if not search_clean:
        return []

    cache_key = f"photon:addresses:{search_clean.lower()}:{area_id_clean.lower()}:{city_clean.lower()}:{clean_str(language).lower()}:{','.join(_to_country_codes(country_codes))}:{int(max_rows)}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    query = search_clean
    if city_clean:
        query = f"{search_clean}, {city_clean}"

    payload = _photon_request(
        photon_base_url,
        {
            "q": query,
            "limit": max(int(max_rows), 1),
            "lang": clean_str(language) or "uk",
        },
        user_agent=user_agent,
        timeout=timeout_seconds,
        retries=retries,
    )
    features = payload.get("features") if isinstance(payload, dict) and isinstance(payload.get("features"), list) else []

    out = []
    allowed = set(_to_country_codes(country_codes))

    for feature in features:
        if not isinstance(feature, dict):
            continue

        props = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
        country_code = clean_str(props.get("countrycode") or "UA").upper()
        if country_code and allowed and country_code not in allowed:
            continue

        geo = _feature_geo(feature)
        if not geo:
            continue

        address_parts = _extract_photon_address_parts(feature)
        road = clean_str(address_parts.get("road"))
        house_number = clean_str(address_parts.get("house_number"))

        area = None
        if area_id_clean:
            area = _build_area_ref(
                city_clean or clean_str(address_parts.get("city")),
                clean_str(address_parts.get("region")),
                country_code,
                area_id_clean,
                source="photon",
            )
        elif _is_settlement_feature(feature):
            area = _photon_area_suggestion_from_feature(feature, country_codes=country_codes)
            area = normalize_area_ref((area or {}).get("area") if isinstance(area, dict) else {})

        if not area or not clean_str(area.get("id")):
            continue

        place_ref = _to_osm_ref(props.get("osm_type"), props.get("osm_id"))
        display = _build_display(
            _build_display(house_number, road).strip(),
            clean_str(address_parts.get("city")) or clean_str(area.get("city")),
            clean_str(address_parts.get("region")) or clean_str(area.get("region")),
            _display_country_name(country_code),
        ) or clean_str(props.get("name")) or clean_str(area.get("display"))

        has_street_and_house = bool(road and house_number)

        out.append({
            "id": clean_str(area.get("id")),
            "display": display,
            "lat": geo.get("coordinates", [None, None])[1],
            "lng": geo.get("coordinates", [None, None])[0],
            "area": area,
            "address": {
                "raw": display,
                "display": display,
                "placeId": place_ref or None,
                "provider": "photon",
                "road": road,
                "houseNumber": house_number,
                "house_number": house_number,
                "city": clean_str(address_parts.get("city")),
                "region": clean_str(address_parts.get("region")),
                "postcode": clean_str(address_parts.get("postcode")),
                "countryCode": country_code,
            },
            "addressParts": address_parts,
            "placeId": place_ref or None,
            "provider": "photon",
            "source": "photon",
            "hasStreetAndHouse": has_street_and_house,
        })

    out = _dedupe_by_key(out, lambda x: (x.get("placeId") or "", x.get("display") or "", x.get("id") or ""))
    out = out[: max(int(max_rows), 1)]
    _cache_set(cache_key, out, ttl_seconds=180)
    return out


def search_location_areas(
    search,
    provider="photon",
    photon_base_url="",
    nominatim_base_url="",
    user_agent="",
    language="uk",
    country_codes="UA",
    max_rows=10,
    timeout_ms=5000,
    retries=1,
    **_,
):
    del provider, nominatim_base_url
    timeout_seconds = max(parse_float(timeout_ms) or 5000, 1000) / 1000.0
    return search_area_suggestions(
        search,
        photon_base_url=photon_base_url,
        user_agent=user_agent,
        language=language,
        country_codes=country_codes,
        max_rows=max_rows,
        timeout_seconds=timeout_seconds,
        retries=retries,
    )


def search_location_addresses(
    search,
    provider="photon",
    photon_base_url="",
    nominatim_base_url="",
    user_agent="",
    language="uk",
    country_codes="UA",
    max_rows=10,
    area_id="",
    city="",
    timeout_ms=5000,
    retries=1,
    **_,
):
    del provider, nominatim_base_url
    timeout_seconds = max(parse_float(timeout_ms) or 5000, 1000) / 1000.0
    return search_address_suggestions(
        search,
        photon_base_url=photon_base_url,
        user_agent=user_agent,
        language=language,
        country_codes=country_codes,
        max_rows=max_rows,
        area_id=area_id,
        city=city,
        timeout_seconds=timeout_seconds,
        retries=retries,
    )


def normalize_location_input(
    payload,
    photon_base_url="",
    nominatim_base_url="",
    user_agent="",
    language="uk",
    country_codes="UA",
    timeout_ms=5000,
    retries=1,
    **_,
):
    payload = payload if isinstance(payload, dict) else {}
    seed = normalize_location_core(payload)

    timeout_seconds = max(parse_float(timeout_ms) or 5000, 1000) / 1000.0
    place_id = clean_str(payload.get("placeId") or ((seed.get("address") or {}).get("placeId") if isinstance(seed.get("address"), dict) else ""))

    area_seed = normalize_area_ref(payload.get("area") if isinstance(payload.get("area"), dict) else {})
    if not area_seed:
        area_seed = normalize_area_ref((seed.get("area") or {}) if isinstance(seed.get("area"), dict) else {})

    if place_id and clean_str(nominatim_base_url):
        item = _lookup_nominatim_by_place(
            place_id,
            nominatim_base_url=nominatim_base_url,
            user_agent=user_agent,
            language=language,
            timeout_seconds=timeout_seconds,
            retries=retries,
        )
        if isinstance(item, dict):
            location = _location_from_nominatim_item(item, area_seed=area_seed)
            if not (location.get("area") or {}).get("id") and area_seed:
                location["area"] = area_seed
                location = normalize_location_core(location)
            if area_seed and clean_str((area_seed or {}).get("id")):
                location["area"] = normalize_area_ref({**(location.get("area") or {}), **area_seed})
                location = normalize_location_core(location)
            return location

    geo_seed = parse_geo_point(seed.get("geo"))
    if geo_seed and clean_str(nominatim_base_url):
        lng, lat = geo_seed.get("coordinates")
        reverse = _nominatim_reverse(
            lat,
            lng,
            nominatim_base_url=nominatim_base_url,
            user_agent=user_agent,
            language=language,
            timeout_seconds=timeout_seconds,
            retries=retries,
            zoom=18,
        )
        if isinstance(reverse, dict):
            area = area_seed
            if not area or not clean_str((area or {}).get("id")):
                area = _resolve_area_from_geo(
                    lat,
                    lng,
                    photon_base_url=photon_base_url,
                    nominatim_base_url=nominatim_base_url,
                    user_agent=user_agent,
                    language=language,
                    country_codes=country_codes,
                    timeout_seconds=timeout_seconds,
                    retries=retries,
                )
            location = _location_from_nominatim_item(reverse, area_seed=area)
            if location and not location.get("geo"):
                location["geo"] = geo_seed
                location = normalize_location_core(location)
            return location

    address_display = clean_str((seed.get("address") or {}).get("display") if isinstance(seed.get("address"), dict) else "")
    if address_display and clean_str(nominatim_base_url):
        items = _nominatim_search(
            address_display,
            nominatim_base_url=nominatim_base_url,
            user_agent=user_agent,
            language=language,
            country_codes=country_codes,
            timeout_seconds=timeout_seconds,
            retries=retries,
            limit=1,
        )
        if items:
            location = _location_from_nominatim_item(items[0], area_seed=area_seed)
            if area_seed and clean_str((area_seed or {}).get("id")):
                location["area"] = normalize_area_ref({**(location.get("area") or {}), **area_seed})
                location = normalize_location_core(location)
            return location

    return seed


def reverse_geocode_location(
    lat,
    lng,
    photon_base_url="",
    nominatim_base_url="",
    user_agent="",
    language="uk",
    country_codes="UA",
    timeout_ms=5000,
    retries=1,
):
    lat_val = parse_float(lat)
    lng_val = parse_float(lng)
    if lat_val is None or lng_val is None:
        return None

    timeout_seconds = max(parse_float(timeout_ms) or 5000, 1000) / 1000.0
    reverse = _nominatim_reverse(
        lat_val,
        lng_val,
        nominatim_base_url=nominatim_base_url,
        user_agent=user_agent,
        language=language,
        timeout_seconds=timeout_seconds,
        retries=retries,
        zoom=18,
    )
    if not isinstance(reverse, dict):
        return None

    area = _resolve_area_from_geo(
        lat_val,
        lng_val,
        photon_base_url=photon_base_url,
        nominatim_base_url=nominatim_base_url,
        user_agent=user_agent,
        language=language,
        country_codes=country_codes,
        timeout_seconds=timeout_seconds,
        retries=retries,
    )
    return _location_from_nominatim_item(reverse, area_seed=area)


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
