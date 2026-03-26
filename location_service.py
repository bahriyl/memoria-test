import math
import requests

from location_mapper import normalize_cemetery_location
from location_schema import clean_str, normalize_area_ref, normalize_location_core, parse_float, parse_geo_point


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
        })
    return out


def normalize_location_input(payload):
    payload = payload if isinstance(payload, dict) else {}

    area_payload = payload.get("area") if isinstance(payload.get("area"), dict) else {
        "id": payload.get("areaId"),
        "display": payload.get("areaDisplay") or payload.get("areaText") or payload.get("area"),
        "city": payload.get("city"),
        "region": payload.get("region"),
        "source": payload.get("areaSource") or ("geonames" if payload.get("areaId") else "manual"),
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
