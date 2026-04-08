from copy import deepcopy

from location_schema import (
    clean_str,
    clean_str_list,
    geo_to_coords_text,
    normalize_location_core,
    parse_geo_point,
)


def _legacy_location_array(raw_location):
    if isinstance(raw_location, list):
        coords = clean_str(raw_location[0]) if len(raw_location) > 0 else ""
        landmarks = clean_str(raw_location[1]) if len(raw_location) > 1 else ""
        photos = clean_str_list(raw_location[2]) if len(raw_location) > 2 and isinstance(raw_location[2], list) else []
        return coords, landmarks, photos
    return "", "", []


def build_person_burial_from_legacy(doc):
    coords_text, landmarks, photos = _legacy_location_array(doc.get("location"))
    location = normalize_location_core({
        "areaId": doc.get("areaId"),
        "area": doc.get("area"),
        "coords": coords_text,
    })

    cemetery_ref = {
        "id": clean_str(doc.get("cemeteryId")) or None,
        "name": clean_str(doc.get("cemetery")),
    }

    return {
        "location": location,
        "landmarks": landmarks,
        "photos": photos,
        "cemeteryRef": cemetery_ref,
    }


def normalize_person_burial(doc):
    burial = doc.get("burial") if isinstance(doc.get("burial"), dict) else None
    if burial:
        normalized = {
            "location": normalize_location_core(burial.get("location") if isinstance(burial.get("location"), dict) else {}),
            "landmarks": clean_str(burial.get("landmarks")),
            "photos": clean_str_list(burial.get("photos")),
            "cemeteryRef": {
                "id": clean_str((burial.get("cemeteryRef") or {}).get("id")) or None,
                "name": clean_str((burial.get("cemeteryRef") or {}).get("name")),
            },
        }
        # Backfill fields if empty from legacy values.
        if not normalized["location"]["display"]:
            legacy = build_person_burial_from_legacy(doc)
            if legacy["location"]["display"]:
                normalized["location"] = legacy["location"]
        if not normalized["landmarks"]:
            normalized["landmarks"] = clean_str((build_person_burial_from_legacy(doc)).get("landmarks"))
        if not normalized["photos"]:
            normalized["photos"] = clean_str_list((build_person_burial_from_legacy(doc)).get("photos"))
        if not normalized["cemeteryRef"]["name"]:
            normalized["cemeteryRef"] = (build_person_burial_from_legacy(doc)).get("cemeteryRef")
        return normalized

    return build_person_burial_from_legacy(doc)


def person_burial_to_legacy_fields(burial):
    burial = burial if isinstance(burial, dict) else {}
    location = normalize_location_core((burial.get("location") or {}) if isinstance(burial.get("location"), dict) else {})
    coords_text = geo_to_coords_text(location.get("geo"))
    landmarks = clean_str(burial.get("landmarks"))
    photos = clean_str_list(burial.get("photos"))
    cemetery_ref = burial.get("cemeteryRef") if isinstance(burial.get("cemeteryRef"), dict) else {}
    cemetery_name = clean_str(cemetery_ref.get("name"))

    return {
        "areaId": clean_str((location.get("area") or {}).get("id")),
        "area": clean_str((location.get("area") or {}).get("display")),
        "cemetery": cemetery_name,
        "location": [coords_text, landmarks, photos],
    }


def normalize_cemetery_location(doc):
    if isinstance(doc.get("location"), dict):
        return normalize_location_core(doc.get("location"))
    return normalize_location_core({
        "area": doc.get("locality") or "",
        "addressLine": doc.get("addressLine") or doc.get("address") or "",
    })


def normalize_church_location(doc):
    if isinstance(doc.get("location"), dict):
        return normalize_location_core(doc.get("location"))
    return normalize_location_core({
        "area": doc.get("locality") or "",
        "addressLine": doc.get("address") or "",
    })


def normalize_ritual_hq_location(doc):
    if isinstance(doc.get("hqLocation"), dict):
        return normalize_location_core(doc.get("hqLocation"))

    addresses = []
    if isinstance(doc.get("address"), list):
        addresses = clean_str_list(doc.get("address"))
    elif clean_str(doc.get("address")):
        addresses = [clean_str(doc.get("address"))]

    geo = parse_geo_point({
        "lat": doc.get("latitude"),
        "lng": doc.get("longitude"),
    })
    return normalize_location_core({
        "addressLine": addresses[0] if addresses else "",
        "geo": geo,
    })


def cemetery_location_to_legacy(location):
    location = normalize_location_core(location if isinstance(location, dict) else {})
    area = location.get("area") or {}
    address = location.get("address") or {}
    locality = clean_str(area.get("city") or area.get("display"))
    # Prefer full canonical display to avoid short street-only values.
    address_line = clean_str(location.get("display") or address.get("display"))
    return {
        "locality": locality,
        "addressLine": address_line,
        "address": address_line,
    }


def church_location_to_legacy(location):
    location = normalize_location_core(location if isinstance(location, dict) else {})
    area = location.get("area") or {}
    address = location.get("address") or {}
    locality = clean_str(area.get("city") or area.get("display"))
    address_line = clean_str(address.get("display") or location.get("display"))
    return {
        "locality": locality,
        "address": address_line,
    }


def ritual_location_to_legacy(hq_location, current_doc=None):
    current_doc = current_doc if isinstance(current_doc, dict) else {}
    location = normalize_location_core(hq_location if isinstance(hq_location, dict) else {})
    # Prefer full canonical display to avoid short street-only values.
    address_display = clean_str(location.get("display") or (location.get("address") or {}).get("display"))
    geo = location.get("geo") or {}
    coords = geo.get("coordinates") if isinstance(geo.get("coordinates"), list) else []
    lng = coords[0] if len(coords) == 2 else None
    lat = coords[1] if len(coords) == 2 else None

    legacy_address = current_doc.get("address")
    address_list = clean_str_list(legacy_address) if isinstance(legacy_address, list) else []
    if address_display:
        if not address_list:
            address_list = [address_display]
        else:
            address_list[0] = address_display

    return {
        "address": address_list,
        "latitude": lat,
        "longitude": lng,
    }


def normalize_refs_list(raw):
    if not isinstance(raw, list):
        return []
    result = []
    for item in raw:
        if isinstance(item, dict):
            ref_id = clean_str(item.get("id"))
            ref_name = clean_str(item.get("name"))
        else:
            ref_id = None
            ref_name = clean_str(item)
        if not ref_id and not ref_name:
            continue
        result.append({"id": ref_id or None, "name": ref_name})
    return result


def refs_to_legacy_names(refs):
    out = []
    for item in normalize_refs_list(refs):
        if item.get("name"):
            out.append(item["name"])
    return out


def merge_dict(base, updates):
    merged = deepcopy(base) if isinstance(base, dict) else {}
    for key, value in (updates or {}).items():
        merged[key] = value
    return merged
