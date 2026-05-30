import argparse
import json
from datetime import datetime, timezone

from pymongo import MongoClient


def clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


def parse_float(value):
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def valid_lat_lon(lat, lon):
    lat_v = parse_float(lat)
    lon_v = parse_float(lon)
    if lat_v is None or lon_v is None:
        return None
    if not (-90 <= lat_v <= 90 and -180 <= lon_v <= 180):
        return None
    return lat_v, lon_v


def coords_from_geo(doc):
    hq = doc.get("hqLocation") if isinstance(doc.get("hqLocation"), dict) else {}
    geo = hq.get("geo") if isinstance(hq, dict) else None
    if not isinstance(geo, dict):
        return None
    coords = geo.get("coordinates")
    if not isinstance(coords, list) or len(coords) != 2:
        return None
    lon = parse_float(coords[0])
    lat = parse_float(coords[1])
    return valid_lat_lon(lat, lon)


def build_settlement_lookup(settlements):
    by_code = {}
    by_name = {}
    for doc in settlements.find({}, {"katottg_code": 1, "name": 1, "lat": 1, "lon": 1}):
        coords = valid_lat_lon(doc.get("lat"), doc.get("lon"))
        if not coords:
            continue
        code = clean_str(doc.get("katottg_code"))
        name = clean_str(doc.get("name"))
        if code:
            by_code[code] = coords
        if name and name not in by_name:
            by_name[name] = coords
    return by_code, by_name


def resolve_coords(doc, by_code, by_name):
    direct = valid_lat_lon(doc.get("latitude"), doc.get("longitude"))
    if direct:
        return direct, "legacy_latlon"

    from_geo = coords_from_geo(doc)
    if from_geo:
        return from_geo, "hq_geo"

    service_area_ids = doc.get("serviceAreaIds") if isinstance(doc.get("serviceAreaIds"), list) else []
    for area_id in service_area_ids:
        area_key = clean_str(area_id)
        if area_key and area_key in by_code:
            return by_code[area_key], "serviceAreaIds"

    settlements = doc.get("settlements") if isinstance(doc.get("settlements"), list) else []
    for settlement_name in settlements:
        name = clean_str(settlement_name)
        if name and name in by_name:
            return by_name[name], "settlements"

    return None, "unresolved"


def has_valid_geo(doc):
    return coords_from_geo(doc) is not None


def main():
    parser = argparse.ArgumentParser(description="Backfill ritual service coordinates from canonical settlements")
    parser.add_argument("--mongo-uri", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--report", default="migration-reports/ritual-services-coordinates-backfill-report.json")
    parser.add_argument("--sample-unresolved", type=int, default=50)
    args = parser.parse_args()

    dry_run = not (args.apply and args.confirm)

    client = MongoClient(args.mongo_uri)
    db = client[args.db]
    ritual_services = db["ritual_services"]
    settlements = db["settlements"]

    by_code, by_name = build_settlement_lookup(settlements)

    scanned = 0
    updated = 0
    unresolved = 0
    unresolved_samples = []

    cursor = ritual_services.find({}, {"serviceAreaIds": 1, "settlements": 1, "latitude": 1, "longitude": 1, "hqLocation": 1, "name": 1})
    for doc in cursor:
        scanned += 1
        needs_fix = not has_valid_geo(doc) or valid_lat_lon(doc.get("latitude"), doc.get("longitude")) is None
        if not needs_fix:
            continue

        coords, source = resolve_coords(doc, by_code, by_name)
        if not coords:
            unresolved += 1
            if len(unresolved_samples) < max(args.sample_unresolved, 0):
                unresolved_samples.append({
                    "id": str(doc.get("_id")),
                    "name": clean_str(doc.get("name")),
                    "serviceAreaIds": doc.get("serviceAreaIds") if isinstance(doc.get("serviceAreaIds"), list) else [],
                    "settlements": doc.get("settlements") if isinstance(doc.get("settlements"), list) else [],
                })
            continue

        lat, lon = coords
        hq = doc.get("hqLocation") if isinstance(doc.get("hqLocation"), dict) else {}
        hq_geo = {
            "type": "Point",
            "coordinates": [lon, lat],
            "precision": "exact",
        }
        hq["geo"] = hq_geo

        update_doc = {
            "latitude": lat,
            "longitude": lon,
            "hqLocation": hq,
            "locationBackfillAt": datetime.now(timezone.utc),
            "locationBackfillSource": source,
        }

        if not dry_run:
            ritual_services.update_one({"_id": doc["_id"]}, {"$set": update_doc})
        updated += 1

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "scanned": scanned,
        "updated": updated,
        "unresolved": unresolved,
        "unresolved_samples": unresolved_samples,
    }

    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
