import argparse
import json
from datetime import datetime, timezone

from pymongo import MongoClient


def clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


def first_part(value):
    text = clean_str(value)
    if not text:
        return ""
    return text.split(",")[0].strip()


def looks_like_full_display(value):
    text = clean_str(value).lower()
    if not text:
        return False
    if "," in text:
        return True
    tokens = ("район", "област", "громад")
    return any(token in text for token in tokens)


def build_settlement_lookup(settlements):
    by_code = {}
    for item in settlements.find({}, {"katottg_code": 1, "name": 1, "district": 1, "region": 1}):
        code = clean_str(item.get("katottg_code"))
        if not code:
            continue
        by_code[code] = {
            "name": clean_str(item.get("name")),
            "district": clean_str(item.get("district")),
            "region": clean_str(item.get("region")),
        }
    return by_code


def main():
    parser = argparse.ArgumentParser(description="Normalize ritual service hqLocation.area.city to city-only value")
    parser.add_argument("--mongo-uri", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--report", default="migration-reports/ritual-services-area-city-normalize-report.json")
    parser.add_argument("--sample-unresolved", type=int, default=50)
    args = parser.parse_args()

    dry_run = not (args.apply and args.confirm)

    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    ritual_services = db["ritual_services"]
    settlements = db["settlements"]
    settlement_by_code = build_settlement_lookup(settlements)

    scanned = 0
    affected = 0
    unresolved = 0
    unresolved_samples = []

    cursor = ritual_services.find({}, {"hqLocation": 1, "serviceAreaIds": 1, "settlements": 1, "name": 1})
    for doc in cursor:
        scanned += 1
        hq_location = doc.get("hqLocation") if isinstance(doc.get("hqLocation"), dict) else None
        area = hq_location.get("area") if isinstance(hq_location, dict) and isinstance(hq_location.get("area"), dict) else None
        if not area:
            continue

        current_city = clean_str(area.get("city"))
        display = clean_str(area.get("display") or hq_location.get("display")) if isinstance(hq_location, dict) else ""

        if current_city and not looks_like_full_display(current_city):
            continue

        resolved_city = ""
        resolved_district = clean_str(area.get("district"))
        resolved_region = clean_str(area.get("region"))

        service_area_ids = doc.get("serviceAreaIds") if isinstance(doc.get("serviceAreaIds"), list) else []
        for area_id in service_area_ids:
            code = clean_str(area_id)
            if code and code in settlement_by_code:
                row = settlement_by_code[code]
                resolved_city = clean_str(row.get("name"))
                resolved_district = clean_str(row.get("district")) or resolved_district
                resolved_region = clean_str(row.get("region")) or resolved_region
                break

        if not resolved_city:
            settlements_list = doc.get("settlements") if isinstance(doc.get("settlements"), list) else []
            if settlements_list:
                resolved_city = first_part(settlements_list[0])

        if not resolved_city:
            resolved_city = first_part(display) or first_part(current_city)

        if not resolved_city:
            unresolved += 1
            if len(unresolved_samples) < max(args.sample_unresolved, 0):
                unresolved_samples.append({
                    "id": str(doc.get("_id")),
                    "name": clean_str(doc.get("name")),
                    "current_city": current_city,
                    "display": display,
                    "serviceAreaIds": service_area_ids,
                })
            continue

        new_area = dict(area)
        new_area["city"] = resolved_city
        if resolved_district:
            new_area["district"] = resolved_district
        if resolved_region:
            new_area["region"] = resolved_region

        if new_area == area:
            continue

        affected += 1
        if not dry_run:
            ritual_services.update_one(
                {"_id": doc["_id"]},
                {"$set": {
                    "hqLocation.area": new_area,
                    "locationAreaCityNormalizedAt": datetime.now(timezone.utc),
                }}
            )

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "scanned": scanned,
        "affected": affected,
        "unresolved": unresolved,
        "unresolved_samples": unresolved_samples,
    }

    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
