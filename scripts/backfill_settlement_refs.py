#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient, UpdateOne

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from location_schema import clean_str


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def build_lookup(settlements):
    by_city_region = {}
    for item in settlements.find({}, {"katottg_code": 1, "name": 1, "region": 1}):
        code = clean_str(item.get("katottg_code"))
        city = clean_str(item.get("name")).lower()
        region = clean_str(item.get("region")).lower()
        if not code or not city:
            continue
        by_city_region.setdefault((city, region), code)
        by_city_region.setdefault((city, ""), code)
    return by_city_region


def resolve_code(by_city_region, area):
    area = area if isinstance(area, dict) else {}
    code = clean_str(area.get("id"))
    if code:
        return code
    city = clean_str(area.get("city")).lower()
    region = clean_str(area.get("region")).lower()
    if not city or not region:
        display = clean_str(area.get("display"))
        if display:
            parts = [clean_str(part) for part in display.split(",") if clean_str(part)]
            if not city and parts:
                city = parts[0].lower()
            if not region and len(parts) >= 2:
                region = parts[-1].lower()
    if not city:
        return ""
    return by_city_region.get((city, region)) or by_city_region.get((city, "")) or ""


def main():
    parser = argparse.ArgumentParser(description="Backfill katottg settlement references in entity collections")
    parser.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"))
    parser.add_argument("--db", default=os.environ.get("MONGO_DB_NAME", "memoria_test"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--report", default="migration-reports/settlement-refs-backfill-report.json")
    args = parser.parse_args()

    if args.apply and not args.confirm:
        raise SystemExit("Refusing to write without --confirm")

    client = MongoClient(args.mongo_uri)
    db = client[args.db]
    settlements = db["settlements"]
    lookup = build_lookup(settlements)

    targets = [
        ("people", "burial.location.area"),
        ("cemeteries", "location.area"),
        ("churches", "location.area"),
        ("ritual_services", "hqLocation.area"),
    ]

    summary = {"timestamp": now_iso(), "dry_run": not args.apply, "collections": {}, "updated_total": 0}

    for coll_name, area_path in targets:
        coll = db[coll_name]
        docs = coll.find({}, {"_id": 1, area_path: 1})
        ops = []
        updated = 0
        scanned = 0

        for doc in docs:
            scanned += 1
            area = doc
            for key in area_path.split("."):
                area = area.get(key) if isinstance(area, dict) else None
            if not isinstance(area, dict):
                continue
            code = resolve_code(lookup, area)
            if not code or clean_str(area.get("id")) == code:
                continue

            new_area = dict(area)
            new_area["id"] = code
            new_area["source"] = "katottg"
            update_doc = {area_path: new_area, "settlementRefUpdatedAt": datetime.utcnow()}

            updated += 1
            if args.apply:
                ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": update_doc}))

        if args.apply and ops:
            coll.bulk_write(ops, ordered=False)

        summary["collections"][coll_name] = {"scanned": scanned, "updated": updated}
        summary["updated_total"] += updated

    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
