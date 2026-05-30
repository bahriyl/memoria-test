#!/usr/bin/env python3
import argparse
import json
import os

from pymongo import MongoClient


def main():
    parser = argparse.ArgumentParser(description="Validate settlements migration")
    parser.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"))
    parser.add_argument("--db", default=os.environ.get("MONGO_DB_NAME", "memoria_test"))
    parser.add_argument("--report", default="migration-reports/settlements-validation-report.json")
    args = parser.parse_args()

    client = MongoClient(args.mongo_uri)
    db = client[args.db]
    settlements = db["settlements"]

    total = settlements.count_documents({})
    unique_count = len(settlements.distinct("katottg_code"))
    missing_geo = settlements.count_documents({"$or": [{"location": None}, {"location": {"$exists": False}}]})

    report = {
        "total_settlements": total,
        "unique_katottg_code": unique_count,
        "duplicate_katottg_exists": unique_count != total,
        "missing_location_count": missing_geo,
        "refs": {
            "people_with_area_id": db["people"].count_documents({"burial.location.area.id": {"$exists": True, "$ne": ""}}),
            "cemeteries_with_area_id": db["cemeteries"].count_documents({"location.area.id": {"$exists": True, "$ne": ""}}),
            "churches_with_area_id": db["churches"].count_documents({"location.area.id": {"$exists": True, "$ne": ""}}),
            "ritual_services_with_area_id": db["ritual_services"].count_documents({"hqLocation.area.id": {"$exists": True, "$ne": ""}}),
        },
    }

    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
