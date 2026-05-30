#!/usr/bin/env python3
import argparse
import json
import os
from datetime import datetime, timezone

from pymongo import MongoClient


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description="Cleanup legacy geocoder/location fields after canonical migration")
    parser.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"))
    parser.add_argument("--db", default=os.environ.get("MONGO_DB_NAME", "memoria_test"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--report", default="migration-reports/cleanup-legacy-location-fields-report.json")
    args = parser.parse_args()

    if args.apply and not args.confirm:
        raise SystemExit("Refusing to write without --confirm")

    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    cleanup_plan = {
        "people": ["locationNormalized", "locationNormalizedUpdatedAt"],
        "cemeteries": ["locationBackfillAt", "locationAreaKatottgBackfillAt"],
        "churches": ["locationBackfillAt"],
        "ritual_services": ["locationBackfillAt"],
    }

    summary = {"timestamp": now_iso(), "dry_run": not args.apply, "collections": {}, "updated_total": 0}

    for coll_name, fields in cleanup_plan.items():
        coll = db[coll_name]
        unset_doc = {field: "" for field in fields}
        if args.apply:
            result = coll.update_many({}, {"$unset": unset_doc})
            modified = result.modified_count
        else:
            modified = coll.count_documents({"$or": [{field: {"$exists": True}} for field in fields]})
        summary["collections"][coll_name] = {"fields": fields, "affected_docs": modified}
        summary["updated_total"] += modified

    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
