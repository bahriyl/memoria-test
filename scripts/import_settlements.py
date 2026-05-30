#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pymongo import ASCENDING, MongoClient, UpdateOne

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from settlements_service import load_source_dataset


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def ensure_indexes(coll):
    coll.create_index([("katottg_code", ASCENDING)], unique=True)
    coll.create_index([("name", ASCENDING)])
    coll.create_index([("normalized_name", ASCENDING)])
    coll.create_index([("region", ASCENDING), ("district", ASCENDING), ("hromada", ASCENDING)])
    coll.create_index([("type", ASCENDING)])
    coll.create_index([("location", "2dsphere")])


def main():
    parser = argparse.ArgumentParser(description="Import canonical settlements JSON into MongoDB settlements collection")
    parser.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"))
    parser.add_argument("--db", default=os.environ.get("MONGO_DB_NAME", "memoria_test"))
    parser.add_argument("--collection", default="settlements")
    parser.add_argument("--dataset", default=os.environ.get("SETTLEMENTS_DATASET_PATH", ""))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--report", default="migration-reports/settlements-import-report.json")
    args = parser.parse_args()

    if args.apply and not args.confirm:
        raise SystemExit("Refusing to write without --confirm")

    rows = load_source_dataset(args.dataset)
    client = MongoClient(args.mongo_uri)
    db = client[args.db]
    coll = db[args.collection]

    summary = {
        "timestamp": now_iso(),
        "dry_run": not args.apply,
        "dataset_rows": len(rows),
        "valid_rows": 0,
        "insert_or_update_candidates": 0,
        "modified_count": 0,
        "matched_count": 0,
        "upserted_count": 0,
        "validation_errors": [],
    }

    if args.apply:
        ensure_indexes(coll)

    ops = []
    for row in rows:
        code = row.get("katottg_code")
        if not code:
            summary["validation_errors"].append({"reason": "missing_katottg_code", "row": row})
            continue
        summary["valid_rows"] += 1
        summary["insert_or_update_candidates"] += 1

        if args.apply:
            now = datetime.utcnow()
            ops.append(
                UpdateOne(
                    {"katottg_code": code},
                    {
                        "$set": {**row, "updated_at": now},
                        "$setOnInsert": {"created_at": now},
                    },
                    upsert=True,
                )
            )
            if len(ops) >= max(int(args.batch_size), 1):
                res = coll.bulk_write(ops, ordered=False)
                summary["modified_count"] += res.modified_count
                summary["matched_count"] += res.matched_count
                summary["upserted_count"] += len(res.upserted_ids or {})
                ops = []

    if args.apply and ops:
        res = coll.bulk_write(ops, ordered=False)
        summary["modified_count"] += res.modified_count
        summary["matched_count"] += res.matched_count
        summary["upserted_count"] += len(res.upserted_ids or {})

    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
