#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient, UpdateOne

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from katottg_settlements import get_settlements_index, normalize_ukrainian_text
from location_mapper import normalize_cemetery_location

DEFAULT_DB_NAME = os.environ.get("MONGO_DB_NAME", "memoria_test")
DEFAULT_MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
REPORT_DIR = "migration-reports/cemeteries-katottg"


def _clean(v):
    if v is None:
        return ""
    return str(v).strip()


def _norm(v):
    text = normalize_ukrainian_text(v)
    text = re.sub(r"^[\s,.-]*(м\.|с\.|смт|місто|село|селище)\s*", "", text, flags=re.IGNORECASE)
    return text.strip()


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _city_from_display(display):
    text = _clean(display)
    if not text:
        return ""
    return _clean(text.split(",")[0])


def _build_indexes():
    rows = get_settlements_index()._items
    by_city = {}
    for row in rows:
        city_key = _norm(row.get("name"))
        if city_key:
            by_city.setdefault(city_key, []).append(row)
    return by_city


def _resolve_match(location):
    area = location.get("area") if isinstance(location.get("area"), dict) else {}

    city_candidates = [
        _clean(area.get("city")),
        _city_from_display(area.get("display")),
    ]
    city_candidates = [c for c in city_candidates if c]
    city_candidates = list(dict.fromkeys(city_candidates))

    if not city_candidates:
        return None, "unmatched", {"reason": "no_city_candidate"}

    region_hint = _clean(area.get("region"))
    by_city = _build_indexes()

    for city in city_candidates:
        matches = by_city.get(_norm(city), [])
        if not matches:
            continue
        if len(matches) == 1:
            return matches[0], "single", {"cityCandidate": city}

        if region_hint:
            narrowed = [m for m in matches if _norm(m.get("region")) == _norm(region_hint)]
            if len(narrowed) == 1:
                return narrowed[0], "disambiguated_by_region", {"cityCandidate": city, "regionHint": region_hint}
            if narrowed:
                matches = narrowed

        return None, "ambiguous", {
            "cityCandidate": city,
            "regionHint": region_hint,
            "variants": [
                {
                    "cityName": _clean(m.get("name")),
                    "katottg": _clean(m.get("katottg")),
                    "region": _clean(m.get("region")),
                    "district": _clean(m.get("district")),
                    "community": _clean(m.get("community")),
                    "label": _clean(m.get("label")),
                }
                for m in matches[:20]
            ],
        }

    return None, "unmatched", {"reason": "city_not_found", "cityCandidates": city_candidates}


def main():
    parser = argparse.ArgumentParser(description="Backfill cemeteries.location.area with KATOTTG identifiers")
    parser.add_argument("--mongo-uri", default=DEFAULT_MONGO_URI)
    parser.add_argument("--db", default=DEFAULT_DB_NAME)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--report-dir", default=REPORT_DIR)
    args = parser.parse_args()

    if args.apply and not args.confirm:
        raise SystemExit("Refusing to write without --confirm")

    client = MongoClient(args.mongo_uri)
    coll = client[args.db]["cemeteries"]

    updated = []
    ambiguous = []
    unmatched = []
    ops = []

    summary = {
        "documentsScanned": 0,
        "documentsUpdated": 0,
        "documentsSkipped": 0,
        "ambiguousMatches": 0,
        "unmatchedValues": 0,
        "dryRun": not args.apply,
    }

    for doc in coll.find({}):
        summary["documentsScanned"] += 1
        location = normalize_cemetery_location(doc)
        match, status, details = _resolve_match(location)

        if status == "ambiguous":
            summary["ambiguousMatches"] += 1
            summary["documentsSkipped"] += 1
            ambiguous.append({"_id": str(doc.get("_id")), "details": details})
            continue

        if status == "unmatched" or not match:
            summary["unmatchedValues"] += 1
            summary["documentsSkipped"] += 1
            unmatched.append({"_id": str(doc.get("_id")), "details": details})
            continue

        area = location.get("area") if isinstance(location.get("area"), dict) else {}
        area.update({
            "id": _clean(match.get("katottg")),
            "source": "katottg",
            "city": _clean(match.get("name")),
            "region": _clean(match.get("region")),
            "countryCode": _clean(area.get("countryCode") or "UA") or "UA",
            "display": _clean(match.get("label")),
        })
        location["area"] = area

        update_doc = {
            "location": location,
            "locationAreaKatottgBackfillAt": _now_iso(),
        }
        updated.append({
            "_id": str(doc.get("_id")),
            "areaId": area.get("id"),
            "areaDisplay": area.get("display"),
            "city": area.get("city"),
            "region": area.get("region"),
        })

        if args.apply:
            ops.append(UpdateOne({"_id": doc.get("_id")}, {"$set": update_doc}))
            if len(ops) >= max(args.batch_size, 1):
                result = coll.bulk_write(ops, ordered=False)
                summary["documentsUpdated"] += result.modified_count
                ops = []
        else:
            summary["documentsUpdated"] += 1

    if args.apply and ops:
        result = coll.bulk_write(ops, ordered=False)
        summary["documentsUpdated"] += result.modified_count

    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "cemeteries-katottg-updated.json").write_text(json.dumps(updated, ensure_ascii=False, indent=2), encoding="utf-8")
    (report_dir / "cemeteries-katottg-ambiguous.json").write_text(json.dumps(ambiguous, ensure_ascii=False, indent=2), encoding="utf-8")
    (report_dir / "cemeteries-katottg-unmatched.json").write_text(json.dumps(unmatched, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
