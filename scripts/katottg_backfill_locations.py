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


DEFAULT_DB_NAME = os.environ.get("MONGO_DB_NAME", "memoria_test")
DEFAULT_MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DEFAULT_BATCH_SIZE = 500
REPORTS_DIR = "migration-reports"

KEY_HINTS = (
    "city", "cityname", "settlement", "area", "oblast", "region", "district", "raion",
    "community", "hromada", "address", "location", "delivery", "client", "locality"
)

REMOVE_PREFIX_RE = re.compile(r"^[\s,.-]*(м\.|с\.|смт|місто|село|селище)\s*", flags=re.IGNORECASE)


def _clean(v):
    if v is None:
        return ""
    return str(v).strip()


def _norm(v):
    text = normalize_ukrainian_text(v)
    text = REMOVE_PREFIX_RE.sub("", text)
    return text.strip()


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _is_candidate_path(path):
    p = path.lower()
    return any(h in p for h in KEY_HINTS)


def _walk(obj, prefix=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else str(k)
            yield from _walk(v, path)
    elif isinstance(obj, list):
        for idx, item in enumerate(obj):
            path = f"{prefix}[{idx}]"
            yield from _walk(item, path)
    else:
        yield prefix, obj


def _extract_candidates(doc):
    candidates = []
    for path, value in _walk(doc):
        if not _is_candidate_path(path):
            continue
        if isinstance(value, str):
            text = _clean(value)
            if text:
                candidates.append((path, text))
    return candidates


def _build_indexes():
    rows = get_settlements_index()._items
    by_name = {}
    by_name_region = {}
    by_name_district = {}

    for row in rows:
        name_n = _norm(row.get("name"))
        region_n = _norm(row.get("region"))
        district_n = _norm(row.get("district"))
        if not name_n:
            continue
        by_name.setdefault(name_n, []).append(row)
        by_name_region.setdefault((name_n, region_n), []).append(row)
        by_name_district.setdefault((name_n, district_n), []).append(row)

    return rows, by_name, by_name_region, by_name_district


def _choose_best(matches, doc_context):
    if len(matches) <= 1:
        return matches[0] if matches else None, "single"

    region_n = _norm(doc_context.get("region"))
    district_n = _norm(doc_context.get("district"))
    community_n = _norm(doc_context.get("community"))

    narrowed = matches
    if region_n:
        region_matches = [m for m in narrowed if _norm(m.get("region")) == region_n]
        if region_matches:
            narrowed = region_matches
    if len(narrowed) > 1 and district_n:
        district_matches = [m for m in narrowed if _norm(m.get("district")) == district_n]
        if district_matches:
            narrowed = district_matches
    if len(narrowed) > 1 and community_n:
        community_matches = [m for m in narrowed if _norm(m.get("community")) == community_n]
        if community_matches:
            narrowed = community_matches

    if len(narrowed) == 1:
        return narrowed[0], "disambiguated"
    return None, "ambiguous"


def _doc_context(candidates):
    out = {"region": "", "district": "", "community": ""}
    for path, value in candidates:
        p = path.lower()
        if not out["region"] and any(x in p for x in ("region", "oblast")):
            out["region"] = value
        if not out["district"] and any(x in p for x in ("district", "raion")):
            out["district"] = value
        if not out["community"] and any(x in p for x in ("community", "hromada")):
            out["community"] = value
    return out


def _make_normalized(m):
    return {
        "cityName": _clean(m.get("name")),
        "settlementType": _clean(m.get("type")),
        "katottg": _clean(m.get("katottg")),
        "region": _clean(m.get("region")),
        "district": _clean(m.get("district")),
        "community": _clean(m.get("community")),
        "label": _clean(m.get("label")),
    }


def _match_doc(candidates, by_name, by_name_region, by_name_district):
    if not candidates:
        return None, "no_candidates", None

    context = _doc_context(candidates)

    for _, value in candidates:
        n = _norm(value)
        if not n:
            continue

        matches = by_name.get(n, [])
        if not matches:
            if context.get("region"):
                matches = by_name_region.get((n, _norm(context.get("region"))), [])
            if not matches and context.get("district"):
                matches = by_name_district.get((n, _norm(context.get("district"))), [])

        if not matches:
            continue

        chosen, how = _choose_best(matches, context)
        if chosen:
            return _make_normalized(chosen), how, {"sourceValue": value, "context": context}
        return None, "ambiguous", {"sourceValue": value, "context": context, "variants": [_make_normalized(m) for m in matches[:20]]}

    return None, "unmatched", {"context": context}


def main():
    parser = argparse.ArgumentParser(description="Backfill normalized KATOTTG settlement fields")
    parser.add_argument("--mongo-uri", default=DEFAULT_MONGO_URI)
    parser.add_argument("--db", default=DEFAULT_DB_NAME)
    parser.add_argument("--apply", action="store_true", help="Apply updates (default is dry-run)")
    parser.add_argument("--confirm", action="store_true", help="Required together with --apply")
    parser.add_argument("--collections", default="", help="Comma-separated collections filter")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--report-dir", default=REPORTS_DIR)
    args = parser.parse_args()

    if args.apply and not args.confirm:
        raise SystemExit("Refusing to write without --confirm")

    _, by_name, by_name_region, by_name_district = _build_indexes()

    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    allowed = {c.strip() for c in args.collections.split(",") if c.strip()}
    collection_names = db.list_collection_names()
    if allowed:
        collection_names = [c for c in collection_names if c in allowed]

    updated_report = []
    ambiguous_report = []
    unmatched_report = []

    summary = {
        "collectionsInspected": 0,
        "documentsScanned": 0,
        "documentsUpdated": 0,
        "documentsSkipped": 0,
        "ambiguousMatches": 0,
        "unmatchedValues": 0,
        "dryRun": not args.apply,
    }

    for coll_name in sorted(collection_names):
        coll = db[coll_name]
        summary["collectionsInspected"] += 1
        ops = []

        for doc in coll.find({}):
            summary["documentsScanned"] += 1
            doc_id = str(doc.get("_id"))
            candidates = _extract_candidates(doc)
            normalized, status, details = _match_doc(candidates, by_name, by_name_region, by_name_district)

            if status in {"no_candidates"}:
                summary["documentsSkipped"] += 1
                continue

            if status == "ambiguous":
                summary["ambiguousMatches"] += 1
                ambiguous_report.append({
                    "collection": coll_name,
                    "_id": doc_id,
                    "details": details,
                    "candidates": candidates[:40],
                })
                summary["documentsSkipped"] += 1
                continue

            if status == "unmatched" or not normalized:
                summary["unmatchedValues"] += 1
                unmatched_report.append({
                    "collection": coll_name,
                    "_id": doc_id,
                    "details": details,
                    "candidates": candidates[:40],
                })
                summary["documentsSkipped"] += 1
                continue

            update_doc = {
                "locationNormalized": normalized,
                "locationNormalizedUpdatedAt": _now_iso(),
            }
            updated_report.append({
                "collection": coll_name,
                "_id": doc_id,
                "locationNormalized": normalized,
                "match": details,
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

    (report_dir / "katottg-updated.json").write_text(json.dumps(updated_report, ensure_ascii=False, indent=2), encoding="utf-8")
    (report_dir / "katottg-ambiguous.json").write_text(json.dumps(ambiguous_report, ensure_ascii=False, indent=2), encoding="utf-8")
    (report_dir / "katottg-unmatched.json").write_text(json.dumps(unmatched_report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
