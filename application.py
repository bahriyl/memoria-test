import eventlet
eventlet.monkey_patch()

import time
import os
import re
import secrets
import bcrypt
import boto3
import requests
import base64
import io
import tempfile
import shutil
import subprocess
import threading
from datetime import datetime, timedelta, timezone
import jwt
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

from flask import Flask, request, jsonify, abort, make_response, send_file, after_this_request
from flask_cors import CORS
from pymongo import MongoClient, ASCENDING
from bson.objectid import ObjectId
from dotenv import load_dotenv
from flask_socketio import SocketIO, join_room

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from location_mapper import (
    cemetery_location_to_legacy,
    church_location_to_legacy,
    merge_dict,
    normalize_cemetery_location,
    normalize_church_location,
    normalize_person_burial,
    normalize_refs_list,
    normalize_ritual_hq_location,
    person_burial_to_legacy_fields,
    refs_to_legacy_names,
    ritual_location_to_legacy,
)
from location_schema import (
    clean_str as loc_clean_str,
    clean_str_list as loc_clean_str_list,
    normalize_location_core,
    parse_float as loc_parse_float,
    parse_geo_point as loc_parse_geo_point,
)
from location_service import (
    cemetery_option_from_doc,
    filter_docs_by_radius,
    normalize_location_input,
    search_location_addresses,
    search_location_areas,
)

load_dotenv()


try:
    from PIL import Image, ImageOps
except Exception:
    Image = None
    ImageOps = None



JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-key")
JWT_ALGORITHM = "HS256"
JWT_EXP_DELTA_SECONDS = 43200

NP_API_KEY = os.getenv('NP_API_KEY')
NP_BASE_URL = 'https://api.novaposhta.ua/v2.0/json/'

application = Flask(__name__)
CORS(application)

socketio = SocketIO(application, cors_allowed_origins="*")
ADMINS_ROOM = "admins"

_compress_video_jobs = {}
_compress_video_lock = threading.Lock()

def _set_compress_job(job_id, **updates):
    with _compress_video_lock:
        job = _compress_video_jobs.get(job_id)
        if not job:
            return
        job.update(updates)

def _get_compress_job(job_id):
    with _compress_video_lock:
        return _compress_video_jobs.get(job_id)

def _remove_compress_job(job_id):
    with _compress_video_lock:
        return _compress_video_jobs.pop(job_id, None)

KYIVSTAR_OTP_SEND_URL = "https://api-gateway.kyivstar.ua/mock/rest/v1/verification/sms"
KYIVSTAR_OTP_VERIFY_URL = "https://api-gateway.kyivstar.ua/mock/rest/v1/verification/sms/check"
KYIVSTAR_ACCESS_TOKEN = os.environ.get("KYIVSTAR_ACCESS_TOKEN")

SPACES_KEY = os.environ.get("SPACES_KEY")
SPACES_SECRET = os.environ.get("SPACES_SECRET")
SPACES_REGION = os.environ.get("SPACES_REGION")
SPACES_BUCKET = os.environ.get("SPACES_BUCKET")
SPACES_CORS_ORIGINS = [o.strip() for o in os.environ.get("SPACES_CORS_ORIGINS", "").split(",") if o.strip()]

# Create S3-compatible client for DigitalOcean Spaces
s3 = boto3.client(
    "s3",
    region_name=SPACES_REGION,
    endpoint_url=f"https://{SPACES_REGION}.digitaloceanspaces.com",
    aws_access_key_id=SPACES_KEY,
    aws_secret_access_key=SPACES_SECRET,
)

def _ensure_spaces_cors():
    if not SPACES_BUCKET or not SPACES_CORS_ORIGINS:
        return
    try:
        s3.put_bucket_cors(
            Bucket=SPACES_BUCKET,
            CORSConfiguration={
                "CORSRules": [
                    {
                        "AllowedOrigins": SPACES_CORS_ORIGINS,
                        "AllowedMethods": ["GET", "HEAD", "PUT", "POST"],
                        "AllowedHeaders": ["*"],
                        "ExposeHeaders": [
                            "ETag",
                            "Content-Range",
                            "Accept-Ranges",
                            "Content-Length",
                            "Location",
                        ],
                        "MaxAgeSeconds": 3000,
                    }
                ]
            },
        )
        application.logger.info("Spaces CORS configured for %s", SPACES_CORS_ORIGINS)
    except Exception as e:
        application.logger.warning("Failed to set Spaces CORS: %s", e)

_ensure_spaces_cors()


client = MongoClient('mongodb+srv://tsbgalcontract:mymongodb26@cluster0.kppkt.mongodb.net/test')
db = client['memoria_test']
people_collection = db['people']
areas_collection = db['areas']
people_moderation_collection = db['people_moderation']
orders_collection = db['orders']
chat_collection = db['chats']
message_collection = db['messages']
cemeteries_collection = db['cemeteries']
churches_collection = db['churches']
ritual_services_collection = db['ritual_services']
ritual_service_categories_collection = db['ritual_service_categories']
location_moderation_collection = db['location_moderation']
liturgies_collection = db['liturgies']
chat_templates_collection = db['chat_templates']

ALLOWED_CHAT_CATEGORIES = {'Реклама', 'Ритуальні послуги', 'Електронна записка'}
CHAT_TEMPLATE_TITLE_MAX_LEN = 120
CHAT_TEMPLATE_TEXT_MAX_LEN = 5000

# Legacy backfill: older chats without explicit admin-open state are treated as already opened.
chat_collection.update_many(
    {'openedByAdmin': {'$exists': False}},
    {'$set': {'openedByAdmin': True, 'openedAt': None}}
)
# Legacy backfill: older chats without explicit chat status are considered open.
chat_collection.update_many(
    {'chatStatus': {'$exists': False}},
    {'$set': {'chatStatus': 'open'}}
)
# Legacy backfill: older chats without category are treated as uncategorized.
chat_collection.update_many(
    {'category': {'$exists': False}},
    {'$set': {'category': None}}
)
# Legacy backfill: older chats without last admin read marker are unread until opened.
chat_collection.update_many(
    {'lastAdminReadAt': {'$exists': False}},
    {'$set': {'lastAdminReadAt': None}}
)
# Legacy backfill: older chats without attached profile are treated as not attached.
chat_collection.update_many(
    {'attachedPersonId': {'$exists': False}},
    {'$set': {'attachedPersonId': None}}
)

BINANCE_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"

ALLOWED_UPDATE_FIELDS = {
    "name",
    "birthYear",
    "birthDate",
    "deathYear",
    "deathDate",
    "notable",
    "avatarUrl",
    "portraitUrl",
    "area",
    "areaId",
    "cemetery",
    "location",
    "bio",
    "photos",
    "sharedPending",
    "sharedPhotos",
    "comments",
    "relatives",
    "heroImage",
    "burial",
}

GEONAMES_USER = os.environ.get("GEONAMES_USER", "memoria")
GEONAMES_LANG = "uk"
LOCATION_PROVIDER = os.environ.get("LOCATION_PROVIDER", "locationiq").strip().lower() or "locationiq"
LOCATIONIQ_API_KEY = os.environ.get("LOCATIONIQ_API_KEY", "").strip()
LOCATIONIQ_REGION = os.environ.get("LOCATIONIQ_REGION", "eu1").strip().lower() or "eu1"
LOCATION_COUNTRY_CODES = os.environ.get("LOCATION_COUNTRY_CODES", "UA").strip() or "UA"
LOCATION_ACCEPT_LANGUAGE = os.environ.get("LOCATION_ACCEPT_LANGUAGE", "uk").strip() or "uk"
LOCATION_READ_MODE = os.environ.get("LOCATION_READ_MODE", "hybrid").strip().lower() or "hybrid"
LOCATION_WRITE_MODE = os.environ.get("LOCATION_WRITE_MODE", "dual").strip().lower() or "dual"
LOCATION_ADMIN_STRICT_LOCATION = os.environ.get("LOCATION_ADMIN_STRICT_LOCATION", "").strip().lower()
LOCATION_ADMIN_STRICT_GEONAMES = os.environ.get("LOCATION_ADMIN_STRICT_GEONAMES", "0").strip().lower() or "0"


def _location_read_mode():
    if LOCATION_READ_MODE in {"legacy", "canonical", "hybrid"}:
        return LOCATION_READ_MODE
    return "hybrid"


def _location_write_mode():
    if LOCATION_WRITE_MODE in {"legacy", "canonical", "dual"}:
        return LOCATION_WRITE_MODE
    return "dual"


def _location_read_uses_canonical():
    return _location_read_mode() in {"canonical", "hybrid"}


def _location_write_is_legacy():
    return _location_write_mode() == "legacy"


def _location_write_is_canonical():
    return _location_write_mode() == "canonical"


def _location_write_is_dual():
    return _location_write_mode() == "dual"


def _location_admin_strict_geonames_enabled():
    value = LOCATION_ADMIN_STRICT_LOCATION or LOCATION_ADMIN_STRICT_GEONAMES
    return value in {"1", "true", "yes", "on"}


def _location_has_provider_area_and_geo(location):
    normalized = normalize_location_core(location if isinstance(location, dict) else {})
    area = normalized.get("area") if isinstance(normalized.get("area"), dict) else {}
    geo = loc_parse_geo_point(normalized.get("geo"))
    area_id = loc_clean_str(area.get("id"))
    area_source = loc_clean_str(area.get("source")).lower()
    return bool(area_id and area_source in {"geonames", "locationiq"} and geo)


def _location_has_geonames_area_and_geo(location):
    # Compatibility alias used by legacy call sites.
    return _location_has_provider_area_and_geo(location)


def _validate_admin_strict_location(field_name, location):
    if not _location_has_provider_area_and_geo(location):
        abort(400, description=f"`{field_name}` must come from provider suggestions (`area.id` + coordinates required)")

try:
    liturgies_collection.create_index([("personId", ASCENDING), ("serviceDate", ASCENDING)])
except Exception:
    pass

try:
    liturgies_collection.create_index([("paymentStatus", ASCENDING), ("churchName", ASCENDING), ("serviceDate", ASCENDING)])
except Exception:
    pass

try:
    liturgies_collection.create_index([("paymentStatus", ASCENDING), ("createdAt", ASCENDING)])
except Exception:
    pass

try:
    churches_collection.create_index([("name", ASCENDING)])
except Exception:
    pass

try:
    message_collection.create_index([("chatId", ASCENDING), ("createdAt", -1)])
except Exception:
    pass

try:
    chat_collection.create_index([("chatStatus", ASCENDING), ("category", ASCENDING)])
except Exception:
    pass

try:
    chat_collection.create_index([("chatStatus", ASCENDING), ("attachedPersonId", ASCENDING)])
except Exception:
    pass

try:
    ritual_service_categories_collection.create_index([("nameNormalized", ASCENDING)], unique=True)
except Exception:
    pass

try:
    people_collection.create_index([("burial.location.area.id", ASCENDING)])
except Exception:
    pass

try:
    people_collection.create_index([("burial.cemeteryRef.id", ASCENDING)])
except Exception:
    pass

try:
    people_collection.create_index([("burial.location.geo", "2dsphere")])
except Exception:
    pass

try:
    cemeteries_collection.create_index([("location.area.id", ASCENDING)])
except Exception:
    pass

try:
    cemeteries_collection.create_index([("location.geo", "2dsphere")])
except Exception:
    pass

try:
    churches_collection.create_index([("location.area.id", ASCENDING)])
except Exception:
    pass

try:
    churches_collection.create_index([("location.geo", "2dsphere")])
except Exception:
    pass

try:
    ritual_services_collection.create_index([("hqLocation.area.id", ASCENDING)])
except Exception:
    pass

try:
    ritual_services_collection.create_index([("hqLocation.geo", "2dsphere")])
except Exception:
    pass


def _to_object_id(s):
    try:
        return ObjectId(s)
    except Exception:
        abort(400, "Invalid ObjectId format")


def verify_password(raw_password: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(raw_password.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


def hash_password(raw: str) -> str:
    return bcrypt.hashpw(raw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def validate_new_password(pw: str):
    if not isinstance(pw, str) or len(pw) < 8:
        abort(400, description="Пароль має містити щонайменше 8 символів")


def send_mail(to_email: str, subject: str, html: str):
    """Send an HTML email using Gmail SMTP with TLS."""
    from_email = os.getenv("GMAIL_USER")
    password = os.getenv("GMAIL_PASS")

    if not from_email or not password:
        raise RuntimeError("Missing GMAIL_USER or GMAIL_PASS in environment variables")

    # Build message
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email
    msg.attach(MIMEText(html, "html"))

    try:
        # Connect to Gmail SMTP (TLS)
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()  # Upgrade to secure connection
            server.login(from_email, password)
            server.send_message(msg)
        print(f"[MAIL] Sent to={to_email} subj={subject}")
    except Exception as e:
        print(f"[MAIL ERROR] to={to_email}: {e}")


@application.route("/api/binance-p2p", methods=["POST"])
def binance_p2p_proxy():
    # Forward the JSON body to Binance
    resp = requests.post(BINANCE_URL, json=request.get_json())
    # Mirror Binance’s status code + JSON back
    return (resp.content, resp.status_code, {"Content-Type": "application/json"})


@application.route('/api/coin-icon', methods=['GET'])
def get_coin_icon():
    symbol = request.args.get('symbol', '').strip().lower()
    if not symbol:
        return jsonify({"error": "Missing 'symbol' parameter"}), 400

    # Step 1: Search for the symbol
    url = f"{COINGECKO_API_BASE}/search?query={symbol}"
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return jsonify({"error": "Search request failed", "details": str(e)}), 500

    image = data['coins'][0]['thumb']
    return jsonify({
        "icon_thumb": image
    })


def _str_to_bool(value):
    raw = loc_clean_str(value).lower()
    if raw in {"1", "true", "yes", "y"}:
        return True
    if raw in {"0", "false", "no", "n"}:
        return False
    return None


def _person_with_location_projection(person):
    burial = normalize_person_burial(person)
    legacy = person_burial_to_legacy_fields(burial)

    result = dict(person)
    if _location_read_uses_canonical() or not _location_write_is_canonical():
        result["burial"] = burial
    result["areaId"] = legacy["areaId"] or loc_clean_str(result.get("areaId"))
    result["area"] = legacy["area"] or loc_clean_str(result.get("area"))
    result["cemetery"] = legacy["cemetery"] or loc_clean_str(result.get("cemetery"))
    result["location"] = legacy["location"]
    return result


def _person_list_payload(person):
    person = _person_with_location_projection(person)
    payload = {
        "id": str(person.get('_id')),
        "name": person.get('name'),
        "birthYear": person.get('birthYear'),
        "deathYear": person.get('deathYear'),
        "notable": person.get('notable'),
        "avatarUrl": person.get('avatarUrl'),
        "portraitUrl": person.get('portraitUrl'),
        "areaId": person.get('areaId'),
        "area": person.get('area'),
        "cemetery": person.get('cemetery'),
    }
    if _location_read_uses_canonical():
        payload["burial"] = person.get("burial")
    return payload


@application.route('/api/people', methods=['GET'])
def people():
    search_query = request.args.get('search', '').strip()
    birth_year = request.args.get('birthYear')
    death_year = request.args.get('deathYear')
    area_id = request.args.get('areaId', '').strip()
    area = request.args.get('area', '').strip()
    cemetery = request.args.get('cemetery', '').strip()
    cemetery_id = request.args.get('cemeteryId', '').strip()
    has_geo = _str_to_bool(request.args.get('hasGeo'))
    lat = request.args.get('lat')
    lng = request.args.get('lng')
    radius_km = request.args.get('radiusKm')

    conditions = []
    if search_query:
        conditions.append({'name': {'$regex': re.escape(search_query), '$options': 'i'}})
    if birth_year and birth_year.isdigit():
        conditions.append({'birthYear': int(birth_year)})
    if death_year and death_year.isdigit():
        conditions.append({'deathYear': int(death_year)})

    if area_id:
        conditions.append({
            '$or': [
                {'areaId': area_id},
                {'burial.location.area.id': area_id},
            ]
        })
    elif area:
        city_part = area.split(',')[0].strip()
        if city_part:
            pattern = r'\b' + re.escape(city_part) + r'\b'
            conditions.append({
                '$or': [
                    {'area': {'$regex': pattern, '$options': 'i'}},
                    {'burial.location.area.display': {'$regex': pattern, '$options': 'i'}},
                    {'burial.location.area.city': {'$regex': pattern, '$options': 'i'}},
                ]
            })

    if cemetery_id:
        conditions.append({
            '$or': [
                {'cemeteryId': cemetery_id},
                {'burial.cemeteryRef.id': cemetery_id},
            ]
        })
    elif cemetery:
        cem_regex = {'$regex': re.escape(cemetery), '$options': 'i'}
        conditions.append({
            '$or': [
                {'cemetery': cem_regex},
                {'burial.cemeteryRef.name': cem_regex},
            ]
        })

    mongo_filter = {'$and': conditions} if len(conditions) > 1 else (conditions[0] if conditions else {})
    docs = list(people_collection.find(mongo_filter))

    if has_geo is not None:
        filtered = []
        for person in docs:
            burial = normalize_person_burial(person)
            has_person_geo = bool(loc_parse_geo_point((burial.get("location") or {}).get("geo")))
            if has_geo and has_person_geo:
                filtered.append(person)
            if has_geo is False and not has_person_geo:
                filtered.append(person)
        docs = filtered

    if lat is not None and lng is not None and radius_km is not None:
        docs = filter_docs_by_radius(
            docs,
            origin_lat=lat,
            origin_lng=lng,
            radius_km=radius_km,
            location_getter=lambda item: (normalize_person_burial(item).get("location") if isinstance(item, dict) else None),
        )

    people_list = [_person_list_payload(person) for person in docs]
    return jsonify({"total": len(people_list), "people": people_list})


def _validate_shared_pending(value):
    if value is None:
        return []
    if not isinstance(value, list):
        abort(400, description="`sharedPending` must be an array of {url}")
    out = []
    for i, item in enumerate(value):
        if not isinstance(item, dict):
            abort(400, description=f"`sharedPending[{i}]` must be an object")
        url = item.get("url")
        if not isinstance(url, str) or not url.strip():
            abort(400, description=f"`sharedPending[{i}].url` must be a non-empty string")
        out.append({"url": url.strip()})
    return out


def mask_phone(phone: str) -> str:
    phone = (phone or '').strip()
    if not phone:
        return ''

    digits = re.sub(r'\D', '', phone)
    if not digits:
        return phone

    suffix_len = min(4, len(digits))
    suffix = digits[-suffix_len:]

    if digits.startswith('380') and len(digits) > suffix_len:
        middle_len = len(digits) - 3 - suffix_len
        masked = '380' + ('*' * max(0, middle_len)) + suffix
        return f"+{masked}" if phone.startswith('+') else masked

    prefix_len = min(3, max(1, len(digits) - suffix_len))
    prefix = digits[:prefix_len]
    middle_len = len(digits) - prefix_len - suffix_len
    masked = prefix + ('*' * max(0, middle_len)) + suffix
    if phone.startswith('+'):
        return f"+{masked}"
    return masked


def sanitize_premium_payload(premium_doc):
    if not isinstance(premium_doc, dict):
        return None

    phone = (premium_doc.get('phone') or '').strip()
    payload = {'locked': True, 'hasPhone': bool(phone)}
    masked = mask_phone(phone)
    if masked:
        payload['phoneMasked'] = masked
    return payload


def _clean_nonempty_string(value):
    if not isinstance(value, str):
        return ""
    return value.strip()


def _build_premium_partner_payload(raw_partner):
    if not isinstance(raw_partner, dict):
        return None

    name = _clean_nonempty_string(raw_partner.get("name"))
    address = _clean_nonempty_string(raw_partner.get("address"))
    logo = _clean_nonempty_string(raw_partner.get("logo"))

    payload = {}
    if name:
        payload["name"] = name
    if address:
        payload["address"] = address
    if logo:
        payload["logo"] = logo
    return payload or None


def _resolve_premium_partner_by_id(raw_ritual_service_id):
    ritual_service_id = _clean_nonempty_string(raw_ritual_service_id)
    if not ritual_service_id:
        return None, ""

    try:
        oid = ObjectId(ritual_service_id)
    except Exception:
        return None, ""

    ritual_service = ritual_services_collection.find_one(
        {"_id": oid},
        {"name": 1, "address": 1, "logo": 1, "link": 1}
    )
    if not ritual_service:
        return None, ""

    partner_payload = _build_premium_partner_payload(ritual_service)
    partner_link = _clean_nonempty_string(ritual_service.get("link"))
    return partner_payload, partner_link


def _attach_premium_partner_to_response(response, person_doc):
    if 'premium' not in person_doc:
        return

    premium_payload = sanitize_premium_payload(person_doc.get('premium'))
    if not premium_payload:
        return

    response['premium'] = premium_payload

    partner_payload = None
    partner_link = ""
    partner_service_id = _clean_nonempty_string(person_doc.get('premiumPartnerRitualServiceId'))
    if partner_service_id:
        response['premiumPartnerRitualServiceId'] = partner_service_id
        partner_payload, partner_link = _resolve_premium_partner_by_id(partner_service_id)

    if partner_link:
        response['premiumPartnerLink'] = partner_link

    if partner_payload:
        response['premiumPartner'] = partner_payload


def verify_sms_code_with_kyivstar(phone: str, code: str):
    payload = {
        "subscriberId": str(phone),
        "validationCode": str(code)
    }

    try:
        resp = requests.post(
            KYIVSTAR_OTP_VERIFY_URL,
            json=payload,
            headers=kyivstar_headers(),
            timeout=10
        )
    except requests.RequestException as exc:
        return False, {
            "error": "kyivstar_otp_verify_exception",
            "details": str(exc)
        }, 502

    if resp.status_code != 200:
        return False, {
            "error": "kyivstar_otp_verify_failed",
            "status_code": resp.status_code,
            "details": resp.text
        }, resp.status_code

    res = resp.json()
    status = (res.get("resource", {}).get("status") or '').upper()

    if status == 'VALID':
        return True, {"status": status}, 200
    if status == 'INVALID':
        return False, {"status": status}, 401
    return False, {
        "status": status,
        "details": res
    }, 400


def _validate_shared_photos(value):
    if value is None:
        return []
    if not isinstance(value, list):
        abort(400, description="`sharedPhotos` must be an array of {url, description}")
    out = []
    for i, item in enumerate(value):
        if not isinstance(item, dict):
            abort(400, description=f"`sharedPhotos[{i}]` must be an object")
        url = item.get("url")
        desc = item.get("description", "")
        if not isinstance(url, str) or not url.strip():
            abort(400, description=f"`sharedPhotos[{i}].url` must be a non-empty string")
        if not isinstance(desc, str):
            abort(400, description=f"`sharedPhotos[{i}].description` must be a string")
        out.append({"url": url.strip(), "description": desc})
    return out


def _normalize_media_item(item):
    # "https://..." legacy
    if isinstance(item, str):
        u = item.strip()
        return {"url": u, "description": ""} if u else None

    if not isinstance(item, dict):
        return None

    desc = item.get("description", "")
    if not isinstance(desc, str):
        desc = str(desc)

    # photo
    url = item.get("url")
    if isinstance(url, str) and url.strip():
        return {"url": url.strip(), "description": desc}

    # video
    v = item.get("video")
    if isinstance(v, dict):
        player = v.get("player")
        poster = v.get("poster", "")
        if isinstance(player, str) and player.strip():
            out_v = {"player": player.strip()}
            if isinstance(poster, str) and poster.strip():
                out_v["poster"] = poster.strip()
            return {"video": out_v, "description": desc}

    return None


@application.route('/api/people/<string:person_id>', methods=['GET'])
def get_person(person_id):
    try:
        oid = ObjectId(person_id)
    except Exception:
        abort(400, description="Invalid person id")

    person = people_collection.find_one({'_id': oid})
    if not person:
        abort(404, description="Person not found")

    raw_photos = person.get('photos', [])
    photos_norm = []
    if isinstance(raw_photos, list):
        for it in raw_photos:
            n = _normalize_media_item(it)
            if n:
                photos_norm.append(n)

    person = _person_with_location_projection(person)
    response = {
        "id": str(person['_id']),
        "name": person.get('name'),
        "birthYear": person.get('birthYear'),
        "birthDate": person.get('birthDate'),
        "deathYear": person.get('deathYear'),
        "deathDate": person.get('deathDate'),
        "notable": person.get('notable', False),
        "avatarUrl": person.get('avatarUrl'),
        "portraitUrl": person.get('portraitUrl'),  # ← NEW
        "heroImage": person.get('heroImage'),
        "areaId": person.get('areaId'),
        "area": person.get('area'),
        "cemetery": person.get('cemetery'),
        "location": person.get('location'),
        "burial": person.get('burial') if _location_read_uses_canonical() else None,
        "bio": person.get('bio'),
        "photos": photos_norm,
        "sharedPending": person.get('sharedPending', []),
        "sharedPhotos": person.get('sharedPhotos', []),
        "comments": _sort_comments(person.get('comments', []))
    }
    if 'relatives' in person:
        response['relatives'] = [
            {"personId": str(r['personId']), "role": r.get('role')}
            for r in person.get('relatives', [])
        ]
    _attach_premium_partner_to_response(response, person)
    return jsonify(response)


def _validate_photos_shape(value):
    """
    Accepts:
      - { url: <string>, description?: <string> }
      - { video: { player: <string>, poster?: <string> }, description?: <string> }
      - Back-compat: "https://..." -> {url: "...", description: ""}
    """
    if value is None:
        return []
    if not isinstance(value, list):
        abort(400, description="`photos` must be an array of objects")

    out = []
    for i, item in enumerate(value):
        # Back-compat string
        if isinstance(item, str):
            u = item.strip()
            if not u:
                abort(400, description=f"`photos[{i}]` string must be a non-empty URL")
            out.append({"url": u, "description": ""})
            continue

        if not isinstance(item, dict):
            abort(400, description=f"`photos[{i}]` must be an object")

        # Normalize empty strings to "missing"
        def _clean_str(s):
            return s.strip() if isinstance(s, str) else s

        desc = item.get("description", "")
        if not isinstance(desc, str):
            abort(400, description=f"`photos[{i}].description` must be a string")

        url = _clean_str(item.get("url"))
        video = item.get("video")

        # ✅ VIDEO first (even if url=="" present)
        if isinstance(video, dict):
            player = _clean_str(video.get("player"))
            poster = _clean_str(video.get("poster", ""))  # optional

            if not player:
                abort(400, description=f"`photos[{i}].video.player` must be a non-empty string")

            vobj = {"player": player}
            if poster:
                vobj["poster"] = poster

            out.append({"video": vobj, "description": desc})
            continue

        # ✅ PHOTO next
        if url is not None:
            if not url:
                abort(400, description=f"`photos[{i}].url` must be a non-empty string")
            out.append({"url": url, "description": desc})
            continue

        abort(400, description=f"`photos[{i}]` must contain either `url` or `video`")
    return out


def _sanitize_comment_authors(comments):
    if not isinstance(comments, list):
        abort(400, description="`comments` must be a list of objects")

    def _parse_comment_moment(comment_obj):
        date_time_raw = (comment_obj.get("dateTime") or "").strip() if isinstance(comment_obj.get("dateTime"), str) else ""
        if date_time_raw:
            try:
                dt = datetime.fromisoformat(date_time_raw.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                return dt
            except Exception:
                abort(400, description="`dateTime` must be a valid ISO datetime string")

        date_raw = (comment_obj.get("date") or "").strip() if isinstance(comment_obj.get("date"), str) else ""
        if not date_raw:
            return None

        m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", date_raw)
        if not m:
            return None

        dd = int(m.group(1))
        mm = int(m.group(2))
        yyyy = int(m.group(3))

        hh = 0
        mi = 0
        time_raw = (comment_obj.get("time") or "").strip() if isinstance(comment_obj.get("time"), str) else ""
        if time_raw:
            tm = re.match(r"^([01]?\d|2[0-3]):([0-5]\d)$", time_raw)
            if not tm:
                abort(400, description="`time` must be in HH:MM format")
            hh = int(tm.group(1))
            mi = int(tm.group(2))

        try:
            return datetime(yyyy, mm, dd, hh, mi, tzinfo=timezone.utc)
        except Exception:
            return None

    sanitized = []
    for c in comments:
        if not isinstance(c, dict):
            abort(400, description="Each comment must be a JSON object")

        author = c.get("author", "")
        if author is not None and not isinstance(author, str):
            abort(400, description="`author` must be a string")

        text = c.get("text", "")
        if text is not None and not isinstance(text, str):
            abort(400, description="`text` must be a string")

        if "date" in c and c.get("date") is not None and not isinstance(c.get("date"), str):
            abort(400, description="`date` must be a string")
        if "time" in c and c.get("time") is not None and not isinstance(c.get("time"), str):
            abort(400, description="`time` must be a string")
        if "dateTime" in c and c.get("dateTime") is not None and not isinstance(c.get("dateTime"), str):
            abort(400, description="`dateTime` must be a string")

        normalized = dict(c)
        moment = _parse_comment_moment(normalized)
        if moment is not None:
            normalized["dateTime"] = moment.isoformat()
            if "time" not in normalized or not isinstance(normalized.get("time"), str) or not normalized.get("time").strip():
                normalized["time"] = moment.strftime("%H:%M")

        sanitized.append(normalized)

    return sanitized


def _comment_sort_key(comment_obj):
    if not isinstance(comment_obj, dict):
        return float("-inf")

    date_time_raw = comment_obj.get("dateTime")
    if isinstance(date_time_raw, str) and date_time_raw.strip():
        try:
            dt = datetime.fromisoformat(date_time_raw.strip().replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.timestamp()
        except Exception:
            pass

    date_raw = comment_obj.get("date")
    if isinstance(date_raw, str):
        m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", date_raw.strip())
        if m:
            dd = int(m.group(1))
            mm = int(m.group(2))
            yyyy = int(m.group(3))
            hh = 0
            mi = 0
            time_raw = comment_obj.get("time")
            if isinstance(time_raw, str):
                tm = re.match(r"^([01]?\d|2[0-3]):([0-5]\d)$", time_raw.strip())
                if tm:
                    hh = int(tm.group(1))
                    mi = int(tm.group(2))
            try:
                dt = datetime(yyyy, mm, dd, hh, mi, tzinfo=timezone.utc)
                return dt.timestamp()
            except Exception:
                pass

    return float("-inf")


def _sort_comments(comments):
    if not isinstance(comments, list):
        return []

    def _safe_sort_key(comment_obj):
        key = _comment_sort_key(comment_obj)
        if isinstance(key, datetime):
            if key.tzinfo is None:
                key = key.replace(tzinfo=timezone.utc)
            else:
                key = key.astimezone(timezone.utc)
            return key.timestamp()
        if isinstance(key, (int, float)):
            return float(key)
        return float("-inf")

    return sorted(comments, key=_safe_sort_key, reverse=True)


ALLOWED_ROLES = {"Мати", "Батько", "Брат", "Сестра", "Чоловік", "Дружина", "Син", "Донька", "Дідусь", "Бабуся", "Онук", "Онука", "Дядько", "Тітка", "Без статусу"}


def _validate_relatives(value):
    if not isinstance(value, list):
        abort(400, description="`relatives` must be a list")

    cleaned = []
    seen = set()
    for i, item in enumerate(value):
        if not isinstance(item, dict):
            abort(400, description=f"relatives[{i}] must be an object")

        pid = item.get('personId') or item.get('id')
        role = item.get('role')

        if not pid:
            abort(400, description=f"relatives[{i}].personId is required")
        try:
            oid = ObjectId(pid)
        except Exception:
            abort(400, description=f"relatives[{i}].personId is invalid")

        if role not in ALLOWED_ROLES:
            abort(400, description=f"relatives[{i}].role must be one of: {', '.join(sorted(ALLOWED_ROLES))}")

        # de-dupe by personId (last wins)
        if oid in seen:
            cleaned = [r for r in cleaned if r['personId'] != oid]
        seen.add(oid)

        cleaned.append({'personId': oid, 'role': role})
    return cleaned


@application.route('/api/people/<string:person_id>', methods=['PUT'])
def update_person(person_id):
    try:
        oid = ObjectId(person_id)
    except Exception:
        abort(400, description="Invalid person id")

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        abort(400, description="Request must be a JSON object")

    if 'photoDescriptions' in data:
        abort(400, description="`photoDescriptions` is no longer supported. Use `photos: [{url, description}]`.")

    existing_person = people_collection.find_one({'_id': oid})
    if not existing_person:
        abort(404, description="Person not found")

    update_doc = {}
    for field, value in data.items():
        if field not in ALLOWED_UPDATE_FIELDS:
            continue

        if field == 'photos':
            update_doc['photos'] = _validate_photos_shape(value)
        elif field == 'sharedPending':
            update_doc['sharedPending'] = _validate_shared_pending(value)
        elif field == 'sharedPhotos':
            update_doc['sharedPhotos'] = _validate_shared_photos(value)
        elif field == 'relatives':
            update_doc['relatives'] = _validate_relatives(value)
        elif field == 'comments':
            update_doc['comments'] = _sanitize_comment_authors(value)
        elif field == 'burial':
            if value is None:
                value = {}
            if not isinstance(value, dict):
                abort(400, description="`burial` must be an object")
            update_doc['burial'] = {
                "location": normalize_location_core(value.get("location") if isinstance(value.get("location"), dict) else {}),
                "landmarks": loc_clean_str(value.get("landmarks")),
                "photos": loc_clean_str_list(value.get("photos")),
                "cemeteryRef": {
                    "id": loc_clean_str((value.get("cemeteryRef") or {}).get("id")) or None,
                    "name": loc_clean_str((value.get("cemeteryRef") or {}).get("name")),
                },
            }
        else:
            update_doc[field] = value

    if not update_doc:
        abort(400, description=f"No valid fields to update. Allowed: {', '.join(sorted(ALLOWED_UPDATE_FIELDS))}")

    location_fields = {"area", "areaId", "cemetery", "location", "burial"}
    has_location_changes = bool(location_fields.intersection(set(update_doc.keys())))
    if has_location_changes:
        if "burial" in update_doc:
            merged_doc = merge_dict(existing_person, {"burial": update_doc["burial"]})
        else:
            legacy_updates = {k: update_doc[k] for k in ("area", "areaId", "cemetery", "location") if k in update_doc}
            merged_doc = merge_dict(existing_person, legacy_updates)

        burial_normalized = normalize_person_burial(merged_doc)

        if _location_write_is_canonical() or _location_write_is_dual():
            update_doc["burial"] = burial_normalized
        else:
            update_doc.pop("burial", None)

        if _location_write_is_legacy() or _location_write_is_dual():
            legacy_fields = person_burial_to_legacy_fields(burial_normalized)
            update_doc["areaId"] = legacy_fields["areaId"]
            update_doc["area"] = legacy_fields["area"]
            update_doc["cemetery"] = legacy_fields["cemetery"]
            update_doc["location"] = legacy_fields["location"]
        else:
            update_doc.pop("areaId", None)
            update_doc.pop("area", None)
            update_doc.pop("cemetery", None)
            update_doc.pop("location", None)

    result = people_collection.update_one({'_id': oid}, {'$set': update_doc})
    if result.matched_count == 0:
        abort(404, description="Person not found")

    person = people_collection.find_one({'_id': oid})
    person = _person_with_location_projection(person)
    response = {
        "id": str(person['_id']),
        "name": person.get('name'),
        "birthYear": person.get('birthYear'),
        "birthDate": person.get('birthDate'),
        "deathYear": person.get('deathYear'),
        "deathDate": person.get('deathDate'),
        "notable": person.get('notable', False),
        "avatarUrl": person.get('avatarUrl'),
        "portraitUrl": person.get('portraitUrl'),
        "heroImage": person.get('heroImage'),
        "areaId": person.get('areaId'),
        "area": person.get('area'),
        "cemetery": person.get('cemetery'),
        "location": person.get('location'),
        "burial": person.get('burial') if _location_read_uses_canonical() else None,
        "bio": person.get('bio'),
        "photos": person.get('photos', []),
        "sharedPending": person.get('sharedPending', []),
        "sharedPhotos": person.get('sharedPhotos', []),
        "comments": _sort_comments(person.get('comments', [])),
        "relatives": [
            {"personId": str(r['personId']), "role": r.get('role')}
            for r in person.get('relatives', [])
        ]
    }
    _attach_premium_partner_to_response(response, person)
    return jsonify(response), 200


@application.route('/api/people/<string:person_id>/shared/offer', methods=['OPTIONS', 'POST'])
def offer_shared_photo(person_id):
    # CORS preflight: just OK it
    if request.method == 'OPTIONS':
        resp = make_response('', 204)
        return resp

    # Validate id
    try:
        oid = ObjectId(person_id)
    except Exception:
        abort(400, description="Invalid person id")

    data = request.get_json(silent=True) or {}
    url = (data.get('url') or '').strip()
    if not url:
        abort(400, description="`url` is required")

    # push to sharedPending (keep minimal shape for now)
    update = {'$push': {'sharedPending': {'url': url, 'createdAt': datetime.utcnow()}}}
    res = people_collection.update_one({'_id': oid}, update)
    if res.matched_count == 0:
        abort(404, description="Person not found")

    return jsonify({'ok': True})


@application.route('/api/people/location_moderation', methods=['POST'])
def people_location_moderation():
    data = request.get_json(silent=True) or {}
    person_id = data.get('personId')
    location = data.get('location')
    burial_payload = data.get('burial') if isinstance(data.get('burial'), dict) else None

    if burial_payload:
        location_v2 = normalize_location_core(burial_payload.get("location") if isinstance(burial_payload.get("location"), dict) else {})
        landmarks = loc_clean_str(burial_payload.get("landmarks"))
        photos = loc_clean_str_list(burial_payload.get("photos"))
    else:
        legacy_doc = {
            "location": location if isinstance(location, list) else [],
            "areaId": data.get("areaId"),
            "area": data.get("area"),
            "cemetery": data.get("cemetery"),
        }
        burial = normalize_person_burial(legacy_doc)
        location_v2 = burial.get("location")
        landmarks = burial.get("landmarks")
        photos = burial.get("photos")

    document = {
        'personId': person_id,
        'location': location,
        'locationV2': location_v2,
        'landmarks': landmarks,
        'photos': photos,
        'burial': {
            'location': location_v2,
            'landmarks': landmarks,
            'photos': photos,
            'cemeteryRef': (burial_payload or {}).get('cemeteryRef', {}),
        },
        'createdAt': datetime.utcnow(),
    }
    location_moderation_collection.insert_one(document)

    return jsonify({'success': True})


@application.route('/api/cemeteries_page', methods=['GET'])
def cemeteries_page():
    search_query = request.args.get('search', '').strip()

    query_filter = {}

    if search_query:
        query_filter['name'] = {'$regex': re.escape(search_query), '$options': 'i'}

    cemeteries_cursor = cemeteries_collection.find(query_filter)

    cemeteries_list = []
    for cemetery in cemeteries_cursor:
        location = normalize_cemetery_location(cemetery)
        legacy_loc = cemetery_location_to_legacy(location)
        cemeteries_list.append({
            "id": str(cemetery.get('_id')),
            "name": cemetery.get('name'),
            "image": cemetery.get('image'),
            "address": cemetery.get('address') or legacy_loc.get("address"),
            "phone": cemetery.get('phone'),
            "description": cemetery.get('description'),
            "location": location if _location_read_uses_canonical() else None,
        })

    return jsonify({
        "total": len(cemeteries_list),
        "cemeteries": cemeteries_list
    })


@application.route('/api/cemeteries_page/<string:cemetery_id>', methods=['GET'])
def get_cemetery_page(cemetery_id):
    # 1) Validate & convert the id
    try:
        oid = ObjectId(cemetery_id)
    except Exception:
        abort(400, description="Invalid cemetery id")

    # 2) Fetch from Mongo
    cemetery = cemeteries_collection.find_one({'_id': oid})
    if not cemetery:
        abort(404, description="Cemetery not found")

    # 3) Build your response payload
    location = normalize_cemetery_location(cemetery)
    legacy_loc = cemetery_location_to_legacy(location)
    return jsonify({
        "id": str(cemetery.get('_id')),
        "name": cemetery.get('name'),
        "image": cemetery.get('image'),
        "address": cemetery.get('address') or legacy_loc.get("address"),
        "phone": cemetery.get('phone'),
        "description": cemetery.get('description'),
        "location": location if _location_read_uses_canonical() else None,
    })


ADMIN_CEMETERY_STATUSES = {'Активний', 'Неактивний'}
ADMIN_CEMETERY_ADDED_LABELS = {'Memoria', 'Сторінки', 'Виробники', 'Таблички'}
ADMIN_CEMETERY_ADDED_TONES = {'green', 'gold'}
ADMIN_CEMETERY_ALLOWED_FIELDS = {
    'name',
    'location',
    'locality',
    'addressLine',
    'churchRefs',
    'addedLabel',
    'addedTone',
    'churchesList',
    'fillPercent',
    'status',
    'phoneContacts',
    'contactPersons',
    'description',
    'notes',
    'pageImage',
}


def _clean_str(value):
    if value is None:
        return ''
    return str(value).strip()


def _clean_str_list(value, field_name):
    if value is None:
        return []
    if not isinstance(value, list):
        abort(400, description=f"`{field_name}` must be an array")

    items = []
    for idx, item in enumerate(value):
        if not isinstance(item, str):
            abort(400, description=f"`{field_name}[{idx}]` must be a string")
        cleaned = item.strip()
        if cleaned:
            items.append(cleaned)
    return items


def _parse_fill_percent(value):
    if value is None or value == '':
        return 0
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        abort(400, description="`fillPercent` must be an integer in range 0..100")
    if parsed < 0 or parsed > 100:
        abort(400, description="`fillPercent` must be an integer in range 0..100")
    return parsed


def _derive_added_tone(added_label):
    return 'green' if _clean_str(added_label).lower() == 'memoria' else 'gold'


def _normalize_admin_cemetery(cemetery):
    location = normalize_cemetery_location(cemetery)
    legacy_loc = cemetery_location_to_legacy(location)
    locality = _clean_str(cemetery.get('locality') or legacy_loc.get('locality') or cemetery.get('addressLine') or cemetery.get('address'))
    address_line = _clean_str(cemetery.get('addressLine') or legacy_loc.get('addressLine') or cemetery.get('locality') or cemetery.get('address'))

    added_label = _clean_str(cemetery.get('addedLabel')) or 'Memoria'
    if added_label not in ADMIN_CEMETERY_ADDED_LABELS:
        added_label = 'Memoria'
    added_tone = _clean_str(cemetery.get('addedTone'))
    if added_tone not in ADMIN_CEMETERY_ADDED_TONES:
        added_tone = _derive_added_tone(added_label)

    status = _clean_str(cemetery.get('status'))
    if status not in ADMIN_CEMETERY_STATUSES:
        status = 'Активний'

    church_refs = normalize_refs_list(cemetery.get('churchRefs') if isinstance(cemetery.get('churchRefs'), list) else cemetery.get('churchesList'))
    churches_list = refs_to_legacy_names(church_refs)
    fill_percent = _parse_fill_percent(cemetery.get('fillPercent'))

    phone_contacts = _clean_str_list(cemetery.get('phoneContacts'), 'phoneContacts')
    legacy_phone = _clean_str(cemetery.get('phone'))
    if not phone_contacts and legacy_phone:
        phone_contacts = [legacy_phone]

    contact_persons = _clean_str_list(cemetery.get('contactPersons'), 'contactPersons')
    description = _clean_str(cemetery.get('description'))
    notes = _clean_str(cemetery.get('notes'))
    page_image = _clean_str(cemetery.get('pageImage') or cemetery.get('image'))

    created_at = cemetery.get('createdAt')
    updated_at = cemetery.get('updatedAt')

    return {
        'id': str(cemetery.get('_id')),
        'name': _clean_str(cemetery.get('name')),
        'locality': locality,
        'addressLine': address_line,
        'location': location,
        'addedLabel': added_label,
        'addedTone': added_tone,
        'churches': len(churches_list),
        'churchesList': churches_list,
        'churchRefs': church_refs,
        'fillPercent': fill_percent,
        'status': status,
        'phoneContacts': phone_contacts,
        'contactPersons': contact_persons,
        'description': description,
        'notes': notes,
        'pageImage': page_image,
        'createdAt': created_at.isoformat() if isinstance(created_at, datetime) else None,
        'updatedAt': updated_at.isoformat() if isinstance(updated_at, datetime) else None,
    }


def _build_admin_cemetery_payload(data, partial=False):
    if not isinstance(data, dict):
        abort(400, description="Body must be a JSON object")

    unknown = set(data.keys()) - ADMIN_CEMETERY_ALLOWED_FIELDS
    if unknown:
        unknown_list = ', '.join(sorted(unknown))
        abort(400, description=f"Unsupported fields: {unknown_list}")

    payload = {}

    def _validate_required_text(key, required):
        if key not in data:
            if required:
                abort(400, description=f"`{key}` is required")
            return
        cleaned = _clean_str(data.get(key))
        if not cleaned:
            abort(400, description=f"`{key}` is required")
        payload[key] = cleaned

    _validate_required_text('name', required=not partial)
    if not partial:
        if _location_admin_strict_geonames_enabled():
            if 'location' not in data:
                abort(400, description="`location` is required and must come from provider suggestions")
        elif 'location' not in data:
            _validate_required_text('locality', required=True)

    location_payload = None
    if 'location' in data:
        raw_location = data.get('location')
        if raw_location is not None and not isinstance(raw_location, dict):
            abort(400, description="`location` must be an object")
        location_payload = normalize_location_core(raw_location or {})
        if _location_admin_strict_geonames_enabled():
            _validate_admin_strict_location('location', location_payload)
        if _location_write_is_canonical() or _location_write_is_dual() or _location_admin_strict_geonames_enabled():
            payload['location'] = location_payload

    if 'addressLine' in data:
        address_line = _clean_str(data.get('addressLine'))
        if not address_line and not partial:
            abort(400, description="`addressLine` cannot be empty")
        payload['addressLine'] = address_line

    if 'addedLabel' in data:
        added_label = _clean_str(data.get('addedLabel')) or 'Memoria'
        if added_label not in ADMIN_CEMETERY_ADDED_LABELS:
            abort(400, description="`addedLabel` must be one of: Memoria, Сторінки, Виробники, Таблички")
        payload['addedLabel'] = added_label

    if 'addedTone' in data:
        tone = _clean_str(data.get('addedTone'))
        if tone not in ADMIN_CEMETERY_ADDED_TONES:
            abort(400, description="`addedTone` must be one of: green, gold")
        payload['addedTone'] = tone

    if 'churchesList' in data:
        payload['churchesList'] = _clean_str_list(data.get('churchesList'), 'churchesList')
    if 'churchRefs' in data:
        if not isinstance(data.get('churchRefs'), list):
            abort(400, description="`churchRefs` must be an array")
        payload['churchRefs'] = normalize_refs_list(data.get('churchRefs'))

    if 'fillPercent' in data:
        payload['fillPercent'] = _parse_fill_percent(data.get('fillPercent'))

    if 'status' in data:
        status = _clean_str(data.get('status'))
        if status not in ADMIN_CEMETERY_STATUSES:
            abort(400, description="`status` must be one of: Активний, Неактивний")
        payload['status'] = status

    if 'phoneContacts' in data:
        payload['phoneContacts'] = _clean_str_list(data.get('phoneContacts'), 'phoneContacts')

    if 'contactPersons' in data:
        payload['contactPersons'] = _clean_str_list(data.get('contactPersons'), 'contactPersons')

    if 'description' in data:
        payload['description'] = _clean_str(data.get('description'))

    if 'notes' in data:
        payload['notes'] = _clean_str(data.get('notes'))

    if 'pageImage' in data:
        payload['pageImage'] = _clean_str(data.get('pageImage'))

    if 'churchRefs' in payload and 'churchesList' not in payload:
        payload['churchesList'] = refs_to_legacy_names(payload['churchRefs'])
    if 'churchesList' in payload and 'churchRefs' not in payload:
        payload['churchRefs'] = normalize_refs_list(payload['churchesList'])

    legacy_locality = payload.get('locality')
    legacy_address_line = payload.get('addressLine')
    if legacy_locality and not legacy_address_line:
        legacy_address_line = legacy_locality
    if legacy_address_line and not legacy_locality:
        legacy_locality = legacy_address_line

    if location_payload is None and (
        ('locality' in payload) or ('addressLine' in payload) or ('locality' in data) or ('addressLine' in data)
    ):
        location_payload = normalize_location_core({
            'area': {'display': legacy_locality or ''},
            'addressLine': legacy_address_line or '',
        })
        if _location_write_is_canonical() or _location_write_is_dual() or _location_admin_strict_geonames_enabled():
            payload['location'] = location_payload

    if location_payload and (_location_write_is_legacy() or _location_write_is_dual()):
        legacy_loc = cemetery_location_to_legacy(location_payload)
        payload['locality'] = legacy_loc.get('locality', '')
        payload['addressLine'] = legacy_loc.get('addressLine', '')
    elif legacy_locality or legacy_address_line:
        payload['locality'] = legacy_locality or ''
        payload['addressLine'] = legacy_address_line or ''

    if not partial:
        payload.setdefault('addedLabel', 'Memoria')
        payload.setdefault('addedTone', _derive_added_tone(payload.get('addedLabel')))
        payload.setdefault('churchRefs', [])
        payload.setdefault('churchesList', refs_to_legacy_names(payload.get('churchRefs')))
        payload.setdefault('fillPercent', 0)
        payload.setdefault('status', 'Активний')
        payload.setdefault('phoneContacts', [])
        payload.setdefault('contactPersons', [])
        payload.setdefault('description', '')
        payload.setdefault('notes', '')
        payload.setdefault('pageImage', '')
        payload.setdefault('addressLine', payload.get('locality', ''))
        payload.setdefault('locality', payload.get('addressLine', ''))

        if 'location' not in payload and (_location_write_is_canonical() or _location_write_is_dual()):
            payload['location'] = normalize_location_core({
                'area': {'display': payload.get('locality', '')},
                'addressLine': payload.get('addressLine', ''),
            })
        if ('locality' not in payload or 'addressLine' not in payload) and payload.get('location'):
            legacy_loc = cemetery_location_to_legacy(payload.get('location'))
            payload.setdefault('locality', legacy_loc.get('locality', ''))
            payload.setdefault('addressLine', legacy_loc.get('addressLine', ''))

    if 'pageImage' in payload:
        payload['image'] = payload['pageImage']
    if 'addressLine' in payload:
        payload['address'] = payload['addressLine']
    if 'phoneContacts' in payload:
        payload['phone'] = payload['phoneContacts'][0] if payload['phoneContacts'] else ''

    return payload


@application.route('/api/admin/cemeteries', methods=['GET'])
def admin_list_cemeteries():
    search = _clean_str(request.args.get('search', ''))
    query_filter = {}

    if search:
        regex = {'$regex': re.escape(search), '$options': 'i'}
        query_filter = {
            '$or': [
                {'name': regex},
                {'locality': regex},
                {'addressLine': regex},
                {'address': regex},
                {'location.display': regex},
                {'location.area.display': regex},
                {'location.area.city': regex},
            ]
        }

    cursor = cemeteries_collection.find(query_filter).sort([('createdAt', -1), ('_id', -1)])
    cemeteries_list = [_normalize_admin_cemetery(cemetery) for cemetery in cursor]

    return jsonify({
        'total': len(cemeteries_list),
        'cemeteries': cemeteries_list
    })


@application.route('/api/admin/cemeteries', methods=['POST'])
def admin_create_cemetery():
    data = request.get_json(silent=True) or {}
    payload = _build_admin_cemetery_payload(data, partial=False)
    now = datetime.utcnow()
    payload['createdAt'] = now
    payload['updatedAt'] = now

    inserted = cemeteries_collection.insert_one(payload)
    created = cemeteries_collection.find_one({'_id': inserted.inserted_id})
    return jsonify(_normalize_admin_cemetery(created)), 201


@application.route('/api/admin/cemeteries/<string:cemetery_id>', methods=['GET'])
def admin_get_cemetery(cemetery_id):
    try:
        oid = ObjectId(cemetery_id)
    except Exception:
        abort(400, description='Invalid cemetery id')

    cemetery = cemeteries_collection.find_one({'_id': oid})
    if not cemetery:
        abort(404, description='Cemetery not found')

    return jsonify(_normalize_admin_cemetery(cemetery))


@application.route('/api/admin/cemeteries/<string:cemetery_id>', methods=['PATCH'])
def admin_update_cemetery(cemetery_id):
    try:
        oid = ObjectId(cemetery_id)
    except Exception:
        abort(400, description='Invalid cemetery id')

    data = request.get_json(silent=True) or {}
    update_fields = _build_admin_cemetery_payload(data, partial=True)
    if not update_fields:
        abort(400, description='Nothing to update')

    update_fields['updatedAt'] = datetime.utcnow()
    result = cemeteries_collection.update_one({'_id': oid}, {'$set': update_fields})
    if result.matched_count == 0:
        abort(404, description='Cemetery not found')

    cemetery = cemeteries_collection.find_one({'_id': oid})
    return jsonify(_normalize_admin_cemetery(cemetery))


@application.route('/api/admin/cemeteries/<string:cemetery_id>', methods=['DELETE'])
def admin_delete_cemetery(cemetery_id):
    try:
        oid = ObjectId(cemetery_id)
    except Exception:
        abort(400, description='Invalid cemetery id')

    result = cemeteries_collection.delete_one({'_id': oid})
    if result.deleted_count == 0:
        abort(404, description='Cemetery not found')

    return jsonify({'ok': True})


ADMIN_CHURCH_STATUSES = {'Активний', 'Неактивний'}
DAY_ID_TO_WEEKDAY = {
    'sun': 0,
    'mon': 1,
    'tue': 2,
    'wed': 3,
    'thu': 4,
    'fri': 5,
    'sat': 6,
}
WEEKDAY_TO_DAY_ID = {value: key for key, value in DAY_ID_TO_WEEKDAY.items()}
WEEKDAY_TO_LITURGY_KEY = {2: 'Вт', 5: 'Пт', 6: 'Сб'}
LITURGY_KEY_TO_WEEKDAY = {value: key for key, value in WEEKDAY_TO_LITURGY_KEY.items()}
ADMIN_CHURCH_ALLOWED_FIELDS = {
    'name',
    'location',
    'address',
    'locality',
    'workingDays',
    'liturgyEndTimes',
    'churchDays',
    'cemeteryRefs',
    'cemeteries',
    'status',
    'contacts',
    'contactPerson',
    'phone',
    'infoNotes',
    'iban',
    'taxCode',
    'recipientName',
    'botCode',
}


def _clean_contacts_list_loose(value):
    if not isinstance(value, list):
        return []

    contacts = []
    for item in value:
        if not isinstance(item, dict):
            continue
        person = _clean_str(item.get('person'))
        phone = _clean_str(item.get('phone'))
        if person or phone:
            contacts.append({'person': person, 'phone': phone})
    return contacts


def _validate_contacts_list(value, field_name):
    if value is None:
        return []
    if not isinstance(value, list):
        abort(400, description=f"`{field_name}` must be an array")

    contacts = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            abort(400, description=f"`{field_name}[{idx}]` must be an object")
        person = _clean_str(item.get('person'))
        phone = _clean_str(item.get('phone'))
        if person or phone:
            contacts.append({'person': person, 'phone': phone})
    return contacts


def _clean_liturgy_end_times(value):
    if not isinstance(value, dict):
        return {'Вт': '', 'Пт': '', 'Сб': ''}
    return {
        'Вт': _clean_str(value.get('Вт')),
        'Пт': _clean_str(value.get('Пт')),
        'Сб': _clean_str(value.get('Сб')),
    }


def _day_ids_to_weekdays(value):
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        day_id = _clean_str(item).lower()
        if day_id in DAY_ID_TO_WEEKDAY:
            out.append(DAY_ID_TO_WEEKDAY[day_id])
    return sorted(set(out))


def _weekdays_to_day_ids(value):
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        try:
            weekday = _normalize_weekday(item)
        except Exception:
            continue
        day_id = WEEKDAY_TO_DAY_ID.get(weekday)
        if day_id:
            out.append(day_id)
    return sorted(set(out), key=lambda day_id: DAY_ID_TO_WEEKDAY.get(day_id, 99))


def _legacy_liturgy_from_church_days(church_days):
    if not isinstance(church_days, dict):
        return {'Вт': '', 'Пт': '', 'Сб': ''}
    by_weekday = church_days.get('liturgyByWeekday')
    if not isinstance(by_weekday, dict):
        by_weekday = {}
    return {
        'Вт': _clean_str(by_weekday.get('2')),
        'Пт': _clean_str(by_weekday.get('5')),
        'Сб': _clean_str(by_weekday.get('6')),
    }


def _church_days_from_legacy(working_days, liturgy_end_times):
    active_weekdays = _day_ids_to_weekdays(working_days)
    legacy_times = _clean_liturgy_end_times(liturgy_end_times)
    liturgy_by_weekday = {}
    for legacy_key, weekday in LITURGY_KEY_TO_WEEKDAY.items():
        value = _clean_str(legacy_times.get(legacy_key))
        if value:
            liturgy_by_weekday[str(weekday)] = value

    if active_weekdays:
        liturgy_by_weekday = {
            key: value for key, value in liturgy_by_weekday.items() if int(key) in active_weekdays
        }

    return {
        'activeWeekdays': active_weekdays,
        'liturgyByWeekday': liturgy_by_weekday,
    }


def _clean_church_days(value, field_name='churchDays'):
    if value is None:
        return {'activeWeekdays': [], 'liturgyByWeekday': {}}
    if not isinstance(value, dict):
        abort(400, description=f"`{field_name}` must be an object")

    raw_active = value.get('activeWeekdays')
    if raw_active is None:
        active_weekdays = []
    elif isinstance(raw_active, list):
        active_weekdays = []
        for idx, item in enumerate(raw_active):
            try:
                active_weekdays.append(_normalize_weekday(item))
            except Exception:
                abort(400, description=f"`{field_name}.activeWeekdays[{idx}]` must be an integer 0..6")
        active_weekdays = sorted(set(active_weekdays))
    else:
        abort(400, description=f"`{field_name}.activeWeekdays` must be an array")

    raw_liturgy = value.get('liturgyByWeekday')
    if raw_liturgy is None:
        liturgy_by_weekday = {}
    elif isinstance(raw_liturgy, dict):
        liturgy_by_weekday = {}
        for key, raw_time in raw_liturgy.items():
            try:
                weekday = _normalize_weekday(key)
            except Exception:
                abort(400, description=f"`{field_name}.liturgyByWeekday` keys must be integers 0..6")
            time_value = _clean_str(raw_time)
            if time_value:
                liturgy_by_weekday[str(weekday)] = time_value
    else:
        abort(400, description=f"`{field_name}.liturgyByWeekday` must be an object")

    if active_weekdays:
        liturgy_by_weekday = {
            key: value for key, value in liturgy_by_weekday.items() if int(key) in active_weekdays
        }

    return {
        'activeWeekdays': active_weekdays,
        'liturgyByWeekday': liturgy_by_weekday,
    }


def _normalize_admin_church(church):
    location = normalize_church_location(church)
    legacy_loc = church_location_to_legacy(location)
    contacts = _clean_contacts_list_loose(church.get('contacts'))
    first_contact = contacts[0] if contacts else {}

    contact_person = _clean_str(church.get('contactPerson')) or _clean_str(first_contact.get('person'))
    if not contact_person:
        contact_person = '-'

    phone = _clean_str(church.get('phone')) or _clean_str(first_contact.get('phone'))
    if not phone:
        phone = '+380-(00)-000-0000'

    status = _clean_str(church.get('status'))
    if status not in ADMIN_CHURCH_STATUSES:
        status = 'Активний'

    working_days = _clean_str_list(church.get('workingDays'), 'workingDays') if isinstance(church.get('workingDays'), list) else []
    cemetery_refs = normalize_refs_list(church.get('cemeteryRefs') if isinstance(church.get('cemeteryRefs'), list) else church.get('cemeteries'))
    cemeteries = refs_to_legacy_names(cemetery_refs)
    liturgy_end_times = _clean_liturgy_end_times(church.get('liturgyEndTimes'))
    church_days = _clean_church_days(church.get('churchDays'))

    if not church_days['activeWeekdays'] and working_days:
        church_days['activeWeekdays'] = _day_ids_to_weekdays(working_days)
    if not church_days['liturgyByWeekday'] and any(liturgy_end_times.values()):
        church_days['liturgyByWeekday'] = _church_days_from_legacy(working_days, liturgy_end_times).get('liturgyByWeekday', {})
    if not working_days and church_days['activeWeekdays']:
        working_days = _weekdays_to_day_ids(church_days['activeWeekdays'])
    if not any(liturgy_end_times.values()) and church_days['liturgyByWeekday']:
        liturgy_end_times = _legacy_liturgy_from_church_days(church_days)

    created_at = church.get('createdAt')
    updated_at = church.get('updatedAt')

    return {
        'id': str(church.get('_id')),
        'name': _clean_str(church.get('name')),
        'address': _clean_str(church.get('address') or legacy_loc.get('address')),
        'locality': _clean_str(church.get('locality') or legacy_loc.get('locality')),
        'location': location,
        'contactPerson': contact_person,
        'phone': phone,
        'status': status,
        'workingDays': working_days,
        'liturgyEndTimes': liturgy_end_times,
        'churchDays': church_days,
        'cemeteries': cemeteries,
        'cemeteryRefs': cemetery_refs,
        'contacts': contacts,
        'infoNotes': _clean_str(church.get('infoNotes')),
        'iban': _clean_str(church.get('iban')),
        'taxCode': _clean_str(church.get('taxCode')),
        'recipientName': _clean_str(church.get('recipientName')),
        'botCode': _clean_str(church.get('botCode')),
        'createdAt': created_at.isoformat() if isinstance(created_at, datetime) else None,
        'updatedAt': updated_at.isoformat() if isinstance(updated_at, datetime) else None,
    }


def _build_admin_church_payload(data, partial=False):
    if not isinstance(data, dict):
        abort(400, description='Body must be a JSON object')

    unknown = set(data.keys()) - ADMIN_CHURCH_ALLOWED_FIELDS
    if unknown:
        unknown_list = ', '.join(sorted(unknown))
        abort(400, description=f'Unsupported fields: {unknown_list}')

    payload = {}

    def _validate_required_text(key, required):
        if key not in data:
            if required:
                abort(400, description=f"`{key}` is required")
            return
        cleaned = _clean_str(data.get(key))
        if not cleaned:
            abort(400, description=f"`{key}` is required")
        payload[key] = cleaned

    _validate_required_text('name', required=not partial)
    if not partial:
        if _location_admin_strict_geonames_enabled():
            if 'location' not in data:
                abort(400, description="`location` is required and must come from provider suggestions")
        elif 'location' not in data:
            _validate_required_text('address', required=True)

    location_payload = None
    if 'location' in data:
        raw_location = data.get('location')
        if raw_location is not None and not isinstance(raw_location, dict):
            abort(400, description="`location` must be an object")
        location_payload = normalize_location_core(raw_location or {})
        if _location_admin_strict_geonames_enabled():
            _validate_admin_strict_location('location', location_payload)
        if _location_write_is_canonical() or _location_write_is_dual():
            payload['location'] = location_payload

    if 'locality' in data:
        payload['locality'] = _clean_str(data.get('locality'))

    if 'workingDays' in data:
        payload['workingDays'] = _clean_str_list(data.get('workingDays'), 'workingDays')

    if 'liturgyEndTimes' in data:
        payload['liturgyEndTimes'] = _clean_liturgy_end_times(data.get('liturgyEndTimes'))

    if 'churchDays' in data:
        payload['churchDays'] = _clean_church_days(data.get('churchDays'))

    if 'cemeteries' in data:
        payload['cemeteries'] = _clean_str_list(data.get('cemeteries'), 'cemeteries')
    if 'cemeteryRefs' in data:
        if not isinstance(data.get('cemeteryRefs'), list):
            abort(400, description="`cemeteryRefs` must be an array")
        payload['cemeteryRefs'] = normalize_refs_list(data.get('cemeteryRefs'))

    if 'status' in data:
        status = _clean_str(data.get('status'))
        if status not in ADMIN_CHURCH_STATUSES:
            abort(400, description="`status` must be one of: Активний, Неактивний")
        payload['status'] = status

    if 'contacts' in data:
        payload['contacts'] = _validate_contacts_list(data.get('contacts'), 'contacts')

    if 'contactPerson' in data:
        payload['contactPerson'] = _clean_str(data.get('contactPerson'))

    if 'phone' in data:
        payload['phone'] = _clean_str(data.get('phone'))

    if 'infoNotes' in data:
        payload['infoNotes'] = _clean_str(data.get('infoNotes'))

    if 'iban' in data:
        payload['iban'] = _clean_str(data.get('iban'))

    if 'taxCode' in data:
        payload['taxCode'] = _clean_str(data.get('taxCode'))

    if 'recipientName' in data:
        payload['recipientName'] = _clean_str(data.get('recipientName'))

    if 'botCode' in data:
        payload['botCode'] = _clean_str(data.get('botCode'))

    if 'contacts' in payload and payload['contacts']:
        if not payload.get('contactPerson'):
            payload['contactPerson'] = payload['contacts'][0].get('person', '')
        if not payload.get('phone'):
            payload['phone'] = payload['contacts'][0].get('phone', '')

    if 'cemeteryRefs' in payload and 'cemeteries' not in payload:
        payload['cemeteries'] = refs_to_legacy_names(payload['cemeteryRefs'])
    if 'cemeteries' in payload and 'cemeteryRefs' not in payload:
        payload['cemeteryRefs'] = normalize_refs_list(payload['cemeteries'])

    if 'infoNotes' in payload:
        payload['description'] = payload['infoNotes']

    legacy_address = payload.get('address')
    legacy_locality = payload.get('locality')
    if location_payload is None and (
        ('address' in payload) or ('locality' in payload) or ('address' in data) or ('locality' in data)
    ):
        location_payload = normalize_location_core({
            'area': {'display': legacy_locality or ''},
            'addressLine': legacy_address or '',
        })
        if _location_write_is_canonical() or _location_write_is_dual():
            payload['location'] = location_payload

    if location_payload and (_location_write_is_legacy() or _location_write_is_dual()):
        legacy_loc = church_location_to_legacy(location_payload)
        payload['locality'] = legacy_loc.get('locality', '')
        payload['address'] = legacy_loc.get('address', '')

    if 'churchDays' in payload:
        if 'workingDays' not in payload:
            payload['workingDays'] = _weekdays_to_day_ids(payload['churchDays'].get('activeWeekdays', []))
        if 'liturgyEndTimes' not in payload:
            payload['liturgyEndTimes'] = _legacy_liturgy_from_church_days(payload['churchDays'])
    elif 'workingDays' in payload or 'liturgyEndTimes' in payload:
        payload['churchDays'] = _church_days_from_legacy(
            payload.get('workingDays', []),
            payload.get('liturgyEndTimes', {'Вт': '', 'Пт': '', 'Сб': ''})
        )

    if not partial:
        payload.setdefault('locality', '')
        payload.setdefault('workingDays', [])
        payload.setdefault('liturgyEndTimes', {'Вт': '', 'Пт': '', 'Сб': ''})
        payload.setdefault('churchDays', _church_days_from_legacy(payload.get('workingDays', []), payload.get('liturgyEndTimes', {'Вт': '', 'Пт': '', 'Сб': ''})))
        payload.setdefault('cemeteryRefs', [])
        payload.setdefault('cemeteries', refs_to_legacy_names(payload.get('cemeteryRefs')))
        payload.setdefault('status', 'Активний')
        payload.setdefault('contacts', [])
        payload.setdefault('contactPerson', '')
        payload.setdefault('phone', '')
        payload.setdefault('infoNotes', '')
        payload.setdefault('iban', '')
        payload.setdefault('taxCode', '')
        payload.setdefault('recipientName', '')
        payload.setdefault('botCode', '')
        payload.setdefault('description', payload.get('infoNotes', ''))

        if 'location' not in payload and (_location_write_is_canonical() or _location_write_is_dual()):
            payload['location'] = normalize_location_core({
                'area': {'display': payload.get('locality', '')},
                'addressLine': payload.get('address', ''),
            })
        if ('address' not in payload or 'locality' not in payload) and payload.get('location'):
            legacy_loc = church_location_to_legacy(payload.get('location'))
            payload.setdefault('locality', legacy_loc.get('locality', ''))
            payload.setdefault('address', legacy_loc.get('address', ''))

    return payload


@application.route('/api/admin/churches', methods=['GET'])
def admin_list_churches():
    search = _clean_str(request.args.get('search', ''))
    query_filter = {}
    if search:
        regex = {'$regex': re.escape(search), '$options': 'i'}
        query_filter = {
            '$or': [
                {'name': regex},
                {'address': regex},
                {'locality': regex},
                {'location.display': regex},
                {'contactPerson': regex},
                {'phone': regex},
            ]
        }

    churches_cursor = churches_collection.find(query_filter).sort([('name', 1), ('_id', 1)]).limit(50)
    churches_list = [_normalize_admin_church(church) for church in churches_cursor]

    return jsonify({
        'total': len(churches_list),
        'churches': churches_list
    })


@application.route('/api/admin/churches', methods=['POST'])
def admin_create_church():
    data = request.get_json(silent=True) or {}
    payload = _build_admin_church_payload(data, partial=False)
    now = datetime.utcnow()
    payload['createdAt'] = now
    payload['updatedAt'] = now

    inserted = churches_collection.insert_one(payload)
    created = churches_collection.find_one({'_id': inserted.inserted_id})
    return jsonify(_normalize_admin_church(created)), 201


@application.route('/api/admin/churches/<string:church_id>', methods=['PATCH'])
def admin_update_church(church_id):
    try:
        oid = ObjectId(church_id)
    except Exception:
        abort(400, description='Invalid church id')

    data = request.get_json(silent=True) or {}
    update_fields = _build_admin_church_payload(data, partial=True)
    if not update_fields:
        abort(400, description='Nothing to update')

    update_fields['updatedAt'] = datetime.utcnow()
    result = churches_collection.update_one({'_id': oid}, {'$set': update_fields})
    if result.matched_count == 0:
        abort(404, description='Church not found')

    church = churches_collection.find_one({'_id': oid})
    return jsonify(_normalize_admin_church(church))


@application.route('/api/admin/churches/<string:church_id>', methods=['DELETE'])
def admin_delete_church(church_id):
    try:
        oid = ObjectId(church_id)
    except Exception:
        abort(400, description='Invalid church id')

    result = churches_collection.delete_one({'_id': oid})
    if result.deleted_count == 0:
        abort(404, description='Church not found')

    return jsonify({'ok': True})


ADMIN_NOTES_SUCCESS_PAYMENT_STATUSES = {'success', 'paid', 'completed'}
ADMIN_NOTES_STATUS_QUEUED = 'На черзі'
ADMIN_NOTES_STATUS_DONE = 'Відбулася'
DEFAULT_ADMIN_NOTES_TIMEZONE = 'Europe/Kyiv'
ADMIN_NOTES_WEEKDAY_LABELS = ['Нд', 'Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб']
ADMIN_NOTES_SUCCESS_QUERY_VALUES = sorted(
    {status_variant for status in ADMIN_NOTES_SUCCESS_PAYMENT_STATUSES for status_variant in {
        status,
        status.upper(),
        status.capitalize(),
    }}
)


def _admin_notes_resolve_timezone(raw_timezone, fallback_reasons):
    timezone_name = _clean_str(raw_timezone) or DEFAULT_ADMIN_NOTES_TIMEZONE
    alias_map = {
        'Europe/Kiev': 'Europe/Kyiv',
    }
    timezone_name = alias_map.get(timezone_name, timezone_name)

    if ZoneInfo is None:
        fallback_reasons.add('Таймзона ZoneInfo недоступна, використано UTC.')
        return timezone.utc, 'UTC'

    try:
        return ZoneInfo(timezone_name), timezone_name
    except Exception:
        fallback_reasons.add(f'Некоректна таймзона "{timezone_name}", використано {DEFAULT_ADMIN_NOTES_TIMEZONE}.')
        return ZoneInfo(DEFAULT_ADMIN_NOTES_TIMEZONE), DEFAULT_ADMIN_NOTES_TIMEZONE


def _admin_notes_parse_datetime(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        normalized = cleaned.replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(normalized)
        except Exception:
            return None
    return None


def _admin_notes_to_local_datetime(value, tzinfo):
    dt_value = _admin_notes_parse_datetime(value)
    if not dt_value:
        return None
    if dt_value.tzinfo is None:
        return dt_value.replace(tzinfo=tzinfo)
    try:
        return dt_value.astimezone(tzinfo)
    except Exception:
        return dt_value.replace(tzinfo=tzinfo)


def _admin_notes_format_ua_date(dt_value):
    if not isinstance(dt_value, datetime):
        return ''
    return dt_value.strftime('%d.%m.%Y')


def _admin_notes_parse_ua_date(value):
    cleaned = _clean_str(value)
    if not cleaned:
        return None
    match = re.match(r'^(\d{2})\.(\d{2})\.(\d{4})$', cleaned)
    if not match:
        return None
    day = int(match.group(1))
    month = int(match.group(2))
    year = int(match.group(3))
    try:
        return datetime(year, month, day).date()
    except Exception:
        return None


def _admin_notes_normalize_payment_status(value):
    return _clean_str(value).lower()


def _admin_notes_is_success_payment(value):
    return _admin_notes_normalize_payment_status(value) in ADMIN_NOTES_SUCCESS_PAYMENT_STATUSES


def _admin_notes_parse_hhmm(value):
    cleaned = _clean_str(value)
    if not cleaned:
        return None
    match = re.match(r'^(\d{1,2})[:.](\d{2})$', cleaned)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return hour, minute


def _admin_notes_weekday_from_value(value):
    try:
        weekday = int(str(value).strip())
    except Exception:
        return None
    if weekday < 0 or weekday > 6:
        return None
    return weekday


def _admin_notes_end_times_by_weekday(church_doc):
    out = {}
    if not isinstance(church_doc, dict):
        return out

    church_days = church_doc.get('churchDays')
    if isinstance(church_days, dict):
        liturgy_by_weekday = church_days.get('liturgyByWeekday')
        if isinstance(liturgy_by_weekday, dict):
            for weekday_raw, time_raw in liturgy_by_weekday.items():
                weekday = _admin_notes_weekday_from_value(weekday_raw)
                parsed_time = _admin_notes_parse_hhmm(time_raw)
                if weekday is None or not parsed_time:
                    continue
                out[weekday] = parsed_time

    legacy_times = church_doc.get('liturgyEndTimes')
    if isinstance(legacy_times, dict):
        for legacy_key, weekday in LITURGY_KEY_TO_WEEKDAY.items():
            if weekday in out:
                continue
            parsed_time = _admin_notes_parse_hhmm(legacy_times.get(legacy_key))
            if parsed_time:
                out[weekday] = parsed_time

    return out


def _admin_notes_resolve_status(service_local_dt, church_doc, now_local_dt, fallback_reasons):
    if not isinstance(service_local_dt, datetime):
        fallback_reasons.add('Для частини записок відсутня дата служби, статус встановлено "На черзі".')
        return ADMIN_NOTES_STATUS_QUEUED

    service_date = service_local_dt.date()
    today = now_local_dt.date()
    if service_date < today:
        return ADMIN_NOTES_STATUS_DONE
    if service_date > today:
        return ADMIN_NOTES_STATUS_QUEUED

    if not isinstance(church_doc, dict):
        fallback_reasons.add('Для частини записок не знайдено церкву, статус залишено "На черзі".')
        return ADMIN_NOTES_STATUS_QUEUED

    sunday_first_weekday = (service_date.weekday() + 1) % 7
    end_time_by_weekday = _admin_notes_end_times_by_weekday(church_doc)
    end_time = end_time_by_weekday.get(sunday_first_weekday)
    if not end_time:
        fallback_reasons.add('Для частини церков не налаштовано час завершення літургії, статус залишено "На черзі".')
        return ADMIN_NOTES_STATUS_QUEUED

    now_minutes = now_local_dt.hour * 60 + now_local_dt.minute
    end_minutes = end_time[0] * 60 + end_time[1]
    return ADMIN_NOTES_STATUS_DONE if now_minutes >= end_minutes else ADMIN_NOTES_STATUS_QUEUED


def _admin_notes_church_display(church_doc):
    name = _clean_str(church_doc.get('name'))
    location = church_doc.get('location')
    location_display = ''
    if isinstance(location, dict):
        location_display = _clean_str(location.get('display'))
    address = _clean_str(church_doc.get('address') or church_doc.get('locality') or location_display)
    return {
        'id': str(church_doc.get('_id')),
        'name': name,
        'address': address,
    }


def _admin_notes_build_church_maps():
    projection = {
        'name': 1,
        'address': 1,
        'locality': 1,
        'location': 1,
        'churchDays': 1,
        'liturgyEndTimes': 1,
    }
    church_docs = list(churches_collection.find({}, projection).sort([('name', 1), ('_id', 1)]))

    by_id = {}
    by_name = {}
    rows = []
    rows_by_id = {}
    for church_doc in church_docs:
        church_id = str(church_doc.get('_id'))
        church_display = _admin_notes_church_display(church_doc)
        rows.append({
            'id': church_display['id'],
            'name': church_display['name'],
            'address': church_display['address'],
            'queued': 0,
            'placedToday': 0,
            'sentToday': 0,
        })
        rows_by_id[church_id] = rows[-1]
        by_id[church_id] = church_doc

        normalized_name = _clean_str(church_display['name']).casefold()
        if normalized_name and normalized_name not in by_name:
            by_name[normalized_name] = church_doc

    return church_docs, by_id, by_name, rows, rows_by_id


def _admin_notes_resolve_church_for_liturgy(liturgy_doc, church_by_id, church_by_name):
    raw_church_id = _clean_str(liturgy_doc.get('churchId'))
    if raw_church_id and raw_church_id in church_by_id:
        return church_by_id[raw_church_id]

    raw_church_name = _clean_str(liturgy_doc.get('churchName'))
    if raw_church_name:
        return church_by_name.get(raw_church_name.casefold())

    return None


def _admin_notes_format_donation(value):
    if value is None or value == '':
        return '-'

    if isinstance(value, bool):
        return '-'

    if isinstance(value, (int, float)):
        amount = int(value) if float(value).is_integer() else round(float(value), 2)
        return f'{amount} грн'

    cleaned = _clean_str(value)
    if not cleaned:
        return '-'
    if 'грн' in cleaned.lower():
        return cleaned
    return f'{cleaned} грн'


def _admin_notes_extract_person_oid(liturgy_doc):
    person_value = liturgy_doc.get('person')
    if isinstance(person_value, ObjectId):
        return person_value

    if isinstance(person_value, str):
        cleaned_person = person_value.strip()
        if cleaned_person:
            try:
                return ObjectId(cleaned_person)
            except Exception:
                return None

    person_id_value = liturgy_doc.get('personId')
    if isinstance(person_id_value, str):
        cleaned_person_id = person_id_value.strip()
        if cleaned_person_id:
            try:
                return ObjectId(cleaned_person_id)
            except Exception:
                return None

    return None


def _admin_notes_build_lifespan(person_doc):
    if not isinstance(person_doc, dict):
        return ''
    birth_year = _clean_str(person_doc.get('birthYear'))
    death_year = _clean_str(person_doc.get('deathYear'))
    if birth_year and death_year:
        return f'{birth_year}-{death_year}'
    if birth_year:
        return f'{birth_year}-'
    if death_year:
        return f'-{death_year}'
    return ''


def _admin_notes_matches_search(search_query, values):
    if not search_query:
        return True
    haystack = ' '.join(_clean_str(value).casefold() for value in values)
    return search_query in haystack


@application.route('/api/admin/notes/overview', methods=['GET'])
def admin_notes_overview():
    search = _clean_str(request.args.get('search')).casefold()
    fallback_reasons = set()
    timezone_value = _clean_str(request.args.get('timezone'))
    tzinfo, resolved_timezone = _admin_notes_resolve_timezone(timezone_value, fallback_reasons)
    now_local = datetime.now(tzinfo)
    today = now_local.date()

    _, church_by_id, church_by_name, church_rows, church_rows_by_id = _admin_notes_build_church_maps()

    projection = {
        '_id': 1,
        'churchId': 1,
        'churchName': 1,
        'serviceDate': 1,
        'createdAt': 1,
        'paymentStatus': 1,
    }
    liturgy_cursor = liturgies_collection.find(
        {'paymentStatus': {'$in': ADMIN_NOTES_SUCCESS_QUERY_VALUES}},
        projection,
    )

    kpi = {
        'queued': 0,
        'placedToday': 0,
        'sentToday': 0,
    }

    for liturgy_doc in liturgy_cursor:
        if not _admin_notes_is_success_payment(liturgy_doc.get('paymentStatus')):
            continue

        church_doc = _admin_notes_resolve_church_for_liturgy(liturgy_doc, church_by_id, church_by_name)
        church_row = None
        if church_doc:
            church_row = church_rows_by_id.get(str(church_doc.get('_id')))

        service_local_dt = _admin_notes_to_local_datetime(liturgy_doc.get('serviceDate'), tzinfo)
        created_local_dt = _admin_notes_to_local_datetime(liturgy_doc.get('createdAt'), tzinfo)
        status = _admin_notes_resolve_status(service_local_dt, church_doc, now_local, fallback_reasons)

        if status == ADMIN_NOTES_STATUS_QUEUED:
            kpi['queued'] += 1
            if church_row:
                church_row['queued'] += 1

        if isinstance(service_local_dt, datetime) and service_local_dt.date() == today:
            kpi['placedToday'] += 1
            if church_row:
                church_row['placedToday'] += 1

        if isinstance(created_local_dt, datetime) and created_local_dt.date() == today:
            kpi['sentToday'] += 1
            if church_row:
                church_row['sentToday'] += 1

    if search:
        filtered_church_rows = [
            row for row in church_rows
            if _admin_notes_matches_search(search, [row.get('name'), row.get('address')])
        ]
    else:
        filtered_church_rows = church_rows

    return jsonify({
        'kpi': kpi,
        'churches': filtered_church_rows,
        'meta': {
            'timezone': resolved_timezone,
            'isFallback': bool(fallback_reasons),
            'fallbackReason': sorted(fallback_reasons),
        },
    })


@application.route('/api/admin/notes/graph', methods=['GET'])
def admin_notes_graph():
    period = _clean_str(request.args.get('period')).lower() or 'month'
    if period not in {'day', 'week', 'month'}:
        abort(400, description='`period` must be one of: day, week, month')

    search = _clean_str(request.args.get('search')).casefold()
    fallback_reasons = set()
    timezone_value = _clean_str(request.args.get('timezone'))
    tzinfo, resolved_timezone = _admin_notes_resolve_timezone(timezone_value, fallback_reasons)
    now_local = datetime.now(tzinfo)

    _, church_by_id, church_by_name, _, _ = _admin_notes_build_church_maps()

    points = []
    if period == 'day':
        labels = [f'{hour:02d}:00' for hour in range(0, 24, 3)]
        counts = [0 for _ in labels]
        today = now_local.date()

        projection = {
            'churchId': 1,
            'churchName': 1,
            'personName': 1,
            'phone': 1,
            'createdAt': 1,
            'serviceDate': 1,
            'paymentStatus': 1,
        }
        liturgy_cursor = liturgies_collection.find({'paymentStatus': {'$in': ADMIN_NOTES_SUCCESS_QUERY_VALUES}}, projection)
        for liturgy_doc in liturgy_cursor:
            if not _admin_notes_is_success_payment(liturgy_doc.get('paymentStatus')):
                continue

            church_doc = _admin_notes_resolve_church_for_liturgy(liturgy_doc, church_by_id, church_by_name)
            church_address = _admin_notes_church_display(church_doc).get('address') if church_doc else ''
            if not _admin_notes_matches_search(search, [
                liturgy_doc.get('churchName'),
                liturgy_doc.get('personName'),
                liturgy_doc.get('phone'),
                church_address,
            ]):
                continue

            event_local_dt = _admin_notes_to_local_datetime(liturgy_doc.get('createdAt'), tzinfo)
            if not event_local_dt:
                event_local_dt = _admin_notes_to_local_datetime(liturgy_doc.get('serviceDate'), tzinfo)
                if event_local_dt:
                    fallback_reasons.add('Для частини точок графіка відсутній createdAt, використано serviceDate.')
            if not event_local_dt or event_local_dt.date() != today:
                continue

            bucket = min(event_local_dt.hour // 3, len(counts) - 1)
            counts[bucket] += 1

        points = [{'label': labels[index], 'value': counts[index]} for index in range(len(labels))]

    elif period == 'week':
        day_list = [now_local.date() - timedelta(days=delta) for delta in range(6, -1, -1)]
        index_by_day = {day: index for index, day in enumerate(day_list)}
        labels = [ADMIN_NOTES_WEEKDAY_LABELS[(day.weekday() + 1) % 7] for day in day_list]
        counts = [0 for _ in day_list]

        projection = {
            'churchId': 1,
            'churchName': 1,
            'personName': 1,
            'phone': 1,
            'createdAt': 1,
            'serviceDate': 1,
            'paymentStatus': 1,
        }
        liturgy_cursor = liturgies_collection.find({'paymentStatus': {'$in': ADMIN_NOTES_SUCCESS_QUERY_VALUES}}, projection)
        for liturgy_doc in liturgy_cursor:
            if not _admin_notes_is_success_payment(liturgy_doc.get('paymentStatus')):
                continue

            church_doc = _admin_notes_resolve_church_for_liturgy(liturgy_doc, church_by_id, church_by_name)
            church_address = _admin_notes_church_display(church_doc).get('address') if church_doc else ''
            if not _admin_notes_matches_search(search, [
                liturgy_doc.get('churchName'),
                liturgy_doc.get('personName'),
                liturgy_doc.get('phone'),
                church_address,
            ]):
                continue

            event_local_dt = _admin_notes_to_local_datetime(liturgy_doc.get('createdAt'), tzinfo)
            if not event_local_dt:
                event_local_dt = _admin_notes_to_local_datetime(liturgy_doc.get('serviceDate'), tzinfo)
                if event_local_dt:
                    fallback_reasons.add('Для частини точок графіка відсутній createdAt, використано serviceDate.')
            if not event_local_dt:
                continue

            event_day = event_local_dt.date()
            if event_day not in index_by_day:
                continue
            counts[index_by_day[event_day]] += 1

        points = [{'label': labels[index], 'value': counts[index]} for index in range(len(labels))]

    else:
        day_list = [now_local.date() - timedelta(days=delta) for delta in range(29, -1, -1)]
        index_by_day = {day: index for index, day in enumerate(day_list)}
        labels = [day.strftime('%d.%m') for day in day_list]
        counts = [0 for _ in day_list]

        projection = {
            'churchId': 1,
            'churchName': 1,
            'personName': 1,
            'phone': 1,
            'createdAt': 1,
            'serviceDate': 1,
            'paymentStatus': 1,
        }
        liturgy_cursor = liturgies_collection.find({'paymentStatus': {'$in': ADMIN_NOTES_SUCCESS_QUERY_VALUES}}, projection)
        for liturgy_doc in liturgy_cursor:
            if not _admin_notes_is_success_payment(liturgy_doc.get('paymentStatus')):
                continue

            church_doc = _admin_notes_resolve_church_for_liturgy(liturgy_doc, church_by_id, church_by_name)
            church_address = _admin_notes_church_display(church_doc).get('address') if church_doc else ''
            if not _admin_notes_matches_search(search, [
                liturgy_doc.get('churchName'),
                liturgy_doc.get('personName'),
                liturgy_doc.get('phone'),
                church_address,
            ]):
                continue

            event_local_dt = _admin_notes_to_local_datetime(liturgy_doc.get('createdAt'), tzinfo)
            if not event_local_dt:
                event_local_dt = _admin_notes_to_local_datetime(liturgy_doc.get('serviceDate'), tzinfo)
                if event_local_dt:
                    fallback_reasons.add('Для частини точок графіка відсутній createdAt, використано serviceDate.')
            if not event_local_dt:
                continue

            event_day = event_local_dt.date()
            if event_day not in index_by_day:
                continue
            counts[index_by_day[event_day]] += 1

        points = [{'label': labels[index], 'value': counts[index]} for index in range(len(labels))]

    return jsonify({
        'period': period,
        'points': points,
        'meta': {
            'timezone': resolved_timezone,
            'isFallback': bool(fallback_reasons),
            'fallbackReason': sorted(fallback_reasons),
        },
    })


@application.route('/api/admin/notes/church-details', methods=['GET'])
def admin_notes_church_details():
    raw_church_id = _clean_str(request.args.get('churchId'))
    if not raw_church_id:
        abort(400, description='`churchId` is required')

    try:
        church_oid = ObjectId(raw_church_id)
    except Exception:
        abort(400, description='Invalid `churchId`')

    search = _clean_str(request.args.get('search')).casefold()
    date_from_raw = _clean_str(request.args.get('dateFrom'))
    date_to_raw = _clean_str(request.args.get('dateTo'))
    offset_raw = _clean_str(request.args.get('offset'))
    limit_raw = _clean_str(request.args.get('limit'))

    date_from = _admin_notes_parse_ua_date(date_from_raw) if date_from_raw else None
    date_to = _admin_notes_parse_ua_date(date_to_raw) if date_to_raw else None
    if date_from_raw and date_from is None:
        abort(400, description='`dateFrom` must be in format DD.MM.YYYY')
    if date_to_raw and date_to is None:
        abort(400, description='`dateTo` must be in format DD.MM.YYYY')
    if date_from and date_to and date_from > date_to:
        abort(400, description='`dateFrom` must be less or equal to `dateTo`')

    try:
        offset = int(offset_raw) if offset_raw else 0
    except Exception:
        abort(400, description='`offset` must be an integer')
    try:
        limit = int(limit_raw) if limit_raw else 200
    except Exception:
        abort(400, description='`limit` must be an integer')

    if offset < 0:
        abort(400, description='`offset` must be >= 0')
    if limit <= 0:
        abort(400, description='`limit` must be > 0')
    if limit > 500:
        limit = 500

    fallback_reasons = set()
    timezone_value = _clean_str(request.args.get('timezone'))
    tzinfo, resolved_timezone = _admin_notes_resolve_timezone(timezone_value, fallback_reasons)
    now_local = datetime.now(tzinfo)

    church_projection = {
        'name': 1,
        'address': 1,
        'locality': 1,
        'location': 1,
        'churchDays': 1,
        'liturgyEndTimes': 1,
    }
    church_doc = churches_collection.find_one({'_id': church_oid}, church_projection)
    if not church_doc:
        abort(404, description='Church not found')

    church_display = _admin_notes_church_display(church_doc)
    church_name = church_display.get('name')
    if not church_name:
        fallback_reasons.add('Для обраної церкви відсутня назва, результати можуть бути неповними.')

    match_by_name = {'$regex': f'^{re.escape(church_name)}$', '$options': 'i'} if church_name else None
    liturgy_query = {
        'paymentStatus': {'$in': ADMIN_NOTES_SUCCESS_QUERY_VALUES},
    }
    if match_by_name:
        liturgy_query['$or'] = [
            {'churchName': match_by_name},
            {'churchId': raw_church_id},
        ]
    else:
        liturgy_query['churchId'] = raw_church_id

    projection = {
        '_id': 1,
        'person': 1,
        'personId': 1,
        'personName': 1,
        'price': 1,
        'phone': 1,
        'serviceDate': 1,
        'createdAt': 1,
        'paymentStatus': 1,
        'churchName': 1,
        'churchId': 1,
    }
    liturgy_docs = list(
        liturgies_collection.find(liturgy_query, projection).sort([('serviceDate', ASCENDING), ('createdAt', ASCENDING), ('_id', ASCENDING)])
    )

    person_oids = []
    seen_person_ids = set()
    for liturgy_doc in liturgy_docs:
        person_oid = _admin_notes_extract_person_oid(liturgy_doc)
        if not person_oid:
            continue
        person_oid_key = str(person_oid)
        if person_oid_key in seen_person_ids:
            continue
        seen_person_ids.add(person_oid_key)
        person_oids.append(person_oid)

    people_by_id = {}
    if person_oids:
        for person_doc in people_collection.find(
            {'_id': {'$in': person_oids}},
            {'name': 1, 'birthYear': 1, 'deathYear': 1, 'avatarUrl': 1}
        ):
            people_by_id[str(person_doc.get('_id'))] = person_doc

    rows = []
    for liturgy_doc in liturgy_docs:
        if not _admin_notes_is_success_payment(liturgy_doc.get('paymentStatus')):
            continue

        service_local_dt = _admin_notes_to_local_datetime(liturgy_doc.get('serviceDate'), tzinfo)
        created_local_dt = _admin_notes_to_local_datetime(liturgy_doc.get('createdAt'), tzinfo)

        service_day = service_local_dt.date() if isinstance(service_local_dt, datetime) else None
        if date_from and service_day and service_day < date_from:
            continue
        if date_to and service_day and service_day > date_to:
            continue
        if (date_from or date_to) and service_day is None:
            fallback_reasons.add('Для частини записок неможливо застосувати фільтр періоду через відсутню дату служби.')

        person_oid = _admin_notes_extract_person_oid(liturgy_doc)
        person_doc = people_by_id.get(str(person_oid)) if person_oid else None
        if person_oid and not person_doc:
            fallback_reasons.add('Для частини записок відсутній профіль особи, використано дані літургії.')

        person_name = _clean_str(person_doc.get('name')) if isinstance(person_doc, dict) else ''
        if not person_name:
            person_name = _clean_str(liturgy_doc.get('personName')) or 'Невідома особа'

        donation_label = _admin_notes_format_donation(liturgy_doc.get('price'))
        phone = _clean_str(liturgy_doc.get('phone'))
        status_label = _admin_notes_resolve_status(service_local_dt, church_doc, now_local, fallback_reasons)
        note_date_label = _admin_notes_format_ua_date(service_local_dt)
        created_date_label = _admin_notes_format_ua_date(created_local_dt)

        if not _admin_notes_matches_search(search, [person_name, phone, donation_label]):
            continue

        rows.append({
            'id': str(liturgy_doc.get('_id')),
            'person': person_name,
            'lifespan': _admin_notes_build_lifespan(person_doc),
            'donation': donation_label,
            'phone': phone,
            'noteDate': note_date_label,
            'createdAt': created_date_label,
            'status': status_label,
            'avatarUrl': _clean_str(person_doc.get('avatarUrl')) if isinstance(person_doc, dict) else '',
        })

    total_rows = len(rows)
    paged_rows = rows[offset: offset + limit]

    return jsonify({
        'church': {
            'id': church_display.get('id'),
            'name': church_display.get('name'),
            'address': church_display.get('address'),
        },
        'rows': paged_rows,
        'total': total_rows,
        'offset': offset,
        'limit': limit,
        'meta': {
            'timezone': resolved_timezone,
            'isFallback': bool(fallback_reasons),
            'fallbackReason': sorted(fallback_reasons),
        },
    })


ADMIN_RITUAL_SERVICE_STATUSES = {'Активний', 'Не активний'}
DEFAULT_RITUAL_SERVICE_CATEGORIES = ['Ресторани', 'Ритуальні послуги', 'Юристи']
ADMIN_RITUAL_SERVICE_ALLOWED_FIELDS = {
    'name',
    'category',
    'status',
    'logo',
    'banner',
    'link',
    'login',
    'password',
    'settlements',
    'contacts',
    'payments',
    'latitude',
    'longitude',
    'hqLocation',
    'serviceAreaIds',
    'description',
    'items',
}


def _normalize_ritual_service_status(value):
    cleaned = _clean_str(value)
    if cleaned == 'Неактивний':
        cleaned = 'Не активний'
    if cleaned not in ADMIN_RITUAL_SERVICE_STATUSES:
        return 'Активний'
    return cleaned


def _clean_ritual_contacts_loose(value):
    if not isinstance(value, list):
        return []

    contacts = []
    for item in value:
        if not isinstance(item, dict):
            continue
        address = _clean_str(item.get('address'))
        phone = _clean_str(item.get('phone'))
        if address or phone:
            contacts.append({'address': address, 'phone': phone})
    return contacts


def _validate_ritual_contacts(value, field_name='contacts'):
    if value is None:
        return []
    if not isinstance(value, list):
        abort(400, description=f"`{field_name}` must be an array")

    contacts = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            abort(400, description=f"`{field_name}[{idx}]` must be an object")
        address = _clean_str(item.get('address'))
        phone = _clean_str(item.get('phone'))
        if address or phone:
            contacts.append({'address': address, 'phone': phone})
    return contacts


def _coerce_ritual_addresses(value):
    if isinstance(value, list):
        return _clean_str_list(value, 'address')
    as_text = _clean_str(value)
    return [as_text] if as_text else []


def _coerce_ritual_phones(value):
    if isinstance(value, list):
        return _clean_str_list(value, 'phone')
    as_text = _clean_str(value)
    return [as_text] if as_text else []


def _derive_ritual_contacts_from_legacy(ritual_service):
    addresses = _coerce_ritual_addresses(ritual_service.get('address'))
    phones = _coerce_ritual_phones(ritual_service.get('phone'))
    length = max(len(addresses), len(phones))
    if length == 0:
        return []

    contacts = []
    for idx in range(length):
        address = addresses[idx] if idx < len(addresses) else ''
        phone = phones[idx] if idx < len(phones) else ''
        if address or phone:
            contacts.append({'address': address, 'phone': phone})
    return contacts


def _parse_ritual_period_str(period_text):
    cleaned = _clean_str(period_text)
    if not cleaned:
        return None

    match = re.match(r'^(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})\s*\((\d+)\s+днів\)$', cleaned)
    if not match:
        return None

    start_text, end_text, days_text = match.group(1), match.group(2), match.group(3)
    try:
        datetime.strptime(start_text, '%d.%m.%Y')
        end_dt = datetime.strptime(end_text, '%d.%m.%Y')
        days = int(days_text)
    except Exception:
        return None

    return {
        'startDate': start_text,
        'endDate': end_text,
        'days': max(0, days),
        'period': f'{start_text} - {end_text} ({max(0, days)} днів)',
        'endDateObj': end_dt,
    }


def _clean_ritual_payments(value, field_name='payments'):
    if value is None:
        return []
    if not isinstance(value, list):
        abort(400, description=f"`{field_name}` must be an array")

    payments = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            abort(400, description=f"`{field_name}[{idx}]` must be an object")

        period = _clean_str(item.get('period'))
        start_date = _clean_str(item.get('startDate')) or _clean_str(item.get('periodStart'))
        end_date = _clean_str(item.get('endDate')) or _clean_str(item.get('periodEnd'))
        raw_pdf_label = _clean_str(item.get('pdfLabel'))
        pdf_url = _clean_str(item.get('pdfUrl'))
        days_raw = item.get('days')
        if days_raw is None or days_raw == '':
            days_raw = item.get('periodDays')
        has_any_data = bool(
            period
            or start_date
            or end_date
            or (days_raw is not None and days_raw != '')
            or raw_pdf_label
            or pdf_url
        )
        if not has_any_data:
            continue

        pdf_label = raw_pdf_label or 'pdf про оплату'
        try:
            days = int(days_raw) if days_raw is not None and days_raw != '' else None
        except Exception:
            abort(400, description=f"`{field_name}[{idx}].days` must be an integer")

        parsed_period = _parse_ritual_period_str(period) if period else None
        if parsed_period:
            if not start_date:
                start_date = parsed_period['startDate']
            if not end_date:
                end_date = parsed_period['endDate']
            if days is None:
                days = parsed_period['days']

        if start_date:
            try:
                datetime.strptime(start_date, '%d.%m.%Y')
            except Exception:
                abort(400, description=f"`{field_name}[{idx}].startDate` must be DD.MM.YYYY")
        if end_date:
            try:
                datetime.strptime(end_date, '%d.%m.%Y')
            except Exception:
                abort(400, description=f"`{field_name}[{idx}].endDate` must be DD.MM.YYYY")

        if (start_date and not end_date) or (end_date and not start_date):
            abort(400, description=f"`{field_name}[{idx}]` requires both `startDate` and `endDate`")

        if start_date and end_date and days is None:
            start_dt = datetime.strptime(start_date, '%d.%m.%Y')
            end_dt = datetime.strptime(end_date, '%d.%m.%Y')
            days = max(0, (end_dt.date() - start_dt.date()).days)

        if days is None:
            days = 0
        days = max(0, int(days))

        if start_date and end_date:
            period = f'{start_date} - {end_date} ({days} днів)'

        payments.append({
            'period': period,
            'startDate': start_date,
            'endDate': end_date,
            'days': days,
            'pdfLabel': pdf_label,
            'pdfUrl': pdf_url,
        })

    return payments


def _get_active_ritual_period(payments):
    best = None
    best_end = None
    for item in payments:
        parsed_period = _parse_ritual_period_str(item.get('period'))
        start_text = _clean_str(item.get('startDate')) or (parsed_period.get('startDate') if parsed_period else '')
        end_text = _clean_str(item.get('endDate')) or (parsed_period.get('endDate') if parsed_period else '')
        if not end_text:
            continue
        try:
            end_dt = datetime.strptime(end_text, '%d.%m.%Y')
        except Exception:
            continue

        days = item.get('days')
        if days is None and parsed_period:
            days = parsed_period.get('days')

        if best is None or end_dt > best_end:
            best = {
                'startDate': start_text,
                'endDate': end_text,
                'days': days,
            }
            best_end = end_dt

    if best is None or best_end is None:
        return {
            'periodStart': '',
            'periodEnd': '',
            'periodDays': 0,
            'daysLeft': 0,
        }

    start_text = _clean_str(best.get('startDate'))
    end_text = _clean_str(best.get('endDate'))
    days = best.get('days')
    try:
        days = int(days)
    except Exception:
        days = 0

    today = datetime.utcnow().date()
    days_left = max(0, (best_end.date() - today).days)

    return {
        'periodStart': start_text,
        'periodEnd': end_text,
        'periodDays': max(0, days),
        'daysLeft': days_left,
    }


def _normalize_admin_ritual_service(ritual_service):
    hq_location = normalize_ritual_hq_location(ritual_service)
    legacy_loc = ritual_location_to_legacy(hq_location, current_doc=ritual_service)
    contacts = _clean_ritual_contacts_loose(ritual_service.get('contacts'))
    if not contacts:
        contacts = _derive_ritual_contacts_from_legacy(ritual_service)

    settlements = _clean_str_list(ritual_service.get('settlements'), 'settlements') if isinstance(ritual_service.get('settlements'), list) else []
    service_area_ids = _clean_str_list(ritual_service.get('serviceAreaIds'), 'serviceAreaIds') if isinstance(ritual_service.get('serviceAreaIds'), list) else []
    payments = _clean_ritual_payments(ritual_service.get('payments'))
    active_period = _get_active_ritual_period(payments)

    addresses = [entry.get('address', '') for entry in contacts if _clean_str(entry.get('address'))]
    if not addresses:
        addresses = _coerce_ritual_addresses(ritual_service.get('address') or legacy_loc.get('address'))
    subtitle = addresses[0] if addresses else (settlements[0] if settlements else '')

    status = _normalize_ritual_service_status(ritual_service.get('status'))
    category = _clean_str(ritual_service.get('category'))

    created_at = ritual_service.get('createdAt')
    updated_at = ritual_service.get('updatedAt')

    return {
        'id': str(ritual_service.get('_id')),
        'name': _clean_str(ritual_service.get('name')),
        'subtitle': subtitle,
        'category': category,
        'status': status,
        'avatarUrl': _clean_str(ritual_service.get('logo')),
        'logo': _clean_str(ritual_service.get('logo')),
        'banner': _clean_str(ritual_service.get('banner')),
        'link': _clean_str(ritual_service.get('link')),
        'login': _clean_str(ritual_service.get('login')),
        'password': _clean_str(ritual_service.get('password')),
        'settlements': settlements,
        'contacts': contacts,
        'payments': payments,
        'addresses': _coerce_ritual_addresses(ritual_service.get('address') or legacy_loc.get('address')),
        'phones': _coerce_ritual_phones(ritual_service.get('phone')),
        'periodStart': active_period['periodStart'],
        'periodEnd': active_period['periodEnd'],
        'periodDays': active_period['periodDays'],
        'daysLeft': active_period['daysLeft'],
        'latitude': ritual_service.get('latitude') if ritual_service.get('latitude') is not None else legacy_loc.get('latitude'),
        'longitude': ritual_service.get('longitude') if ritual_service.get('longitude') is not None else legacy_loc.get('longitude'),
        'hqLocation': hq_location,
        'serviceAreaIds': service_area_ids,
        'description': ritual_service.get('description'),
        'items': ritual_service.get('items'),
        'createdAt': created_at.isoformat() if isinstance(created_at, datetime) else None,
        'updatedAt': updated_at.isoformat() if isinstance(updated_at, datetime) else None,
    }


def _build_admin_ritual_service_payload(data, partial=False):
    if not isinstance(data, dict):
        abort(400, description='Body must be a JSON object')

    unknown = set(data.keys()) - ADMIN_RITUAL_SERVICE_ALLOWED_FIELDS
    if unknown:
        unknown_list = ', '.join(sorted(unknown))
        abort(400, description=f'Unsupported fields: {unknown_list}')

    payload = {}

    if not partial:
        name = _clean_str(data.get('name'))
        if not name:
            abort(400, description='`name` is required')
        if _location_admin_strict_geonames_enabled() and 'hqLocation' not in data:
            abort(400, description="`hqLocation` is required and must come from provider suggestions")

    if 'name' in data:
        name = _clean_str(data.get('name'))
        if not name:
            abort(400, description='`name` is required')
        payload['name'] = name

    if 'category' in data:
        payload['category'] = _clean_str(data.get('category'))

    if 'status' in data:
        payload['status'] = _normalize_ritual_service_status(data.get('status'))

    if 'logo' in data:
        payload['logo'] = _clean_str(data.get('logo'))
    if 'banner' in data:
        payload['banner'] = _clean_str(data.get('banner'))
    if 'link' in data:
        payload['link'] = _clean_str(data.get('link'))
    if 'login' in data:
        payload['login'] = _clean_str(data.get('login'))
    if 'password' in data:
        payload['password'] = _clean_str(data.get('password'))

    if 'settlements' in data:
        payload['settlements'] = _clean_str_list(data.get('settlements'), 'settlements')

    if 'contacts' in data:
        payload['contacts'] = _validate_ritual_contacts(data.get('contacts'), 'contacts')

    if 'payments' in data:
        payload['payments'] = _clean_ritual_payments(data.get('payments'), 'payments')

    if 'latitude' in data:
        payload['latitude'] = data.get('latitude')
    if 'longitude' in data:
        payload['longitude'] = data.get('longitude')
    if 'hqLocation' in data:
        if data.get('hqLocation') is not None and not isinstance(data.get('hqLocation'), dict):
            abort(400, description='`hqLocation` must be an object')
        hq_location = normalize_location_core(data.get('hqLocation') or {})
        if _location_admin_strict_geonames_enabled():
            _validate_admin_strict_location('hqLocation', hq_location)
        if _location_write_is_canonical() or _location_write_is_dual() or _location_admin_strict_geonames_enabled():
            payload['hqLocation'] = hq_location
        if _location_write_is_legacy() or _location_write_is_dual():
            legacy = ritual_location_to_legacy(hq_location, current_doc={'address': payload.get('address', [])})
            payload['address'] = legacy.get('address', [])
            payload['latitude'] = legacy.get('latitude')
            payload['longitude'] = legacy.get('longitude')
    if 'serviceAreaIds' in data:
        if not isinstance(data.get('serviceAreaIds'), list):
            abort(400, description='`serviceAreaIds` must be an array')
        payload['serviceAreaIds'] = [loc_clean_str(v) for v in data.get('serviceAreaIds', []) if loc_clean_str(v)]
    if 'description' in data:
        payload['description'] = data.get('description')
    if 'items' in data:
        payload['items'] = data.get('items')

    if 'contacts' in payload:
        addresses = [entry.get('address', '') for entry in payload['contacts'] if _clean_str(entry.get('address'))]
        phones = [entry.get('phone', '') for entry in payload['contacts'] if _clean_str(entry.get('phone'))]
        payload['address'] = addresses
        payload['phone'] = phones

    if 'hqLocation' not in payload and (
        ('latitude' in payload) or ('longitude' in payload) or ('contacts' in payload) or ('settlements' in payload)
    ):
        address_seed = ''
        if isinstance(payload.get('contacts'), list) and payload.get('contacts'):
            address_seed = _clean_str((payload['contacts'][0] or {}).get('address'))
        if not address_seed and isinstance(payload.get('address'), list) and payload.get('address'):
            address_seed = _clean_str(payload['address'][0])
        hq_location = normalize_location_core({
            'addressLine': address_seed,
            'geo': {
                'lat': payload.get('latitude'),
                'lng': payload.get('longitude'),
            },
        })
        if _location_write_is_canonical() or _location_write_is_dual():
            payload['hqLocation'] = hq_location

    if not partial:
        payload.setdefault('category', DEFAULT_RITUAL_SERVICE_CATEGORIES[1])
        payload.setdefault('status', 'Активний')
        payload.setdefault('settlements', [])
        payload.setdefault('contacts', [])
        payload.setdefault('payments', [])
        payload.setdefault('logo', '')
        payload.setdefault('banner', '')
        payload.setdefault('link', '')
        payload.setdefault('login', '')
        payload.setdefault('password', '')
        payload.setdefault('description', '')
        payload.setdefault('items', [])
        payload.setdefault('latitude', None)
        payload.setdefault('longitude', None)
        payload.setdefault('serviceAreaIds', [])
        if 'address' not in payload:
            payload['address'] = []
        if 'phone' not in payload:
            payload['phone'] = []
        if 'hqLocation' not in payload and (_location_write_is_canonical() or _location_write_is_dual()):
            payload['hqLocation'] = normalize_location_core({
                'addressLine': payload['address'][0] if payload.get('address') else '',
                'geo': {
                    'lat': payload.get('latitude'),
                    'lng': payload.get('longitude'),
                },
            })

    return payload


def _ensure_default_ritual_service_categories():
    now = datetime.utcnow()
    for order, name in enumerate(DEFAULT_RITUAL_SERVICE_CATEGORIES):
        normalized = _clean_str(name).lower()
        if not normalized:
            continue
        ritual_service_categories_collection.update_one(
            {'nameNormalized': normalized},
            {
                '$setOnInsert': {
                    'name': _clean_str(name),
                    'nameNormalized': normalized,
                    'isDefault': True,
                    'order': order,
                    'createdAt': now,
                    'updatedAt': now,
                }
            },
            upsert=True
        )


def _normalize_ritual_service_category(doc):
    return {
        'id': str(doc.get('_id')),
        'name': _clean_str(doc.get('name')),
        'isDefault': bool(doc.get('isDefault')),
        'createdAt': doc.get('createdAt').isoformat() if isinstance(doc.get('createdAt'), datetime) else None,
        'updatedAt': doc.get('updatedAt').isoformat() if isinstance(doc.get('updatedAt'), datetime) else None,
    }


@application.route('/api/admin/ritual-services/categories', methods=['GET'])
def admin_list_ritual_service_categories():
    _ensure_default_ritual_service_categories()
    docs = list(
        ritual_service_categories_collection.find({}, {'name': 1, 'isDefault': 1, 'createdAt': 1, 'updatedAt': 1, 'order': 1})
    )

    def _sort_key(item):
        normalized_name = _clean_str(item.get('name')).lower()
        if normalized_name in [_clean_str(v).lower() for v in DEFAULT_RITUAL_SERVICE_CATEGORIES]:
            return (0, DEFAULT_RITUAL_SERVICE_CATEGORIES.index(next(v for v in DEFAULT_RITUAL_SERVICE_CATEGORIES if _clean_str(v).lower() == normalized_name)))
        created_at = item.get('createdAt')
        created_ts = created_at.timestamp() if isinstance(created_at, datetime) else 0
        return (1, created_ts, normalized_name)

    docs.sort(key=_sort_key)
    categories = [_normalize_ritual_service_category(doc) for doc in docs if _clean_str(doc.get('name'))]

    return jsonify({
        'total': len(categories),
        'categories': categories,
    })


@application.route('/api/admin/ritual-services/categories', methods=['POST'])
def admin_create_ritual_service_category():
    _ensure_default_ritual_service_categories()
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        abort(400, description='Body must be a JSON object')

    raw_name = _clean_str(data.get('name'))
    if not raw_name:
        abort(400, description='`name` is required')

    normalized = raw_name.lower()
    now = datetime.utcnow()
    result = ritual_service_categories_collection.update_one(
        {'nameNormalized': normalized},
        {
            '$setOnInsert': {
                'name': raw_name,
                'nameNormalized': normalized,
                'isDefault': False,
                'order': 9999,
                'createdAt': now,
                'updatedAt': now,
            }
        },
        upsert=True
    )

    created = ritual_service_categories_collection.find_one({'nameNormalized': normalized})
    status_code = 201 if result.upserted_id else 200
    return jsonify(_normalize_ritual_service_category(created)), status_code


@application.route('/api/admin/ritual-services', methods=['GET'])
def admin_list_ritual_services():
    search = _clean_str(request.args.get('search', ''))
    query_filter = {}

    if search:
        regex = {'$regex': re.escape(search), '$options': 'i'}
        query_filter = {
            '$or': [
                {'name': regex},
                {'category': regex},
                {'address': regex},
                {'hqLocation.display': regex},
                {'settlements': regex},
                {'contacts.address': regex},
                {'contacts.phone': regex},
                {'login': regex},
            ]
        }

    cursor = ritual_services_collection.find(query_filter).sort([('updatedAt', -1), ('createdAt', -1), ('_id', -1)])
    services = [_normalize_admin_ritual_service(item) for item in cursor]
    return jsonify({
        'total': len(services),
        'services': services,
    })


@application.route('/api/admin/ritual-services', methods=['POST'])
def admin_create_ritual_service():
    data = request.get_json(silent=True) or {}
    payload = _build_admin_ritual_service_payload(data, partial=False)
    now = datetime.utcnow()
    payload['createdAt'] = now
    payload['updatedAt'] = now

    inserted = ritual_services_collection.insert_one(payload)
    created = ritual_services_collection.find_one({'_id': inserted.inserted_id})
    return jsonify(_normalize_admin_ritual_service(created)), 201


@application.route('/api/admin/ritual-services/<string:service_id>', methods=['PATCH'])
def admin_update_ritual_service(service_id):
    try:
        oid = ObjectId(service_id)
    except Exception:
        abort(400, description='Invalid ritual service id')

    data = request.get_json(silent=True) or {}
    update_fields = _build_admin_ritual_service_payload(data, partial=True)
    if not update_fields:
        abort(400, description='Nothing to update')

    update_fields['updatedAt'] = datetime.utcnow()
    result = ritual_services_collection.update_one({'_id': oid}, {'$set': update_fields})
    if result.matched_count == 0:
        abort(404, description='Ritual service not found')

    ritual_service = ritual_services_collection.find_one({'_id': oid})
    return jsonify(_normalize_admin_ritual_service(ritual_service))


@application.route('/api/admin/ritual-services/<string:service_id>', methods=['DELETE'])
def admin_delete_ritual_service(service_id):
    try:
        oid = ObjectId(service_id)
    except Exception:
        abort(400, description='Invalid ritual service id')

    result = ritual_services_collection.delete_one({'_id': oid})
    if result.deleted_count == 0:
        abort(404, description='Ritual service not found')

    return jsonify({'ok': True})


@application.route('/api/churches_page', methods=['GET'])
def churches_page():
    search_query = request.args.get('search', '').strip()

    query_filter = {}

    if search_query:
        query_filter['name'] = {'$regex': re.escape(search_query), '$options': 'i'}

    churches_cursor = churches_collection.find(query_filter)

    churches_list = []
    for church in churches_cursor:
        location = normalize_church_location(church)
        legacy_loc = church_location_to_legacy(location)
        churches_list.append({
            "id": str(church.get('_id')),
            "name": church.get('name'),
            "image": church.get('image'),
            "address": church.get('address') or legacy_loc.get("address"),
            "phone": church.get('phone'),
            "description": church.get('description'),
            "location": location if _location_read_uses_canonical() else None,
        })

    return jsonify({
        "total": len(churches_list),
        "churches": churches_list
    })


@application.route('/api/churches_page/<string:church_id>', methods=['GET'])
def get_church_page(church_id):
    try:
        oid = ObjectId(church_id)
    except Exception:
        abort(400, description="Invalid church id")

    church = churches_collection.find_one({'_id': oid})
    if not church:
        abort(404, description="Church not found")

    location = normalize_church_location(church)
    legacy_loc = church_location_to_legacy(location)
    return jsonify({
        "id": str(church.get('_id')),
        "name": church.get('name'),
        "image": church.get('image'),
        "address": church.get('address') or legacy_loc.get("address"),
        "phone": church.get('phone'),
        "description": church.get('description'),
        "location": location if _location_read_uses_canonical() else None,
    })


@application.route('/api/locations', methods=['GET'])
def locations():
    search = request.args.get('search', '').strip()
    if not search:
        return jsonify([])
    return jsonify(
        search_location_areas(
            search,
            provider=LOCATION_PROVIDER,
            locationiq_api_key=LOCATIONIQ_API_KEY,
            locationiq_region=LOCATIONIQ_REGION,
            locationiq_language=LOCATION_ACCEPT_LANGUAGE,
            country_codes=LOCATION_COUNTRY_CODES,
            geonames_user=GEONAMES_USER,
            geonames_lang=GEONAMES_LANG,
            max_rows=10,
        )
    )


def _query_cemetery_options(area_id='', area='', search='', limit=10):
    area_id = loc_clean_str(area_id)
    area = loc_clean_str(area)
    search = loc_clean_str(search)
    query_filter = {}
    if search:
        query_filter["name"] = {'$regex': f'^{re.escape(search)}', '$options': 'i'}

    docs = list(cemeteries_collection.find(query_filter).limit(max(int(limit), 1)))
    options = []
    for doc in docs:
        item = cemetery_option_from_doc(doc)
        if not item.get("name"):
            continue
        if area_id:
            if item.get("areaId") != area_id:
                continue
        elif area:
            city_part = area.split(',')[0].strip()
            if city_part:
                pattern = re.compile(r'\b' + re.escape(city_part) + r'\b', re.IGNORECASE)
                area_text = item.get("area") or ""
                if not pattern.search(area_text):
                    continue
        options.append(item)

    # Fallback for legacy docs without normalized location.
    if not options and (area_id or area):
        legacy_query = {}
        if area:
            city_part = area.split(',')[0].strip()
            if city_part:
                legacy_query["locality"] = {'$regex': r'\b' + re.escape(city_part) + r'\b', '$options': 'i'}
        if search:
            legacy_query["name"] = {'$regex': f'^{re.escape(search)}', '$options': 'i'}
        for doc in cemeteries_collection.find(legacy_query).limit(max(int(limit), 1)):
            item = cemetery_option_from_doc(doc)
            if area_id and item.get("areaId") and item.get("areaId") != area_id:
                continue
            options.append(item)

    # Keep stable, unique response by name+areaId.
    uniq = []
    seen = set()
    for item in options:
        key = (item.get("name"), item.get("areaId") or item.get("area"))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(item)
    uniq.sort(key=lambda x: (loc_clean_str(x.get("name")).lower(), loc_clean_str(x.get("area")).lower()))
    return uniq[: max(int(limit), 1)]


@application.route('/api/cemeteries', methods=['GET'])
def cemeteries():
    area_id = request.args.get('areaId', '').strip()
    area = request.args.get('area', '').strip()
    search = request.args.get('search', '').strip()
    items = _query_cemetery_options(area_id=area_id, area=area, search=search, limit=10)
    return jsonify(items)


@application.route('/api/location/areas', methods=['GET'])
def location_areas():
    search = request.args.get('search', '').strip()
    if not search:
        return jsonify({"total": 0, "items": []})
    items = search_location_areas(
        search,
        provider=LOCATION_PROVIDER,
        locationiq_api_key=LOCATIONIQ_API_KEY,
        locationiq_region=LOCATIONIQ_REGION,
        locationiq_language=LOCATION_ACCEPT_LANGUAGE,
        country_codes=LOCATION_COUNTRY_CODES,
        geonames_user=GEONAMES_USER,
        geonames_lang=GEONAMES_LANG,
        max_rows=10,
    )
    return jsonify({"total": len(items), "items": items})


@application.route('/api/location/addresses', methods=['GET'])
def location_addresses():
    search = request.args.get('search', '').strip()
    if not search:
        return jsonify({"total": 0, "items": []})
    items = search_location_addresses(
        search,
        provider=LOCATION_PROVIDER,
        locationiq_api_key=LOCATIONIQ_API_KEY,
        locationiq_region=LOCATIONIQ_REGION,
        locationiq_language=LOCATION_ACCEPT_LANGUAGE,
        country_codes=LOCATION_COUNTRY_CODES,
        geonames_user=GEONAMES_USER,
        geonames_lang=GEONAMES_LANG,
        max_rows=20,
    )
    return jsonify({"total": len(items), "items": items})


@application.route('/api/location/cemeteries', methods=['GET'])
def location_cemeteries():
    area_id = request.args.get('areaId', '').strip()
    area = request.args.get('area', '').strip()
    search = request.args.get('search', '').strip()
    items = _query_cemetery_options(area_id=area_id, area=area, search=search, limit=20)
    return jsonify({"total": len(items), "items": items})


@application.route('/api/location/normalize', methods=['POST'])
def location_normalize():
    payload = request.get_json(silent=True) or {}
    location = normalize_location_input(payload)
    return jsonify({"location": location})


@application.route('/api/ritual_services', methods=['GET'])
def ritual_services():
    search_query = request.args.get('search', '').strip()
    address = request.args.get('address', '').strip()

    query_filter = {}

    if search_query:
        query_filter['name'] = {'$regex': re.escape(search_query), '$options': 'i'}

    if address:
        query_filter['$or'] = [
            {'address': {'$regex': re.escape(address), '$options': 'i'}},
            {'hqLocation.display': {'$regex': re.escape(address), '$options': 'i'}},
            {'contacts.address': {'$regex': re.escape(address), '$options': 'i'}},
        ]

    ritual_services_cursor = ritual_services_collection.find(query_filter)

    ritual_services_list = []
    for ritual_service in ritual_services_cursor:
        hq_location = normalize_ritual_hq_location(ritual_service)
        legacy_loc = ritual_location_to_legacy(hq_location, current_doc=ritual_service)
        ritual_services_list.append({
            "id": str(ritual_service.get('_id')),
            "name": ritual_service.get('name'),
            "address": ritual_service.get('address') or legacy_loc.get("address"),
            "category": ritual_service.get('category'),
            "logo": ritual_service.get('logo'),
            "latitude": ritual_service.get("latitude") if ritual_service.get("latitude") is not None else legacy_loc.get("latitude"),
            "longitude": ritual_service.get("longitude") if ritual_service.get("longitude") is not None else legacy_loc.get("longitude"),
            "hqLocation": hq_location if _location_read_uses_canonical() else None,
            "serviceAreaIds": ritual_service.get("serviceAreaIds", []),
        })

    return jsonify({
        "total": len(ritual_services_list),
        "ritual_services": ritual_services_list
    })


@application.route('/api/ritual_services/<string:ritual_service_id>', methods=['GET', 'PUT'])
def get_ritual_service(ritual_service_id):
    try:
        oid = ObjectId(ritual_service_id)
    except Exception:
        abort(400, description="Invalid ritual service id")

    if request.method == 'GET':
        ritual_service = ritual_services_collection.find_one({'_id': oid})
        if not ritual_service:
            abort(404, description="Ritual service not found")

        # ---- Normalization (GET) ----
        # Always return items as albums: [title: str, albums: [{photos|video, description}], ...]
        def _normalize_album_entry(x):
            """
            Back-compat:
              - "string"         -> photo album with one photo
              - ["u1","u2"]      -> photo album with those photos
              - {"photos":[...]} -> photo album
              - {"video":{...}}  -> video album (NEW)
            """
            if isinstance(x, str):
                return {"photos": [x], "description": ""}
            if isinstance(x, list):
                photos = [u for u in x if isinstance(u, str) and u.strip()]
                return {"photos": photos, "description": ""}
            if isinstance(x, dict):
                # Video first
                if isinstance(x.get("video"), dict):
                    v = x["video"]
                    player = v.get("player") if isinstance(v.get("player"), str) else ""
                    poster = v.get("poster") if isinstance(v.get("poster"), str) else ""
                    desc = x.get("description", "")
                    if not isinstance(desc, str):
                        desc = str(desc)
                    if player.strip():
                        return {"video": {"player": player.strip(), "poster": poster.strip() if poster else ""},
                                "description": desc}
                    # If malformed video (no player), fall through to photos parsing.

                photos = [u for u in x.get("photos", []) if isinstance(u, str) and u.strip()]
                desc = x.get("description", "")
                if not isinstance(desc, str):
                    desc = str(desc)
                return {"photos": photos, "description": desc}
            # unknown -> empty album
            return {"photos": [], "description": ""}

        raw_items = ritual_service.get('items') or []
        norm_items = []
        if isinstance(raw_items, list):
            for it in raw_items:
                # expect [title, albumsLike]
                if isinstance(it, list) and len(it) == 2 and isinstance(it[0], str):
                    title = it[0]
                    raw_albums = it[1] if isinstance(it[1], list) else []
                    albums = [_normalize_album_entry(a) for a in raw_albums]

                    # Drop empty albums (no photos AND no video.player)
                    def _not_empty(a):
                        if "video" in a and isinstance(a["video"], dict):
                            return bool(a["video"].get("player"))
                        return bool(a.get("photos"))

                    albums = [a for a in albums if _not_empty(a)]
                    norm_items.append([title, albums])

        hq_location = normalize_ritual_hq_location(ritual_service)
        legacy_loc = ritual_location_to_legacy(hq_location, current_doc=ritual_service)
        return jsonify({
            "id": str(ritual_service['_id']),
            "name": ritual_service.get('name'),
            "address": ritual_service.get('address') or legacy_loc.get("address"),
            "category": ritual_service.get('category'),
            "logo": ritual_service.get('logo'),
            "latitude": ritual_service.get('latitude') if ritual_service.get('latitude') is not None else legacy_loc.get("latitude"),
            "longitude": ritual_service.get('longitude') if ritual_service.get('longitude') is not None else legacy_loc.get("longitude"),
            "banner": ritual_service.get('banner'),
            "description": ritual_service.get('description'),
            "link": ritual_service.get('link'),
            "phone": ritual_service.get('phone'),
            "items": norm_items,  # normalized for the new UI (photos or video)
            "hqLocation": hq_location if _location_read_uses_canonical() else None,
            "serviceAreaIds": ritual_service.get("serviceAreaIds", []),
        })

    # ---------------------------- PUT ----------------------------
    data = request.get_json(silent=True) or {}
    ritual_service = ritual_services_collection.find_one({'_id': oid})
    if not ritual_service:
        abort(404, description="Ritual service not found")

    update_fields = {}

    if 'description' in data:
        if not isinstance(data['description'], (str, type(None))):
            abort(400, description="`description` must be a string")
        update_fields['description'] = data['description'] or ""

    # Accept and normalize items to the albums shape
    if 'items' in data:
        if not isinstance(data['items'], list):
            abort(400, description="`items` must be an array")
        normalized_items = []

        def _validate_url(u, i_title, i_album, i_photo):
            if not isinstance(u, str) or not u.strip():
                abort(400, description=f"`items[{i_title}][1][{i_album}].photos[{i_photo}]` must be a non-empty string")

        def _validate_video(player_val, i_title, i_album):
            if not isinstance(player_val, str) or not player_val.strip():
                abort(400, description=f"`items[{i_title}][1][{i_album}].video.player` must be a non-empty string")

        for i_title, item in enumerate(data['items']):
            # Each item must be [title, albumsLike]
            if not (isinstance(item, list) and len(item) == 2 and isinstance(item[0], str)):
                abort(400, description=f"`items[{i_title}]` must be [title: string, albums: array]")

            title, raw_albums = item[0], item[1]
            if not isinstance(raw_albums, list):
                abort(400, description=f"`items[{i_title}][1]` must be an array")

            albums_out = []
            for i_album, a in enumerate(raw_albums):
                # Back-compat & normalization
                if isinstance(a, str):
                    photos = [a]
                    desc = ""
                    albums_out.append({"photos": [u.strip() for u in photos], "description": desc})
                    continue

                if isinstance(a, list):
                    photos = [u for u in a if isinstance(u, str) and u.strip()]
                    if not photos:
                        abort(400, description=f"`items[{i_title}][1][{i_album}].photos` must contain at least one URL")
                    for i_photo, u in enumerate(photos):
                        _validate_url(u, i_title, i_album, i_photo)
                    albums_out.append({"photos": [u.strip() for u in photos], "description": ""})
                    continue

                if isinstance(a, dict):
                    desc = a.get("description", "")
                    if not isinstance(desc, str):
                        desc = str(desc)

                    # --- NEW: video album ---
                    if isinstance(a.get("video"), dict):
                        v = a["video"]
                        player = v.get("player")
                        poster = v.get("poster") if isinstance(v.get("poster"), str) else ""
                        _validate_video(player, i_title, i_album)
                        albums_out.append(
                            {"video": {"player": player.strip(), "poster": poster.strip() if poster else ""},
                             "description": desc})
                        continue

                    # photo album object
                    photos = a.get("photos", [])
                    if not isinstance(photos, list):
                        abort(400, description=f"`items[{i_title}][1][{i_album}].photos` must be an array")
                    if not photos:
                        abort(400, description=f"`items[{i_title}][1][{i_album}].photos` must contain at least one URL")
                    for i_photo, u in enumerate(photos):
                        _validate_url(u, i_title, i_album, i_photo)
                    albums_out.append({"photos": [u.strip() for u in photos], "description": desc})
                    continue

                abort(400, description=f"`items[{i_title}][1][{i_album}]` must be string | array | object")

            normalized_items.append([title, albums_out])

        update_fields['items'] = normalized_items

    if 'hqLocation' in data:
        if data['hqLocation'] is not None and not isinstance(data['hqLocation'], dict):
            abort(400, description="`hqLocation` must be an object")
        hq_location = normalize_location_core(data.get('hqLocation') or {})
        update_fields['hqLocation'] = hq_location
        legacy = ritual_location_to_legacy(hq_location, current_doc=ritual_service)
        update_fields['address'] = legacy['address']
        update_fields['latitude'] = legacy['latitude']
        update_fields['longitude'] = legacy['longitude']

    if 'serviceAreaIds' in data:
        if not isinstance(data['serviceAreaIds'], list):
            abort(400, description="`serviceAreaIds` must be an array")
        update_fields['serviceAreaIds'] = [loc_clean_str(v) for v in data['serviceAreaIds'] if loc_clean_str(v)]

    if not update_fields:
        abort(400, description="Nothing to update")

    ritual_services_collection.update_one({'_id': oid}, {'$set': update_fields})
    return jsonify({"message": "Ritual service updated successfully"}), 200


@application.route('/api/ritual_services/login', methods=['POST'])
def ritual_services_login():
    data = request.get_json() or {}
    ritual_service_id = data.get('ritual_service_id')
    login = data.get('login')
    password = data.get('password')

    try:
        oid = ObjectId(ritual_service_id)
    except Exception:
        abort(400, description="Invalid ritual service id")

    ritual_service = ritual_services_collection.find_one({'_id': oid})
    if not ritual_service:
        abort(404, description="Ritual service not found")

    if ritual_service.get("login") != login or ritual_service.get("password") != password:
        abort(401, description="Invalid login or password")

    payload = {
        "ritual_service_id": ritual_service_id,
        "exp": datetime.utcnow() + timedelta(seconds=JWT_EXP_DELTA_SECONDS)
    }

    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    return jsonify({"token": token})


@application.route('/api/ritual_services/verify_token', methods=['POST'])
def verify_token():
    data = request.get_json()
    token = data.get("token")

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return jsonify({"valid": True, "ritual_service_id": payload["ritual_service_id"]})
    except jwt.ExpiredSignatureError:
        abort(401, description="Token expired")
    except jwt.InvalidTokenError:
        abort(401, description="Invalid token")


def kyivstar_headers():
    """Повертає стандартні заголовки для Kyivstar API."""
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {KYIVSTAR_ACCESS_TOKEN}"
    }


@application.route('/api/send-code', methods=['POST'])
def send_code():
    """Надсилає користувачу SMS з OTP кодом через Kyivstar API."""
    data = request.get_json() or {}
    phone = (data.get('phone') or '').strip()
    person_id = (data.get('personId') or '').strip()

    person = None
    if person_id:
        try:
            oid = ObjectId(person_id)
        except Exception:
            return jsonify({'error': 'invalid_person_id'}), 400
        person = people_collection.find_one({'_id': oid}, {'premium': 1})
        if not person:
            return jsonify({'error': 'person_not_found'}), 404
        premium = person.get('premium') or {}
        phone = (premium.get('phone') or '').strip()
        if not phone:
            return jsonify({'error': 'phone_not_set'}), 400
    elif not phone:
        return jsonify({'error': 'phone is required'}), 400

    try:
        payload = {"to": str(phone)}
        resp = requests.post(KYIVSTAR_OTP_SEND_URL, json=payload, headers=kyivstar_headers(), timeout=10)

        if resp.status_code != 200:
            return jsonify({
                "error": "kyivstar_otp_send_failed",
                "status_code": resp.status_code,
                "details": resp.text
            }), 502

        res = resp.json()
        status = res.get("resource", {}).get("status")
        message_id = res.get("resource", {}).get("messageId")

        if person:
            people_collection.update_one(
                {'_id': person['_id']},
                {'$set': {
                    'premium.resetSms': {
                        'phone': phone,
                        'messageId': message_id,
                        'status': status,
                        'sentAt': datetime.utcnow()
                    }
                }}
            )

        return jsonify({
            "status": status,  # очікувано "SUCCESS"
            "message_id": message_id,
            "req_id": res.get("reqId"),
            "cid": res.get("cid")
        })

    except requests.RequestException as e:
        return jsonify({
            "error": "kyivstar_otp_send_exception",
            "details": str(e)
        }), 502


@application.route('/api/verify-code', methods=['POST'])
def verify_code():
    """Перевіряє OTP код користувача через Kyivstar API."""
    data = request.get_json() or {}
    phone = (data.get('phone') or '').strip()
    code = (data.get('code') or '').strip()
    person_id = (data.get('personId') or '').strip()

    if not code:
        return jsonify({'error': 'code is required'}), 400

    person = None
    if person_id:
        try:
            oid = ObjectId(person_id)
        except Exception:
            return jsonify({'error': 'invalid_person_id'}), 400
        person = people_collection.find_one({'_id': oid}, {'premium': 1})
        if not person:
            return jsonify({'error': 'person_not_found'}), 404
        premium = person.get('premium') or {}
        phone = (premium.get('phone') or '').strip()
        if not phone:
            return jsonify({'error': 'phone_not_set'}), 400
    elif not phone:
        return jsonify({'error': 'phone is required'}), 400

    ok, payload, status_code = verify_sms_code_with_kyivstar(phone, code)
    if not ok:
        return jsonify(payload or {}), status_code or 400

    if person:
        people_collection.update_one(
            {'_id': person['_id']},
            {'$set': {
                'premium.resetSms.lastStatus': payload.get('status'),
                'premium.resetSms.verifiedAt': datetime.utcnow()
            }}
        )

    return jsonify({
        "success": True,
        "status": payload.get('status', 'VALID')
    })


@application.route('/api/people/add_moderation', methods=['POST'])
def people_add_moderation():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({'error': 'invalid_payload', 'message': 'JSON object expected'}), 400

    def _parse_iso_date(value, field_name):
        if not isinstance(value, str):
            return None
        raw = value.strip()
        if not raw:
            return None
        try:
            dt = datetime.strptime(raw, '%Y-%m-%d')
        except ValueError:
            raise ValueError(f'{field_name} must be a valid date in YYYY-MM-DD format')
        return dt.strftime('%Y-%m-%d')

    def _normalize_year(value):
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value) if value.is_integer() else None
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            return int(raw) if raw.isdigit() else None
        return None

    try:
        birth_date = _parse_iso_date(data.get('birthDate'), 'birthDate')
        death_date = _parse_iso_date(data.get('deathDate'), 'deathDate')
    except ValueError as exc:
        return jsonify({'error': 'validation_error', 'message': str(exc)}), 400

    birth_year = _normalize_year(data.get('birthYear'))
    death_year = _normalize_year(data.get('deathYear'))

    if birth_year is None and birth_date:
        birth_year = int(birth_date[:4])
    if death_year is None and death_date:
        death_year = int(death_date[:4])

    area = loc_clean_str(data.get('area'))
    area_id = loc_clean_str(data.get('areaId'))
    cemetery = loc_clean_str(data.get('cemetery'))

    burial_site_coords = loc_clean_str(data.get('burialSiteCoords'))
    burial_site_photo_url = loc_clean_str(data.get('burialSitePhotoUrl'))
    burial_site_photo_urls = loc_clean_str_list(data.get('burialSitePhotoUrls'))
    if not burial_site_photo_urls and burial_site_photo_url:
        burial_site_photo_urls = [burial_site_photo_url]

    incoming_burial = data.get('burial') if isinstance(data.get('burial'), dict) else None
    if incoming_burial:
        burial = normalize_person_burial({
            'burial': incoming_burial,
            'area': area,
            'areaId': area_id,
            'cemetery': cemetery,
            'location': [burial_site_coords, '', burial_site_photo_urls],
        })
    else:
        burial = normalize_person_burial({
            'area': area,
            'areaId': area_id,
            'cemetery': cemetery,
            'location': [burial_site_coords, '', burial_site_photo_urls],
        })

    legacy = person_burial_to_legacy_fields(burial)

    document = {
        'name': data.get('name'),
        'areaId': area_id or legacy.get('areaId', ''),
        'area': area or legacy.get('area', ''),
        'cemetery': cemetery or legacy.get('cemetery', ''),
        'location': legacy.get('location', []),
        'burialSiteCoords': burial_site_coords,
        'burialSitePhotoUrl': burial_site_photo_urls[0] if burial_site_photo_urls else '',
        'burialSitePhotoUrls': burial_site_photo_urls,
        'burialSitePhotoCount': len(burial_site_photo_urls),
        'burial': burial,
        'link': data.get('link', ''),
        'bio': data.get('bio', ''),
        'createdAt': datetime.utcnow(),
    }

    if birth_year is not None:
        document['birthYear'] = birth_year
    if death_year is not None:
        document['deathYear'] = death_year
    if birth_date:
        document['birthDate'] = birth_date
    if death_date:
        document['deathDate'] = death_date

    people_moderation_collection.insert_one(document)

    return jsonify({'success': True})


@application.route('/api/settlements', methods=['GET'])
def search_settlements():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify({'success': False, 'data': [], 'errors': ['Missing q parameter']}), 400

    payload = {
        "apiKey": NP_API_KEY,
        "modelName": "Address",
        "calledMethod": "searchSettlements",
        "methodProperties": {
            "CityName": q,
            "Limit": 5
        }
    }
    resp = requests.post(NP_BASE_URL, json=payload)
    return jsonify(resp.json())


@application.route('/api/warehouses', methods=['GET'])
def get_warehouses():
    city_ref = request.args.get('cityRef', '').strip()
    q = request.args.get('q', '').strip()
    if not city_ref:
        return jsonify({'success': False, 'data': [], 'errors': ['Missing cityRef parameter']}), 400

    payload = {
        "apiKey": NP_API_KEY,
        "modelName": "Address",
        "calledMethod": "getWarehouses",
        "methodProperties": {
            "SettlementRef": city_ref,
            "FindByString": q,
            "Limit": 5
        }
    }
    resp = requests.post(NP_BASE_URL, json=payload)
    return jsonify(resp.json())


@application.route('/api/orders', methods=['POST'])
def create_order():
    data = request.get_json() or {}

    # Required fields (add email)
    required = ['name', 'cityRef', 'branchRef', 'phone', 'paymentMethod']
    missing = [k for k in required if k not in data or (isinstance(data[k], str) and not data[k].strip())]
    if missing:
        return jsonify({'error': 'Missing required fields', 'fields': missing}), 400

    raw_person_ids = data.get('personIds')
    if raw_person_ids is None:
        raw_person_ids = [data.get('personId')] if data.get('personId') else []
    if not isinstance(raw_person_ids, list):
        return jsonify({'error': 'Invalid personIds'}), 400

    person_ids = [str(pid).strip() for pid in raw_person_ids if str(pid).strip()]
    if not person_ids:
        return jsonify({'error': 'Missing personId'}), 400

    # Validate email (optional)
    email = (data.get('email') or '').strip().lower()

    # Validate personIds & fetch persons
    person_oids = []
    persons = []
    for pid in person_ids:
        try:
            person_oid = ObjectId(pid)
        except Exception:
            return jsonify({'error': 'Invalid personId'}), 400
        person = people_collection.find_one({'_id': person_oid})
        if not person:
            return jsonify({'error': 'Person not found'}), 404
        person_oids.append(person_oid)
        persons.append(person)

    # Update person's email if provided
    if email:
        try:
            for person_oid in person_oids:
                people_collection.update_one(
                    {'_id': person_oid},
                    {'$set': {
                        'email': email,
                        'updatedAt': datetime.utcnow()
                    }}
                )
        except Exception as e:
            return jsonify({'error': 'Failed to update person email', 'details': str(e)}), 500

    person_names = data.get('personNames')
    if not isinstance(person_names, list) or len(person_names) != len(persons):
        person_names = [p.get('name') for p in persons]

    # Build order document (store email as well for audit)
    order_doc = {
        'personId': person_ids[0],
        'personIds': person_ids,
        'personName': data.get('personName') or (person_names[0] if person_names else None),
        'personNames': person_names,
        'name': data['name'],
        'cityRef': data['cityRef'],
        'cityName': data.get('cityName'),  # optional
        'branchRef': data['branchRef'],
        'branchDesc': data.get('branchDesc'),  # optional
        'phone': data['phone'],
        'email': email or None,  # optional
        'paymentMethod': data['paymentMethod'],  # 'online' or 'cod'
        'paymentStatus': 'pending' if data['paymentMethod'] == 'online' else 'cod',
        'createdAt': datetime.utcnow(),
        'invoiceId': data.get('invoiceId')  # optional
        # webhook(s) can append status / payload later
    }

    try:
        result = orders_collection.insert_one(order_doc)
    except Exception as e:
        return jsonify({'error': 'DB insert failed', 'details': str(e)}), 500

    return jsonify({'orderId': str(result.inserted_id)}), 201


@application.route('/api/merchant/invoice/create', methods=['POST'])
def create_invoice():
    data = request.get_json() or {}
    # Перевіряємо, що всі потрібні поля є
    required = ['amount', 'redirectUrl', 'webHookUrl']
    if not all(k in data for k in required):
        return jsonify({'error': 'Missing one of required fields: ' + ', '.join(required)}), 400

    # Заголовок з токеном Monopay – додайте MONOPAY_TOKEN у ваш .env
    headers = {
        'Content-Type': 'application/json',
        'X-Token': os.getenv('MONOPAY_TOKEN')
    }

    del data["merchantPaymInfo"]
    try:
        resp = requests.post(
            'https://api.monobank.ua/api/merchant/invoice/create',
            headers=headers,
            json=data
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        print(str(e))
        return jsonify({
            'error': 'Monopay request failed',
            'details': str(e)
        }), 502

    # Повертаємо клієнту JSON із полями invoiceId і pageUrl
    return jsonify(resp.json()), resp.status_code


@application.route('/api/monopay/webhook', methods=['POST'])
def monopay_webhook():
    data = request.get_json() or {}
    invoice_id = data.get('invoiceId')
    status = data.get('status')

    if not invoice_id or not status:
        return jsonify({'error': 'Invalid webhook payload'}), 400

    # Оновлюємо запис у MongoDB під ключем invoiceId
    # Добавляємо поле paymentStatus і зберігаємо весь отриманий body
    orders_collection.update_one(
        {'invoiceId': invoice_id},
        {
            '$set': {
                'paymentStatus': status,
                'webhookData': data
            }
        },
        upsert=True
    )

    liturgies_collection.update_one(
        {'invoiceId': invoice_id},
        {
            '$set': {
                'paymentStatus': status,
                'webhookData': data
            }
        }
    )

    # Monopay очікує 200 OK
    return jsonify({'result': 'ok'}), 200


@application.route('/api/chats', methods=['POST'])
def create_chat():
    """Create a new chat session and send initial admin welcome message."""
    result = chat_collection.insert_one({
        'createdAt': datetime.utcnow(),
        'chatStatus': 'open',
        'openedByAdmin': False,
        'openedAt': None,
        'category': None,
        'lastAdminReadAt': None,
        'attachedPersonId': None
    })
    chat_id = str(result.inserted_id)

    admin_msg = {
        'chatId': result.inserted_id,
        'sender': 'admin',
        'text': "Вітаємо! Чим можемо вам допомогти?",
        'createdAt': datetime.utcnow(),
        'imageData': None,
        'attachments': []
    }
    message_collection.insert_one(admin_msg)
    socketio.emit('newMessage', {
        'sender': admin_msg['sender'],
        'text': admin_msg['text'],
        'imageData': admin_msg['imageData'],
        'attachments': admin_msg['attachments'],
        'createdAt': admin_msg['createdAt'].isoformat()
    }, room=chat_id)
    socketio.emit('adminChatUpdated', {'chatId': chat_id}, room=ADMINS_ROOM)

    return jsonify({'chatId': chat_id}), 201


def _normalize_attached_person(person):
    if not isinstance(person, dict):
        return None
    return {
        'id': str(person.get('_id')),
        'name': str(person.get('name') or ''),
        'birthYear': person.get('birthYear'),
        'deathYear': person.get('deathYear'),
        'avatarUrl': person.get('avatarUrl')
    }


@application.route('/api/chats', methods=['GET'])
def list_chats():
    """List all chat sessions for admin."""
    chats = list(chat_collection.find().sort('createdAt', -1))
    attached_person_ids = []
    for chat_doc in chats:
        raw_person_id = chat_doc.get('attachedPersonId')
        if not isinstance(raw_person_id, str):
            continue
        normalized_person_id = raw_person_id.strip()
        if not normalized_person_id:
            continue
        attached_person_ids.append(normalized_person_id)

    person_ids_to_load = []
    for person_id in sorted(set(attached_person_ids)):
        try:
            person_ids_to_load.append(ObjectId(person_id))
        except Exception:
            continue

    people_by_id = {}
    if person_ids_to_load:
        projection = {'name': 1, 'birthYear': 1, 'deathYear': 1, 'avatarUrl': 1}
        for person in people_collection.find({'_id': {'$in': person_ids_to_load}}, projection):
            people_by_id[str(person.get('_id'))] = _normalize_attached_person(person)

    out = []
    total_unread = 0
    for c in chats:
        chat_status = str(c.get('chatStatus') or 'open').strip().lower()
        if chat_status not in ('open', 'history'):
            chat_status = 'open'
        opened_by_admin = bool(c.get('openedByAdmin', True))
        status = 'history' if chat_status == 'history' else ('open' if opened_by_admin else 'new')
        last_admin_read_at = c.get('lastAdminReadAt') if isinstance(c.get('lastAdminReadAt'), datetime) else None
        attached_person_id = c.get('attachedPersonId') if isinstance(c.get('attachedPersonId'), str) else None
        if attached_person_id:
            attached_person_id = attached_person_id.strip() or None

        unread_filter = {'chatId': c['_id'], 'sender': 'user'}
        if last_admin_read_at is not None:
            unread_filter['createdAt'] = {'$gt': last_admin_read_at}
        unread_count = message_collection.count_documents(unread_filter)
        if status != 'history':
            total_unread += unread_count

        out.append({
            'chatId': str(c['_id']),
            'createdAt': c['createdAt'].isoformat(),
            'chatStatus': chat_status,
            'openedByAdmin': opened_by_admin,
            'openedAt': c['openedAt'].isoformat() if isinstance(c.get('openedAt'), datetime) else None,
            'lastAdminReadAt': last_admin_read_at.isoformat() if last_admin_read_at else None,
            'status': status,
            'category': c.get('category') if isinstance(c.get('category'), str) else None,
            'attachedPersonId': attached_person_id,
            'attachedPerson': people_by_id.get(attached_person_id) if attached_person_id else None,
            'unreadCount': unread_count
        })
    return jsonify({
        'chats': out,
        'totalUnread': total_unread
    })


@application.route('/api/chats/<chat_id>/open', methods=['PATCH'])
def mark_chat_open(chat_id):
    """Mark chat as opened by admin (idempotent)."""
    try:
        cid = ObjectId(chat_id)
    except Exception:
        abort(400, 'Invalid chat_id')

    current = chat_collection.find_one(
        {'_id': cid},
        {'chatStatus': 1, 'openedByAdmin': 1, 'openedAt': 1, 'category': 1, 'lastAdminReadAt': 1, 'attachedPersonId': 1}
    )
    if current and str(current.get('chatStatus') or '').lower() == 'history':
        opened_at = current.get('openedAt')
        last_admin_read_at = current.get('lastAdminReadAt')
        attached_person_id = current.get('attachedPersonId') if isinstance(current.get('attachedPersonId'), str) else None
        attached_person_id = attached_person_id.strip() if attached_person_id else None
        attached_person = None
        if attached_person_id:
            try:
                person_doc = people_collection.find_one(
                    {'_id': ObjectId(attached_person_id)},
                    {'name': 1, 'birthYear': 1, 'deathYear': 1, 'avatarUrl': 1}
                )
                attached_person = _normalize_attached_person(person_doc)
            except Exception:
                attached_person = None
        return jsonify({
            'chatId': chat_id,
            'chatStatus': 'history',
            'status': 'history',
            'openedByAdmin': bool(current.get('openedByAdmin', True)),
            'openedAt': opened_at.isoformat() if isinstance(opened_at, datetime) else None,
            'lastAdminReadAt': last_admin_read_at.isoformat() if isinstance(last_admin_read_at, datetime) else None,
            'category': current.get('category') if isinstance(current.get('category'), str) else None,
            'attachedPersonId': attached_person_id,
            'attachedPerson': attached_person
        }), 200

    now = datetime.utcnow()
    chat_collection.update_one(
        {'_id': cid},
        {
            '$set': {'openedByAdmin': True, 'lastAdminReadAt': now},
            '$setOnInsert': {
                'createdAt': now,
                'chatStatus': 'open',
                'openedAt': now,
                'category': None,
                'attachedPersonId': None
            }
        },
        upsert=True
    )

    # Keep openedAt as "first open" timestamp.
    chat_collection.update_one(
        {'_id': cid, '$or': [{'openedAt': {'$exists': False}}, {'openedAt': None}]},
        {'$set': {'openedAt': now}}
    )

    updated = chat_collection.find_one(
        {'_id': cid},
        {'chatStatus': 1, 'openedByAdmin': 1, 'openedAt': 1, 'category': 1, 'lastAdminReadAt': 1, 'attachedPersonId': 1}
    )
    opened_at = updated.get('openedAt') if updated else None
    last_admin_read_at = updated.get('lastAdminReadAt') if updated else None
    attached_person_id = updated.get('attachedPersonId') if updated and isinstance(updated.get('attachedPersonId'), str) else None
    attached_person_id = attached_person_id.strip() if attached_person_id else None
    attached_person = None
    if attached_person_id:
        try:
            person_doc = people_collection.find_one(
                {'_id': ObjectId(attached_person_id)},
                {'name': 1, 'birthYear': 1, 'deathYear': 1, 'avatarUrl': 1}
            )
            attached_person = _normalize_attached_person(person_doc)
        except Exception:
            attached_person = None
    return jsonify({
        'chatId': chat_id,
        'chatStatus': 'open',
        'status': 'open',
        'openedByAdmin': True,
        'openedAt': opened_at.isoformat() if isinstance(opened_at, datetime) else None,
        'lastAdminReadAt': last_admin_read_at.isoformat() if isinstance(last_admin_read_at, datetime) else None,
        'category': updated.get('category') if updated and isinstance(updated.get('category'), str) else None,
        'attachedPersonId': attached_person_id,
        'attachedPerson': attached_person
    }), 200


@application.route('/api/chats/<chat_id>/close', methods=['PATCH'])
def close_chat(chat_id):
    """Mark chat as history (idempotent)."""
    try:
        cid = ObjectId(chat_id)
    except Exception:
        abort(400, 'Invalid chat_id')

    result = chat_collection.update_one({'_id': cid}, {'$set': {'chatStatus': 'history'}})
    if result.matched_count == 0:
        abort(404, 'Chat not found')

    return jsonify({
        'chatId': chat_id,
        'chatStatus': 'history',
        'status': 'history'
    }), 200


def _normalize_chat_template(doc):
    if not isinstance(doc, dict):
        return None
    return {
        'id': str(doc.get('_id')),
        'title': str(doc.get('title') or ''),
        'text': str(doc.get('text') or ''),
        'createdAt': doc.get('createdAt').isoformat() if isinstance(doc.get('createdAt'), datetime) else None,
        'updatedAt': doc.get('updatedAt').isoformat() if isinstance(doc.get('updatedAt'), datetime) else None
    }


def _validate_chat_template_payload(data):
    if not isinstance(data, dict):
        abort(400, 'Invalid payload')

    raw_title = data.get('title')
    raw_text = data.get('text')
    if not isinstance(raw_title, str) or not isinstance(raw_text, str):
        abort(400, 'Invalid payload')

    title = raw_title.strip()
    text = raw_text.strip()

    if not title or not text:
        abort(400, 'title and text are required')
    if len(title) > CHAT_TEMPLATE_TITLE_MAX_LEN:
        abort(400, 'title is too long')
    if len(text) > CHAT_TEMPLATE_TEXT_MAX_LEN:
        abort(400, 'text is too long')

    return title, text


@application.route('/api/chat-templates', methods=['GET'])
def list_chat_templates():
    docs = chat_templates_collection.find().sort('updatedAt', -1)
    return jsonify([_normalize_chat_template(doc) for doc in docs]), 200


@application.route('/api/chat-templates', methods=['POST'])
def create_chat_template():
    data = request.get_json(silent=True) or {}
    title, text = _validate_chat_template_payload(data)
    now = datetime.utcnow()

    result = chat_templates_collection.insert_one({
        'title': title,
        'text': text,
        'createdAt': now,
        'updatedAt': now
    })

    created = chat_templates_collection.find_one({'_id': result.inserted_id})
    return jsonify(_normalize_chat_template(created)), 201


@application.route('/api/chat-templates/<template_id>', methods=['PATCH'])
def update_chat_template(template_id):
    try:
        tid = ObjectId(template_id)
    except Exception:
        abort(400, 'Invalid template_id')

    data = request.get_json(silent=True) or {}
    title, text = _validate_chat_template_payload(data)

    result = chat_templates_collection.update_one(
        {'_id': tid},
        {'$set': {'title': title, 'text': text, 'updatedAt': datetime.utcnow()}}
    )
    if result.matched_count == 0:
        abort(404, 'Template not found')

    updated = chat_templates_collection.find_one({'_id': tid})
    return jsonify(_normalize_chat_template(updated)), 200


@application.route('/api/chat-templates/<template_id>', methods=['DELETE'])
def delete_chat_template(template_id):
    try:
        tid = ObjectId(template_id)
    except Exception:
        abort(400, 'Invalid template_id')

    result = chat_templates_collection.delete_one({'_id': tid})
    if result.deleted_count == 0:
        abort(404, 'Template not found')

    return jsonify({'id': template_id}), 200


@application.route('/api/chats/<chat_id>/category', methods=['PATCH'])
def set_chat_category(chat_id):
    """Set or clear chat category (idempotent)."""
    try:
        cid = ObjectId(chat_id)
    except Exception:
        abort(400, 'Invalid chat_id')

    data = request.get_json(silent=True) or {}
    raw_category = data.get('category')

    if raw_category is None:
        category = None
    elif isinstance(raw_category, str):
        cleaned = raw_category.strip()
        if cleaned in ('', 'Категорія'):
            category = None
        elif cleaned in ALLOWED_CHAT_CATEGORIES:
            category = cleaned
        else:
            abort(400, 'Invalid category')
    else:
        abort(400, 'Invalid category')

    result = chat_collection.update_one({'_id': cid}, {'$set': {'category': category}})
    if result.matched_count == 0:
        abort(404, 'Chat not found')

    return jsonify({
        'chatId': chat_id,
        'category': category
    }), 200


@application.route('/api/chats/<chat_id>/profile', methods=['PATCH'])
def set_chat_profile(chat_id):
    """Attach or clear a profile for chat (idempotent)."""
    try:
        cid = ObjectId(chat_id)
    except Exception:
        abort(400, 'Invalid chat_id')

    data = request.get_json(silent=True) or {}
    raw_person_id = data.get('personId')
    attached_person_id = None
    attached_person = None

    if raw_person_id is None:
        attached_person_id = None
    elif isinstance(raw_person_id, str):
        cleaned = raw_person_id.strip()
        if not cleaned:
            attached_person_id = None
        else:
            try:
                person_oid = ObjectId(cleaned)
            except Exception:
                abort(400, 'Invalid personId')
            person_doc = people_collection.find_one(
                {'_id': person_oid},
                {'name': 1, 'birthYear': 1, 'deathYear': 1, 'avatarUrl': 1}
            )
            if not person_doc:
                abort(404, 'Person not found')
            attached_person_id = str(person_oid)
            attached_person = _normalize_attached_person(person_doc)
    else:
        abort(400, 'Invalid personId')

    result = chat_collection.update_one({'_id': cid}, {'$set': {'attachedPersonId': attached_person_id}})
    if result.matched_count == 0:
        abort(404, 'Chat not found')

    socketio.emit('adminChatUpdated', {'chatId': chat_id}, room=ADMINS_ROOM)

    return jsonify({
        'chatId': chat_id,
        'attachedPersonId': attached_person_id,
        'attachedPerson': attached_person
    }), 200


@application.route('/api/chats/history-search', methods=['GET'])
def history_search_chats():
    raw_q = request.args.get('q', '')
    raw_date_from = request.args.get('dateFrom', '')
    raw_date_to = request.args.get('dateTo', '')
    raw_category = request.args.get('category', '')
    raw_person_id = request.args.get('personId', '')

    keyword = raw_q.strip()
    category = raw_category.strip()
    if category in ('', 'Категорія'):
        category = ''
    person_id = raw_person_id.strip()
    if person_id in ('', 'null', 'None'):
        person_id = ''

    date_from = None
    date_to = None
    if raw_date_from:
        try:
            date_from = datetime.strptime(raw_date_from, '%Y-%m-%d')
        except Exception:
            abort(400, 'Invalid dateFrom')
    if raw_date_to:
        try:
            date_to = datetime.strptime(raw_date_to, '%Y-%m-%d') + timedelta(days=1)
        except Exception:
            abort(400, 'Invalid dateTo')

    chat_filter = {'chatStatus': 'history'}
    if category:
        chat_filter['category'] = category
    if person_id:
        chat_filter['attachedPersonId'] = person_id

    history_docs = list(chat_collection.find(chat_filter, {'_id': 1}))
    if not history_docs:
        return jsonify({'chatIds': []}), 200

    history_ids = [doc['_id'] for doc in history_docs]

    msg_filter = {'chatId': {'$in': history_ids}}
    if keyword:
        msg_filter['text'] = {'$regex': re.escape(keyword), '$options': 'i'}

    candidate_chat_ids = set(message_collection.distinct('chatId', msg_filter))
    if not candidate_chat_ids:
        return jsonify({'chatIds': []}), 200

    latest_by_chat = {}
    projection = {'chatId': 1, 'createdAt': 1}
    for msg in message_collection.find({'chatId': {'$in': list(candidate_chat_ids)}}, projection).sort('createdAt', -1):
        cid = msg.get('chatId')
        created_at = msg.get('createdAt')
        if cid in latest_by_chat:
            continue
        if not isinstance(created_at, datetime):
            continue
        latest_by_chat[cid] = created_at

    if not latest_by_chat:
        return jsonify({'chatIds': []}), 200

    filtered = []
    for cid, last_dt in latest_by_chat.items():
        if date_from and last_dt < date_from:
            continue
        if date_to and last_dt >= date_to:
            continue
        filtered.append((cid, last_dt))

    filtered.sort(key=lambda item: item[1], reverse=True)

    return jsonify({
        'chatIds': [str(cid) for cid, _ in filtered]
    }), 200


@application.route('/api/chats/<chat_id>/messages', methods=['GET'])
def get_messages(chat_id):
    """Fetch full message history."""
    try:
        cid = ObjectId(chat_id)
    except:
        abort(400, 'Invalid chat_id')
    msgs = message_collection.find({'chatId': cid}).sort('createdAt', 1)
    out = []
    for m in msgs:
        out.append({
            'sender': m['sender'],
            'text': m['text'],
            'createdAt': m['createdAt'].isoformat(),
            'imageData': m.get('imageData'),
            'attachments': _message_attachments_for_output(m)
        })
    return jsonify(out)


_CHAT_ATTACHMENT_TYPES = {"image", "video"}


def _parse_data_url_mime(data_url):
    if not isinstance(data_url, str):
        return ""
    match = re.match(r"^data:([^;,]+)", data_url.strip(), flags=re.IGNORECASE)
    return (match.group(1) or "").strip() if match else ""


def _normalize_chat_attachment(item):
    if not isinstance(item, dict):
        return None
    media_type = str(item.get("type") or "").strip().lower()
    url = str(item.get("url") or "").strip()
    if media_type not in _CHAT_ATTACHMENT_TYPES or not url:
        return None
    mime_type = str(item.get("mimeType") or "").strip() or _parse_data_url_mime(url)
    out = {"type": media_type, "url": url}
    if mime_type:
        out["mimeType"] = mime_type
    return out


def _attachments_from_image_data(image_data):
    if isinstance(image_data, list):
        values = image_data
    elif isinstance(image_data, str) and image_data.strip():
        values = [image_data]
    else:
        values = []

    out = []
    for value in values:
        url = str(value or "").strip()
        if not url:
            continue
        item = {"type": "image", "url": url}
        mime_type = _parse_data_url_mime(url)
        if mime_type:
            item["mimeType"] = mime_type
        out.append(item)
    return out


def _message_attachments_for_output(message):
    raw_attachments = message.get("attachments")
    out = []
    if isinstance(raw_attachments, list):
        for item in raw_attachments:
            normalized = _normalize_chat_attachment(item)
            if normalized:
                out.append(normalized)
    if out:
        return out
    return _attachments_from_image_data(message.get("imageData"))


@application.route('/api/chats/<chat_id>/messages', methods=['POST'])
def post_message(chat_id):
    """Post a message (user or admin) with optional media attachments and broadcast."""
    sender = None
    text = ''
    image_files = []
    attachments = []

    # Розбір multipart/form-data або JSON
    if request.content_type and 'multipart/form-data' in request.content_type:
        sender = request.form.get('sender')
        text = (request.form.get('text', '') or '').strip()
        image_files = request.files.getlist('image')
    else:
        data = request.get_json() or {}
        sender = data.get('sender')
        text = (data.get('text') or '').strip()
        raw_attachments = data.get('attachments', [])
        if raw_attachments is None:
            raw_attachments = []
        if not isinstance(raw_attachments, list):
            abort(400, 'Invalid payload')
        if len(raw_attachments) > 5:
            abort(400, 'Too many attachments')
        for raw_item in raw_attachments:
            normalized = _normalize_chat_attachment(raw_item)
            if not normalized:
                abort(400, 'Invalid attachment')
            attachments.append(normalized)

    image_files = [img for img in image_files if img and img.filename]
    if len(image_files) > 5:
        abort(400, 'Too many images')
    if len(image_files) + len(attachments) > 5:
        abort(400, 'Too many attachments')

    if sender not in ('user', 'admin') or (not text and not image_files and not attachments):
        abort(400, 'Invalid payload')

    try:
        cid = ObjectId(chat_id)
    except Exception:
        abort(400, 'Invalid chat_id')

    # Перевіряємо чи є попередні повідомлення від користувача
    existing_user_msgs = message_collection.count_documents({'chatId': cid, 'sender': 'user'})

    # Створюємо чат, якщо його ще не було
    chat_collection.update_one(
        {'_id': cid},
        {
            '$setOnInsert': {
                'createdAt': datetime.utcnow(),
                'chatStatus': 'open',
                'openedByAdmin': False,
                'openedAt': None,
                'category': None,
                'lastAdminReadAt': None,
                'attachedPersonId': None
            }
        },
        upsert=True
    )

    # Кодуємо зображення, якщо воно є (legacy multipart flow)
    image_data = None
    message_attachments = list(attachments)
    if image_files:
        if len(image_files) == 1:
            raw = image_files[0].read()
            b64 = base64.b64encode(raw).decode('utf-8')
            image_data = f"data:{image_files[0].mimetype};base64,{b64}"
        else:
            image_data = []
            for image in image_files:
                raw = image.read()
                b64 = base64.b64encode(raw).decode('utf-8')
                image_data.append(f"data:{image.mimetype};base64,{b64}")
        message_attachments.extend(_attachments_from_image_data(image_data))

    # Зберігаємо повідомлення
    msg = {
        'chatId': cid,
        'sender': sender,
        'text': text,
        'createdAt': datetime.utcnow(),
        'imageData': image_data,
        'attachments': message_attachments
    }
    message_collection.insert_one(msg)

    # Розсилаємо повідомлення через Socket.IO
    payload = {
        'sender': sender,
        'text': text,
        'imageData': image_data,
        'attachments': message_attachments,
        'createdAt': msg['createdAt'].isoformat()
    }
    socketio.emit('newMessage', payload, room=chat_id)
    socketio.emit('adminChatUpdated', {'chatId': chat_id}, room=ADMINS_ROOM)

    # Автовідповідь на перше повідомлення користувача
    if sender == 'user' and existing_user_msgs == 0:
        followup = {
            'chatId': cid,
            'sender': 'admin',
            'text': "Дякуємо за Ваше повідомлення! Наш спеціаліст відповість Вам протягом 5 хвилин",
            'createdAt': datetime.utcnow(),
            'imageData': None,
            'attachments': []
        }
        time.sleep(2)
        message_collection.insert_one(followup)
        socketio.emit('newMessage', {
            'sender': followup['sender'],
            'text': followup['text'],
            'imageData': followup['imageData'],
            'attachments': followup['attachments'],
            'createdAt': followup['createdAt'].isoformat()
        }, room=chat_id)
        socketio.emit('adminChatUpdated', {'chatId': chat_id}, room=ADMINS_ROOM)

    return jsonify({'success': True}), 201


@application.route("/api/liturgies", methods=["POST"])
def create_liturgy():
    """
    Minimal create: keep only createdAt, serviceDate, churchName, person, price.
    Expected JSON:
    {
      "personId": "<ObjectId string>",   # required
      "date": "2025-09-21" | "2025-09-21T10:00",  # required
      "time": "10:00",                   # optional (ignored if date has time)
      "churchName": "Святоюрський собор",# optional
      "price": 500,                      # optional (number)
      "phone": "+380(67)-123-4567",      # optional
      "invoiceId": "monopay_invoice",    # optional
      "paymentMethod": "online",         # optional
      "paymentStatus": "pending",        # optional
      "personName": "Ім'я Прізвище"      # optional
    }
    """
    data = request.get_json(silent=True) or {}

    # person
    person_id = (data.get("personId") or "").strip()
    if not person_id:
        abort(400, "personId is required")
    try:
        person_oid = ObjectId(person_id)
    except Exception:
        abort(400, "Invalid personId")

    # date/time -> serviceDate (datetime)
    raw_date = (data.get("date") or "").strip()
    raw_time = (data.get("time") or "").strip()
    if not raw_date:
        abort(400, "date is required (YYYY-MM-DD or ISO-8601)")
    try:
        if "T" in raw_date:
            service_dt = datetime.fromisoformat(raw_date)
        else:
            service_dt = datetime.fromisoformat(
                f"{raw_date}T{raw_time}" if raw_time else f"{raw_date}T00:00"
            )
    except ValueError:
        abort(400, "Invalid date/time format. Use YYYY-MM-DD and optional HH:MM or full ISO-8601")

    # optional fields
    church_name = (data.get("churchName") or "").strip() or None
    price = data.get("price")  # keep as-is if number or None
    phone = (data.get("phone") or "").strip() or None
    invoice_id = (data.get("invoiceId") or "").strip() or None
    payment_method = (data.get("paymentMethod") or "").strip() or None
    payment_status = (data.get("paymentStatus") or "").strip() or None
    person_name = (data.get("personName") or "").strip() or None
    if invoice_id and not payment_status:
        payment_status = "pending"

    now_utc = datetime.now(timezone.utc)

    # Only the required minimal fields are persisted
    doc = {
        "person": person_oid,
        "personName": person_name,
        "serviceDate": service_dt,
        "churchName": church_name,
        "price": price,
        "phone": phone,
        "invoiceId": invoice_id,
        "paymentMethod": payment_method,
        "paymentStatus": payment_status,
        "createdAt": now_utc,
    }

    ins = liturgies_collection.insert_one(doc)

    # Minimal response with only the requested fields
    out = {
        "_id": str(ins.inserted_id),
        "person": str(doc["person"]),
        "personName": doc.get("personName"),
        "serviceDate": doc["serviceDate"].isoformat(),
        "churchName": doc["churchName"],
        "price": doc["price"],
        "phone": doc.get("phone"),
        "invoiceId": doc.get("invoiceId"),
        "paymentMethod": doc.get("paymentMethod"),
        "paymentStatus": doc.get("paymentStatus"),
        "createdAt": doc["createdAt"].isoformat(),
    }
    return jsonify(out), 201


@application.route("/api/people/<person_id>/liturgies", methods=["GET"])
def list_liturgies(person_id):
    """Return all liturgies for a given person, sorted by serviceDate ascending."""
    try:
        person_oid = ObjectId(person_id)
    except Exception:
        abort(400, "Invalid personId")

    cursor = liturgies_collection.find({"person": person_oid}).sort("serviceDate", ASCENDING)

    results = []
    for doc in cursor:
        results.append({
            "_id": str(doc["_id"]),
            "person": str(doc["person"]),
            "personName": doc.get("personName"),
            "serviceDate": doc["serviceDate"].isoformat(),
            "churchName": doc.get("churchName"),
            "price": doc.get("price"),
            "phone": doc.get("phone"),
            "invoiceId": doc.get("invoiceId"),
            "paymentMethod": doc.get("paymentMethod"),
            "paymentStatus": doc.get("paymentStatus"),
            "createdAt": doc["createdAt"].isoformat(),
        })
    return jsonify(results)


@application.route("/api/liturgies/payment-status", methods=["GET"])
def liturgy_payment_status():
    invoice_id = (request.args.get("invoiceId") or "").strip()
    if not invoice_id:
        abort(400, "invoiceId is required")

    doc = liturgies_collection.find_one({"invoiceId": invoice_id})
    if not doc:
        return jsonify({"status": "not_found"}), 404

    return jsonify({
        "status": doc.get("paymentStatus") or "unknown",
        "invoiceId": invoice_id
    })


def _normalize_weekday(value):
    """
    Normalize a weekday representation to an integer in [0, 6] (Sunday=0 ... Saturday=6).
    Accepts integers or strings containing an integer.
    """
    if isinstance(value, int):
        day = value
    elif isinstance(value, str):
        v = value.strip()
        if not v.isdigit():
            abort(400, description="Weekdays must be integers 0..6")
        day = int(v, 10)
    else:
        abort(400, description="Weekdays must be integers 0..6")

    if day < 0 or day > 6:
        abort(400, description="Weekdays must be in range 0..6 (Sunday=0 ... Saturday=6)")
    return day


@application.route("/api/liturgy/church-active-days", methods=["GET"])
def get_church_active_days():
    """
    Return active weekdays for a given church name.
    Query: ?churchName=...
    - If churchName is missing/empty → treat as “no church selected” and return empty days.
    - If church has no stored config → also return empty days.
    Response:
      {
        "churchName": "<name or null>",
        "activeWeekdays": [0,1,2]  # Sunday=0 ... Saturday=6
      }
    """
    name = (request.args.get("churchName") or "").strip()
    if not name:
        # No church selected on frontend
        return jsonify({"churchName": None, "activeWeekdays": []})

    church = churches_collection.find_one(
        {"name": name},
        {"name": 1, "workingDays": 1, "churchDays": 1}
    )
    if not church:
        # Known church with no config yet → empty schedule
        return jsonify({"churchName": name, "activeWeekdays": []})

    cleaned = []
    church_days_raw = church.get("churchDays")
    if isinstance(church_days_raw, dict):
        raw_days = church_days_raw.get("activeWeekdays")
        if isinstance(raw_days, list):
            for d in raw_days:
                try:
                    cleaned.append(_normalize_weekday(d))
                except Exception:
                    # ignore malformed stored values instead of failing the request
                    continue

    if not cleaned and isinstance(church.get("workingDays"), list):
        cleaned = _day_ids_to_weekdays(church.get("workingDays"))

    cleaned = sorted(set(cleaned))
    return jsonify({"churchName": church.get("name"), "activeWeekdays": cleaned})


@application.route("/api/liturgy/church-active-days", methods=["PUT"])
def set_church_active_days():
    """
    Update active weekdays configuration for an existing church.
    Expected JSON:
      {
        "churchName": "Собор Св. Юра",          # required, existing church name
        "activeWeekdays": [0, 1, 2, 3, 4]      # required, array of ints (0=Sun..6=Sat)
      }
    """
    payload = request.get_json(silent=True) or {}
    name = (payload.get("churchName") or "").strip()
    if not name:
        abort(400, description="'churchName' is required")

    raw_days = payload.get("activeWeekdays")
    if not isinstance(raw_days, list):
        abort(400, description="'activeWeekdays' must be an array of integers 0..6")

    cleaned = []
    for d in raw_days:
        cleaned.append(_normalize_weekday(d))

    cleaned = sorted(set(cleaned))

    church = churches_collection.find_one(
        {"name": name},
        {"churchDays": 1}
    )
    if not church:
        abort(404, description="Church not found")

    existing_liturgy = {}
    church_days_raw = church.get("churchDays")
    if isinstance(church_days_raw, dict) and isinstance(church_days_raw.get("liturgyByWeekday"), dict):
        for key, raw_time in church_days_raw.get("liturgyByWeekday").items():
            try:
                weekday = _normalize_weekday(key)
            except Exception:
                continue
            cleaned_time = _clean_str(raw_time)
            if cleaned_time:
                existing_liturgy[str(weekday)] = cleaned_time

    existing_liturgy = {
        key: value for key, value in existing_liturgy.items() if int(key) in cleaned
    }
    church_days = {
        "activeWeekdays": cleaned,
        "liturgyByWeekday": existing_liturgy,
    }

    churches_collection.update_one(
        {"name": name},
        {"$set": {
            "churchDays": church_days,
            "workingDays": _weekdays_to_day_ids(cleaned),
            "liturgyEndTimes": _legacy_liturgy_from_church_days(church_days),
            "updatedAt": datetime.utcnow(),
        }},
    )

    return jsonify({"churchName": name, "activeWeekdays": cleaned})


@application.route("/api/media/compress/image", methods=["POST"])
def compress_image():
    """
    Compress/resize an image and return the optimized file.
    Multipart form-data:
      - file: image file
    Query params (optional):
      - maxWidth (int): max width in px (default 1600)
      - maxHeight (int): max height in px (default 1600)
      - quality (int): 1..95 (default 82)
      - format: jpeg|jpg|webp|png (default jpeg)
    """
    if Image is None:
        return jsonify({"error": "Pillow is not installed"}), 500

    file = request.files.get("file")
    if not file:
        return jsonify({"error": "file required"}), 400

    def _int_param(name, default, min_v=None, max_v=None):
        raw = request.args.get(name)
        if raw is None or raw == "":
            return default
        try:
            val = int(raw)
        except Exception:
            return default
        if min_v is not None and val < min_v:
            val = min_v
        if max_v is not None and val > max_v:
            val = max_v
        return val

    max_w = _int_param("maxWidth", 1600, 200, 8000)
    max_h = _int_param("maxHeight", 1600, 200, 8000)
    quality = _int_param("quality", 82, 30, 95)
    fmt_raw = (request.args.get("format") or "jpeg").strip().lower()
    if fmt_raw in ("jpg", "jpeg"):
        fmt = "JPEG"
        mime = "image/jpeg"
        ext = "jpg"
    elif fmt_raw == "webp":
        fmt = "WEBP"
        mime = "image/webp"
        ext = "webp"
    elif fmt_raw == "png":
        fmt = "PNG"
        mime = "image/png"
        ext = "png"
    else:
        fmt = "JPEG"
        mime = "image/jpeg"
        ext = "jpg"

    try:
        img = Image.open(file.stream)
        if ImageOps is not None:
            img = ImageOps.exif_transpose(img)
        resample = getattr(Image, "Resampling", Image).LANCZOS
        img.thumbnail((max_w, max_h), resample)
        if fmt == "JPEG" and img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        out = io.BytesIO()
        save_kwargs = {}
        if fmt in ("JPEG", "WEBP"):
            save_kwargs["quality"] = quality
            save_kwargs["optimize"] = True
        img.save(out, format=fmt, **save_kwargs)
        out.seek(0)
    except Exception as e:
        return jsonify({"error": f"failed to compress image: {e}"}), 500

    filename = os.path.splitext(file.filename or "image")[0] or "image"
    download_name = f"{filename}.{ext}"
    return send_file(out, mimetype=mime, as_attachment=False, download_name=download_name)


def _int_param(name, default, min_v=None, max_v=None):
    raw = request.args.get(name)
    if raw is None or raw == "":
        return default
    try:
        val = int(raw)
    except Exception:
        return default
    if min_v is not None and val < min_v:
        val = min_v
    if max_v is not None and val > max_v:
        val = max_v
    return val

def _parse_video_compress_params():
    max_w = _int_param("maxWidth", 1280, 320, 3840)
    max_h = _int_param("maxHeight", 720, 240, 2160)
    crf = _int_param("crf", 28, 18, 35)
    audio_bitrate = _int_param("audioBitrate", 128, 32, 320)

    preset = (request.args.get("preset") or "veryfast").strip().lower()
    allowed_presets = {
        "ultrafast",
        "superfast",
        "veryfast",
        "faster",
        "fast",
        "medium",
        "slow",
        "slower",
        "veryslow",
    }
    if preset not in allowed_presets:
        preset = "veryfast"

    fmt_raw = (request.args.get("format") or "mp4").strip().lower()
    if fmt_raw != "mp4":
        fmt_raw = "mp4"
    ext = "mp4"
    mime = "video/mp4"
    return {
        "max_w": max_w,
        "max_h": max_h,
        "crf": crf,
        "audio_bitrate": audio_bitrate,
        "preset": preset,
        "ext": ext,
        "mime": mime,
    }

def _probe_video_duration(path):
    try:
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=nk=1:nw=1",
            path,
        ]
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8", errors="ignore").strip()
        if not out:
            return None
        return float(out)
    except Exception:
        return None

def _run_video_compress_job(job_id):
    job = _get_compress_job(job_id)
    if not job:
        return

    params = job["params"]
    in_path = job["input_path"]
    out_path = job["output_path"]
    duration = job.get("duration")
    tmp_dir = job.get("tmp_dir")

    scale = f"scale='min(iw,{params['max_w']})':'min(ih,{params['max_h']})':force_original_aspect_ratio=decrease"
    vf = f"{scale},scale=trunc(iw/2)*2:trunc(ih/2)*2"
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        in_path,
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-vf",
        vf,
        "-c:v",
        "libx264",
        "-preset",
        params["preset"],
        "-crf",
        str(params["crf"]),
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        "-c:a",
        "aac",
        "-b:a",
        f"{params['audio_bitrate']}k",
        "-ac",
        "2",
        "-ar",
        "44100",
        "-progress",
        "pipe:1",
        "-nostats",
        out_path,
    ]

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if proc.stdout:
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                if line.startswith("out_time_ms=") and duration:
                    try:
                        out_ms = int(line.split("=", 1)[1])
                        pct = max(0.0, min(100.0, (out_ms / (duration * 1_000_000.0)) * 100.0))
                        _set_compress_job(job_id, percent=pct, status="compressing")
                    except Exception:
                        pass
                elif line.startswith("progress="):
                    val = line.split("=", 1)[1].strip()
                    if val == "end":
                        _set_compress_job(job_id, percent=100.0, status="done")
        rc = proc.wait()
        if rc != 0:
            _set_compress_job(job_id, status="error", error="failed to compress video")
            if tmp_dir:
                shutil.rmtree(tmp_dir, ignore_errors=True)
        else:
            _set_compress_job(job_id, status="done", percent=100.0)
    except Exception as e:
        _set_compress_job(job_id, status="error", error=f"failed to compress video: {e}")
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)

@application.route("/api/media/compress/video/start", methods=["POST"])
def compress_video_start():
    """
    Start a video compression job and return a jobId for progress polling.
    Multipart form-data:
      - file: video file
    Query params: same as /api/media/compress/video
    """
    if shutil.which("ffmpeg") is None:
        return jsonify({"error": "ffmpeg is not installed"}), 500

    file = request.files.get("file")
    if not file:
        return jsonify({"error": "file required"}), 400

    params = _parse_video_compress_params()
    tmp_dir = tempfile.mkdtemp(prefix="compress_video_")
    in_path = os.path.join(tmp_dir, "input")
    out_path = os.path.join(tmp_dir, f"output.{params['ext']}")

    try:
        file.save(in_path)
    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return jsonify({"error": f"failed to save input: {e}"}), 500

    duration = _probe_video_duration(in_path)
    job_id = secrets.token_urlsafe(16)
    with _compress_video_lock:
        _compress_video_jobs[job_id] = {
            "status": "queued",
            "percent": 0.0,
            "error": None,
            "tmp_dir": tmp_dir,
            "input_path": in_path,
            "output_path": out_path,
            "params": params,
            "duration": duration,
            "filename": os.path.splitext(file.filename or "video")[0] or "video",
            "created_at": time.time(),
        }

    _set_compress_job(job_id, status="compressing")
    eventlet.spawn_n(_run_video_compress_job, job_id)

    return jsonify({"jobId": job_id}), 200

@application.route("/api/media/compress/video/progress", methods=["GET"])
def compress_video_progress():
    job_id = request.args.get("jobId") or ""
    job = _get_compress_job(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    return jsonify({
        "status": job.get("status"),
        "percent": job.get("percent"),
        "error": job.get("error"),
    }), 200

@application.route("/api/media/compress/video/result", methods=["GET"])
def compress_video_result():
    job_id = request.args.get("jobId") or ""
    job = _get_compress_job(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    if job.get("status") != "done":
        return jsonify({"status": job.get("status"), "error": job.get("error")}), 202

    out_path = job.get("output_path")
    if not out_path or not os.path.exists(out_path):
        return jsonify({"error": "output missing"}), 500

    @after_this_request
    def _cleanup(response):
        tmp_dir = job.get("tmp_dir")
        _remove_compress_job(job_id)
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return response

    download_name = f"{job.get('filename')}.{job['params']['ext']}"
    return send_file(out_path, mimetype=job["params"]["mime"], as_attachment=False, download_name=download_name)


@application.route("/api/media/compress/video", methods=["POST"])
def compress_video():
    """
    Compress/resize a video and return the optimized MP4.
    Multipart form-data:
      - file: video file
    Query params (optional):
      - maxWidth (int): max width in px (default 1280)
      - maxHeight (int): max height in px (default 720)
      - crf (int): 18..35 (default 28)
      - preset: ultrafast|superfast|veryfast|faster|fast|medium|slow|slower|veryslow (default veryfast)
      - audioBitrate (int): kbps (default 128)
      - format: mp4 (default mp4)
    """
    if shutil.which("ffmpeg") is None:
        return jsonify({"error": "ffmpeg is not installed"}), 500

    file = request.files.get("file")
    if not file:
        return jsonify({"error": "file required"}), 400

    params = _parse_video_compress_params()

    tmp_dir = tempfile.mkdtemp(prefix="compress_video_")
    in_path = os.path.join(tmp_dir, "input")
    out_path = os.path.join(tmp_dir, f"output.{params['ext']}")

    try:
        file.save(in_path)
        scale = f"scale='min(iw,{params['max_w']})':'min(ih,{params['max_h']})':force_original_aspect_ratio=decrease"
        vf = f"{scale},scale=trunc(iw/2)*2:trunc(ih/2)*2"
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            in_path,
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-vf",
            vf,
            "-c:v",
            "libx264",
            "-preset",
            params["preset"],
            "-crf",
            str(params["crf"]),
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            "-c:a",
            "aac",
            "-b:a",
            f"{params['audio_bitrate']}k",
            "-ac",
            "2",
            "-ar",
            "44100",
            out_path,
        ]
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    except subprocess.CalledProcessError as e:
        err = e.stderr.decode("utf-8", errors="ignore")
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return jsonify({"error": "failed to compress video", "details": err[-2000:]}), 500
    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return jsonify({"error": f"failed to compress video: {e}"}), 500

    @after_this_request
    def _cleanup(response):
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return response

    filename = os.path.splitext(file.filename or "video")[0] or "video"
    download_name = f"{filename}.{params['ext']}"
    return send_file(out_path, mimetype=params["mime"], as_attachment=False, download_name=download_name)


@application.route("/api/spaces/video-upload-url", methods=["GET"])
def spaces_video_upload_url():
    """
    Issues a presigned PUT URL so the browser can upload a video straight to DigitalOcean Spaces.
    Query params:
      - filename (required): original file name (used to derive object key)
      - contentType (optional): e.g., video/mp4, video/quicktime; defaults to application/octet-stream
      - prefix (optional): folder prefix, defaults to "videos/"
    Returns:
      {
        "uploadUrl": "<presigned PUT URL>",
        "objectUrl": "https://<bucket>.<region>.digitaloceanspaces.com/<key>",
        "key": "<key>",
        "expiresIn": 600
      }
    """

    filename = (request.args.get("filename") or "").strip()
    contentType = (request.args.get("contentType") or "application/octet-stream").strip()
    prefix = (request.args.get("prefix") or "videos/").strip()

    if not filename:
        return jsonify({"error": "filename required"}), 400

    # Basic filename hardening
    # keep alnum, dash, underscore, dot; replace others with '_'
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", filename)
    if not safe_name:
        safe_name = f"video_{int(time.time())}.mp4"

    # Compose object key: prefix + timestamped filename to avoid collisions
    ts = int(time.time())
    key = f"{prefix.rstrip('/')}/{ts}_{safe_name}"

    try:
        # Generate presigned PUT URL (10 minutes)
        presigned_url = s3.generate_presigned_url(
            ClientMethod="put_object",
            Params={
                "Bucket": SPACES_BUCKET,
                "Key": key,
            },
            ExpiresIn=600,
            HttpMethod="PUT",
        )

        # Public URL (origin). If you use a CDN endpoint, replace this with your CDN base.
        object_url = f"https://{SPACES_BUCKET}.{SPACES_REGION}.digitaloceanspaces.com/{key}"

        return jsonify({
            "uploadUrl": presigned_url,
            "objectUrl": object_url,
            "key": key,
            "expiresIn": 600
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@application.route("/api/spaces/make-public", methods=["POST"])
def spaces_make_public():
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not key:
        return jsonify({"error": "key required"}), 400
    try:
        s3.put_object_acl(Bucket=SPACES_BUCKET, Key=key, ACL="public-read")
        object_url = f"https://{SPACES_BUCKET}.{SPACES_REGION}.digitaloceanspaces.com/{key}"
        return jsonify({"objectUrl": object_url})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@application.post("/api/premium/request-reset")
def premium_request_reset():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()

    if not email:
        return jsonify({"error": "Email is required"}), 400

    person = people_collection.find_one({"premium.login": email})
    if not person:
        # Return an error if the user does not exist
        return jsonify({
            "ok": False,
            "error": "Користувача з такою електронною поштою не знайдено."
        }), 404

    # Generate 6-digit code
    code = f"{secrets.randbelow(1_000_000):06d}"
    expires = datetime.utcnow() + timedelta(minutes=15)

    # Update DB with reset code and expiry
    people_collection.update_one(
        {"_id": person["_id"]},
        {"$set": {"premium.reset": {"code": code, "expiresAt": expires, "attempts": 0}}}
    )

    # Send email with code
    send_mail(
        email,
        "Код для відновлення пароля",
        f"""
        <p>Ваш код для відновлення пароля:</p>
        <p style="font-size:20px;font-weight:700;letter-spacing:3px">{code}</p>
        <p>Діє протягом 15 хвилин.</p>
        """
    )

    return jsonify({"ok": True})


@application.post("/api/premium/reset")
def premium_reset():
    data = request.get_json(silent=True) or {}
    person_id = (data.get("personId") or "").strip()
    email = (data.get("email") or "").strip().lower()
    code = (data.get("code") or "").strip()
    new_pw = (data.get("newPassword") or "")

    validate_new_password(new_pw)

    if person_id:
        if not code:
            abort(400, description="Введіть код")
        try:
            oid = ObjectId(person_id)
        except Exception:
            abort(400, description="Invalid person id")

        person = people_collection.find_one({'_id': oid})
        if not person:
            abort(404, description="Person not found")

        premium = person.get('premium') or {}
        phone = (premium.get('phone') or '').strip()
        if not phone:
            abort(400, description="Для цієї особи не вказано номер телефону")

        ok, payload, status_code = verify_sms_code_with_kyivstar(phone, code)
        if not ok:
            status = (payload or {}).get('status')
            if status == 'INVALID':
                abort(400, description="Невірний код")
            message = (payload or {}).get('details') or (payload or {}).get('error') or 'Не вдалося підтвердити код'
            abort(status_code or 400, description=message)

        people_collection.update_one(
            {'_id': person['_id']},
            {"$set": {
                "premium.password": hash_password(new_pw),
                "premium.updatedAt": datetime.utcnow()
             },
             "$unset": {"premium.reset": "", "premium.resetSms": ""}}
        )
        return jsonify({"ok": True})

    if not email or not code:
        abort(400, description="Потрібні email та код")

    p = people_collection.find_one({"premium.login": email})
    if not p:
        # Don't leak whether email exists
        abort(400, description="Невірний код або прострочено")

    reset = (p.get("premium") or {}).get("reset") or {}
    expires_at = reset.get("expiresAt")
    stored_code = (reset.get("code") or "").strip()
    attempts = int(reset.get("attempts") or 0)

    # Basic protections
    if attempts >= 5:
        abort(400, description="Перевищено кількість спроб. Запросіть новий код.")
    if not stored_code or not isinstance(expires_at, datetime) or expires_at < datetime.utcnow():
        abort(400, description="Невірний код або прострочено")

    # Constant-time compare
    ok = secrets.compare_digest(code, stored_code)
    if not ok:
        people_collection.update_one(
            {"_id": p["_id"]},
            {"$set": {"premium.reset.attempts": attempts + 1}}
        )
        abort(400, description="Невірний код")

    # Success → set new password (bcrypt) & clear reset
    people_collection.update_one(
        {"_id": p["_id"]},
        {"$set": {
            "premium.password": hash_password(new_pw),
            "premium.updatedAt": datetime.utcnow()
         },
         "$unset": {"premium.reset": ""}}
    )
    return jsonify({"ok": True})


@application.post("/api/people/login")
def people_login():
    """
    Authenticates a premium person by password (person scoped) or legacy login.
    Returns a JWT-like random token (stored in-memory or to be verified separately).
    """
    data = request.get_json(silent=True) or {}
    login = (data.get("login") or "").strip().lower()
    password = (data.get("password") or "").strip()
    person_id = (data.get("person_id") or "").strip()

    if not password:
        abort(400, description="Потрібно вказати пароль")

    person = None
    if person_id:
        try:
            oid = ObjectId(person_id)
        except Exception:
            abort(400, description="Invalid person id")
        person = people_collection.find_one({'_id': oid})
    elif login:
        person = people_collection.find_one({"premium.login": login})
    else:
        abort(400, description="Потрібно вказати person_id або логін")

    if not person:
        abort(401, description="Невірний логін або пароль")

    premium = person.get("premium", {})
    hashed_pw = premium.get("password")

    if not hashed_pw or not verify_password(password, hashed_pw):
        abort(401, description="Невірний логін або пароль")

    # Generate temporary token (you can later switch to JWT)
    token = secrets.token_urlsafe(32)
    expires = datetime.utcnow() + timedelta(days=1)

    people_collection.update_one(
        {"_id": person["_id"]},
        {"$set": {"premium.session": {"token": token, "expiresAt": expires}}}
    )

    return jsonify({
        "token": token,
        "personId": str(person["_id"]),
        "expiresAt": expires.isoformat() + "Z"
    }), 200


@socketio.on('joinRoom')
def handle_join(room):
    """Called by client/admin: socket.emit('joinRoom', chatId)"""
    join_room(room)


@socketio.on('joinAdmin')
def handle_join_admin():
    """Called by admin: socket.emit('joinAdmin')"""
    join_room(ADMINS_ROOM)


if __name__ == '__main__':
    socketio.run(application, host='0.0.0.0', port=5000)
