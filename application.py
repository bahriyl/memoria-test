import eventlet
eventlet.monkey_patch()

import time
import os
import re
import random
import secrets
import bcrypt
import boto3
import requests
import base64
import io
import zipfile
import tempfile
import shutil
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlparse
import jwt
import hashlib
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

from flask import Flask, request, jsonify, abort, make_response, send_file, after_this_request, redirect
from flask_cors import CORS
from pymongo import MongoClient, ASCENDING
from bson.objectid import ObjectId
from dotenv import load_dotenv
from flask_socketio import SocketIO, join_room
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

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
    reverse_geocode_location,
    search_location_addresses,
    search_location_areas,
)

load_dotenv()


try:
    from PIL import Image, ImageOps
except Exception:
    Image = None
    ImageOps = None

try:
    from pillow_heif import register_heif_opener
except Exception:
    register_heif_opener = None

if Image is not None and register_heif_opener is not None:
    try:
        register_heif_opener()
    except Exception:
        pass

try:
    import pypdfium2 as pdfium
except Exception:
    pdfium = None

try:
    import segno
except Exception:
    segno = None



JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-key")
JWT_ALGORITHM = "HS256"
JWT_EXP_DELTA_SECONDS = 43200
ADMIN_LOGIN = (os.environ.get("ADMIN_LOGIN") or "").strip()
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD") or ""
ADMIN_JWT_SECRET = os.environ.get("ADMIN_JWT_SECRET") or JWT_SECRET
ADMIN_JWT_EXP_DELTA_SECONDS = 43200

NP_API_KEY = os.getenv('NP_API_KEY')
NP_BASE_URL = 'https://api.novaposhta.ua/v2.0/json/'
NP_TRACKING_COOLDOWN_SECONDS = max(int(os.getenv('NP_TRACKING_COOLDOWN_SECONDS', '1800') or '1800'), 30)
NP_TRACKING_BATCH_LIMIT_DEFAULT = max(int(os.getenv('NP_TRACKING_BATCH_LIMIT_DEFAULT', '25') or '25'), 1)
NP_TRACKING_BATCH_LIMIT_MAX = max(int(os.getenv('NP_TRACKING_BATCH_LIMIT_MAX', '50') or '50'), NP_TRACKING_BATCH_LIMIT_DEFAULT)
NP_TRACKING_BATCH_CONCURRENCY = max(int(os.getenv('NP_TRACKING_BATCH_CONCURRENCY', '4') or '4'), 1)
FINANCE_FALLBACK_PLAQUE_UAH = max(int(os.getenv('FINANCE_FALLBACK_PLAQUE_UAH', '200') or '200'), 0)
FINANCE_FALLBACK_RITUAL_PAYMENT_UAH = max(int(os.getenv('FINANCE_FALLBACK_RITUAL_PAYMENT_UAH', '0') or '0'), 0)
FINANCE_NOTES_DEVELOPMENT_PERCENT = max(int(os.getenv('FINANCE_NOTES_DEVELOPMENT_PERCENT', '10') or '10'), 0)
DEFAULT_FINANCE_TIMEZONE = os.getenv('DEFAULT_FINANCE_TIMEZONE', 'Europe/Kyiv') or 'Europe/Kyiv'
PUBLIC_WEB_BASE_URL = (
    os.getenv('PUBLIC_WEB_BASE_URL')
    or os.getenv('FRONTEND_BASE_URL')
    or os.getenv('APP_BASE_URL')
    or ''
).strip()

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
premium_orders_collection = db['premium_orders']
plaques_moderation_collection = db['plaques_moderation']
plaques_orders_collection = db['plaques_orders']
admin_qr_batches_collection = db['admin_qr_batches']
admin_qr_codes_collection = db['admin_qr_codes']
chat_collection = db['chats']
message_collection = db['messages']
cemeteries_collection = db['cemeteries']
ads_columns_meta_collection = db['ads_columns_meta']
ads_campaigns_collection = db['ads_campaigns']
ads_applications_collection = db['ads_applications']
churches_collection = db['churches']
ritual_services_collection = db['ritual_services']
ritual_service_categories_collection = db['ritual_service_categories']
premium_qr_firmas_collection = db['premium_qr_firmas']
location_moderation_collection = db['location_moderation']
liturgies_collection = db['liturgies']
chat_templates_collection = db['chat_templates']
admin_note_payments_collection = db['admin_note_payments']

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

PHOTON_BASE_URL = os.environ.get("PHOTON_BASE_URL", "https://photon.komoot.io").strip()
NOMINATIM_BASE_URL = os.environ.get("NOMINATIM_BASE_URL", "https://nominatim.openstreetmap.org").strip()
GEOCODING_USER_AGENT = os.environ.get(
    "GEOCODING_USER_AGENT",
    "memoria-geocoder/1.0 (+https://memoria.com.ua)",
).strip()
try:
    GEOCODING_TIMEOUT_MS = max(int(os.environ.get("GEOCODING_TIMEOUT_MS", "5000").strip() or "5000"), 1000)
except Exception:
    GEOCODING_TIMEOUT_MS = 5000
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
    return bool(area_id and area_source in {"photon", "nominatim"} and geo)


def _location_has_manual_address_text(location):
    normalized = normalize_location_core(location if isinstance(location, dict) else {})
    address = normalized.get("address") if isinstance(normalized.get("address"), dict) else {}
    address_display = loc_clean_str(address.get("display") or address.get("raw"))
    return bool(address_display)


def _location_has_geonames_area_and_geo(location):
    # Compatibility alias used by legacy call sites.
    return _location_has_provider_area_and_geo(location)


def _validate_admin_strict_location(field_name, location):
    if not _location_has_provider_area_and_geo(location):
        abort(400, description=f"`{field_name}` must come from provider suggestions (`area.id` + coordinates required)")
    if not _location_has_manual_address_text(location):
        abort(400, description=f"`{field_name}.address` must include manual street text")

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

try:
    premium_qr_firmas_collection.create_index([("name", ASCENDING)])
except Exception:
    pass

try:
    premium_qr_firmas_collection.create_index([("updatedAt", DESCENDING), ("createdAt", DESCENDING)])
except Exception:
    pass

try:
    ads_columns_meta_collection.create_index([("cemeteryId", ASCENDING)], unique=True)
except Exception:
    pass

try:
    ads_campaigns_collection.create_index([("cemeteryId", ASCENDING), ("status", ASCENDING), ("updatedAt", -1)])
except Exception:
    pass

try:
    ads_campaigns_collection.create_index([("surfaceType", ASCENDING), ("status", ASCENDING), ("cemeteryId", ASCENDING), ("updatedAt", -1)])
except Exception:
    pass

try:
    ads_applications_collection.create_index([("status", ASCENDING), ("cemeteryId", ASCENDING), ("createdAt", -1)])
except Exception:
    pass

try:
    ads_applications_collection.create_index([("surfaceType", ASCENDING), ("status", ASCENDING), ("cemeteryId", ASCENDING), ("createdAt", -1)])
except Exception:
    pass

try:
    admin_qr_codes_collection.create_index([("qrNumber", ASCENDING)], unique=True)
except Exception:
    pass

try:
    admin_qr_codes_collection.create_index([("pathKey", ASCENDING), ("createdAt", -1)])
except Exception:
    pass

try:
    admin_qr_codes_collection.create_index([("batchId", ASCENDING)])
except Exception:
    pass

try:
    admin_qr_codes_collection.create_index([("qrToken", ASCENDING)], unique=True, sparse=True)
except Exception:
    pass

try:
    admin_qr_batches_collection.create_index([("pathKey", ASCENDING), ("createdAt", -1)])
except Exception:
    pass

try:
    plaques_moderation_collection.create_index([("createdAt", -1)])
except Exception:
    pass

try:
    plaques_moderation_collection.create_index([("status", ASCENDING), ("createdAt", -1)])
except Exception:
    pass

try:
    plaques_orders_collection.create_index([("createdAt", DESCENDING), ("_id", DESCENDING)])
except Exception:
    pass

try:
    plaques_orders_collection.create_index([("updatedAt", DESCENDING), ("_id", DESCENDING)])
except Exception:
    pass

try:
    plaques_orders_collection.create_index([("invoiceId", ASCENDING)], sparse=True)
except Exception:
    pass

try:
    plaques_orders_collection.create_index([("paymentStatus", ASCENDING), ("createdAt", DESCENDING)])
except Exception:
    pass

try:
    plaques_orders_collection.create_index([("moderationId", ASCENDING)])
except Exception:
    pass

try:
    plaques_orders_collection.create_index([("qrDocId", ASCENDING), ("wiredQrCodeId", ASCENDING)])
except Exception:
    pass

try:
    admin_note_payments_collection.create_index([("period.year", ASCENDING), ("period.month", ASCENDING)], unique=True)
except Exception:
    pass

try:
    admin_note_payments_collection.create_index([("status", ASCENDING), ("updatedAt", -1)])
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


def _password_reveal_key() -> bytes:
    key_source = (
        os.getenv('PREMIUM_PASSWORD_REVEAL_KEY')
        or os.getenv('ADMIN_JWT_SECRET')
        or os.getenv('JWT_SECRET')
        or 'memoria-password-reveal-default'
    )
    return hashlib.sha256(key_source.encode('utf-8')).digest()


def _password_reveal_encrypt(raw_password: str) -> dict:
    cleaned = _admin_auth_str(raw_password)
    if not cleaned:
        return {}
    key = _password_reveal_key()
    iv = os.urandom(12)
    encrypted = AESGCM(key).encrypt(iv, cleaned.encode('utf-8'), None)
    ciphertext = encrypted[:-16]
    tag = encrypted[-16:]
    return {
        'version': 'v1',
        'ciphertext': base64.b64encode(ciphertext).decode('utf-8'),
        'iv': base64.b64encode(iv).decode('utf-8'),
        'tag': base64.b64encode(tag).decode('utf-8'),
        'updatedAt': datetime.utcnow(),
    }


def _password_reveal_decrypt(payload) -> str:
    if not isinstance(payload, dict):
        return ''
    try:
        ciphertext = base64.b64decode(_admin_auth_str(payload.get('ciphertext')))
        iv = base64.b64decode(_admin_auth_str(payload.get('iv')))
        tag = base64.b64decode(_admin_auth_str(payload.get('tag')))
        if not ciphertext or not iv or not tag:
            return ''
        key = _password_reveal_key()
        decrypted = AESGCM(key).decrypt(iv, ciphertext + tag, None)
        return decrypted.decode('utf-8').strip()
    except Exception:
        return ''


def _premium_password_set_payload(raw_password: str, *, include_updated_at: bool = True) -> dict:
    cleaned = _admin_auth_str(raw_password)
    if not cleaned:
        return {}
    payload = {
        'premium.password': hash_password(cleaned),
        'premium.passwordReveal': _password_reveal_encrypt(cleaned),
    }
    if include_updated_at:
        payload['premium.updatedAt'] = datetime.utcnow()
    return payload


def _admin_auth_str(value):
    if value is None:
        return ""
    return str(value).strip()


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


def _admin_pages_str(value):
    if value is None:
        return ''
    return str(value).strip()


def _admin_pages_parse_year(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    raw = _admin_pages_str(value)
    if not raw or not raw.isdigit():
        return None
    return int(raw)


def _admin_pages_parse_life_range(value):
    raw = _admin_pages_str(value)
    if not raw:
        return None, None
    years = re.findall(r'\b(\d{4})\b', raw)
    if len(years) >= 2:
        return int(years[0]), int(years[1])
    if len(years) == 1:
        return int(years[0]), None
    return None, None


def _admin_pages_parse_iso_date(value):
    raw = _admin_pages_str(value)
    if not raw:
        return None
    try:
        return datetime.strptime(raw, '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        abort(400, description='birthDate/deathDate must be in YYYY-MM-DD format')


def _admin_pages_ua_date(iso_value):
    raw = _admin_pages_str(iso_value)
    if not raw:
        return ''
    try:
        dt = datetime.strptime(raw, '%Y-%m-%d')
    except ValueError:
        return ''
    months = {
        1: 'січня',
        2: 'лютого',
        3: 'березня',
        4: 'квітня',
        5: 'травня',
        6: 'червня',
        7: 'липня',
        8: 'серпня',
        9: 'вересня',
        10: 'жовтня',
        11: 'листопада',
        12: 'грудня',
    }
    return f'{dt.day} {months.get(dt.month, "")} {dt.year}'.strip()


def _admin_pages_has_premium(person):
    premium = person.get('premium')
    return isinstance(premium, dict) and bool(premium)


def _admin_pages_is_premium_firma(person):
    partner_ref = _admin_pages_str(person.get('premiumPartnerRitualServiceId')).lower()
    if partner_ref.startswith('firma:'):
        return True

    admin_page = person.get('adminPage') if isinstance(person.get('adminPage'), dict) else {}
    path_key = _admin_pages_str(admin_page.get('pathKey')).lower()
    path_label = _admin_pages_str(admin_page.get('pathLabel')).lower()
    return path_key == 'premium_qr_firma' or path_label == 'преміум qr | фірма'


def _admin_pages_has_plaques_tag(person, extra_ctx=None):
    admin_page = person.get('adminPage') if isinstance(person.get('adminPage'), dict) else {}
    path_key = _admin_pages_str(admin_page.get('pathKey')).lower()
    path_label = _admin_pages_str(admin_page.get('pathLabel')).lower()
    if path_key == 'plaques' or path_label == 'табличка':
        return True

    if isinstance(extra_ctx, dict):
        plaques_person_ids = extra_ctx.get('plaquesPersonIds')
        if isinstance(plaques_person_ids, set):
            person_id = str(person.get('_id') or '')
            if person_id and person_id in plaques_person_ids:
                return True
    return False


def _admin_pages_status_tags(person, extra_ctx=None):
    tags = []
    if _admin_pages_is_premium_firma(person):
        tags.append('Преміум QR | Фірма')
    elif _admin_pages_has_premium(person):
        tags.append('Преміум QR')
    if bool(person.get('notable')):
        tags.append('видатна особа')
    if _admin_pages_has_plaques_tag(person, extra_ctx=extra_ctx):
        tags.append('табличка')

    if not tags:
        tags.append('Стандарт')
    return tags


def _admin_pages_infer_path_key(admin_page, status_tags):
    admin_page = admin_page if isinstance(admin_page, dict) else {}
    path_key = _admin_pages_str(admin_page.get('pathKey')).lower()
    if path_key in {'premium_qr', 'premium_qr_firma', 'plaques'}:
        return path_key

    path_label = _admin_pages_str(admin_page.get('pathLabel')).lower()
    if 'фірма' in path_label:
        return 'premium_qr_firma'
    if 'таблич' in path_label:
        return 'plaques'
    if 'преміум qr' in path_label:
        return 'premium_qr'

    for tag in status_tags if isinstance(status_tags, list) else []:
        lowered = _admin_pages_str(tag).lower()
        if 'фірма' in lowered:
            return 'premium_qr_firma'
        if 'таблич' in lowered:
            return 'plaques'
        if 'преміум qr' in lowered:
            return 'premium_qr'

    return ''


def _admin_pages_pick_fallback_qr(person_id, path_key, extra_ctx=None):
    if not isinstance(extra_ctx, dict):
        return ''
    qr_lookup = extra_ctx.get('wiredQrByPersonAndPath')
    if not isinstance(qr_lookup, dict):
        return ''
    lookup_key = f'{_admin_pages_str(person_id)}::{_admin_pages_str(path_key).lower()}'
    if not lookup_key:
        return ''
    return _admin_pages_str(qr_lookup.get(lookup_key))


def _admin_pages_build_burial_from_fields(area_id, area, cemetery, burial_site_coords='', burial_site_photo_urls=None):
    photos = burial_site_photo_urls if isinstance(burial_site_photo_urls, list) else []
    cleaned_photos = [_admin_pages_str(photo) for photo in photos if _admin_pages_str(photo)]
    burial = normalize_person_burial({
        'areaId': area_id,
        'area': area,
        'cemetery': cemetery,
        'location': [_admin_pages_str(burial_site_coords), '', cleaned_photos],
    })
    legacy = person_burial_to_legacy_fields(burial)
    return burial, legacy


def _admin_pages_projection(person, index, extra_ctx=None):
    item = _person_with_location_projection(person)
    admin_page = item.get('adminPage') if isinstance(item.get('adminPage'), dict) else {}
    burial = normalize_person_burial(item)
    location = burial.get('location') if isinstance(burial.get('location'), dict) else {}
    address = location.get('address') if isinstance(location.get('address'), dict) else {}
    cemetery_ref = burial.get('cemeteryRef') if isinstance(burial.get('cemeteryRef'), dict) else {}

    cemetery_name = _admin_pages_str(cemetery_ref.get('name')) or _admin_pages_str(item.get('cemetery'))
    cemetery_address = _admin_pages_str(address.get('display')) or _admin_pages_str(item.get('area'))
    birth_year = _admin_pages_parse_year(item.get('birthYear'))
    death_year = _admin_pages_parse_year(item.get('deathYear'))
    birth_date = _admin_pages_str(item.get('birthDate'))
    death_date = _admin_pages_str(item.get('deathDate'))
    years_label = ''
    if birth_year and death_year:
        years_label = f'{birth_year}-{death_year}'
    elif birth_year:
        years_label = f'{birth_year}-'
    elif death_year:
        years_label = f'-{death_year}'

    birth_date_ua = _admin_pages_ua_date(birth_date)
    death_date_ua = _admin_pages_ua_date(death_date)
    if birth_date_ua and death_date_ua:
        life_range_text = f'{birth_date_ua} - {death_date_ua}'
    else:
        life_range_text = years_label

    status_tags = _admin_pages_status_tags(item, extra_ctx=extra_ctx)
    path_key = _admin_pages_infer_path_key(admin_page, status_tags)
    qr_code = _admin_pages_str(admin_page.get('qrCode'))
    wired_qr_code_id = _admin_pages_str(admin_page.get('wiredQrCodeId')) or qr_code
    if not wired_qr_code_id:
        fallback_qr = _admin_pages_pick_fallback_qr(item.get('_id'), path_key, extra_ctx=extra_ctx)
        if fallback_qr:
            wired_qr_code_id = fallback_qr
    if not qr_code and wired_qr_code_id:
        qr_code = wired_qr_code_id
    phone = _admin_pages_str(admin_page.get('phone')) or _admin_pages_str(item.get('phone'))
    customer_phone = (
        _admin_pages_str(admin_page.get('customerPhone'))
        or _admin_pages_str(item.get('customerPhone'))
        or phone
    )
    customer_name = (
        _admin_pages_str(admin_page.get('customerName'))
        or _admin_pages_str(item.get('customerName'))
    )
    burial_site_coords = _admin_pages_str(item.get('burialSiteCoords'))
    location_photos = location.get('photos') if isinstance(location, dict) else []
    if not isinstance(location_photos, list):
        location_photos = []
    burial_photo_urls = [_admin_pages_str(photo) for photo in location_photos if _admin_pages_str(photo)]
    if not burial_photo_urls:
        legacy_urls = item.get('burialSitePhotoUrls')
        if isinstance(legacy_urls, list):
            burial_photo_urls = [_admin_pages_str(photo) for photo in legacy_urls if _admin_pages_str(photo)]
    lead_photo = _admin_pages_str(item.get('burialSitePhotoUrl'))
    if lead_photo and lead_photo not in burial_photo_urls:
        burial_photo_urls.insert(0, lead_photo)
    elif not lead_photo and burial_photo_urls:
        lead_photo = burial_photo_urls[0]
    internet_links = _admin_pages_str(item.get('sourceLink') or item.get('link') or item.get('internetLinks'))
    achievements = _admin_pages_str(item.get('achievements'))
    bio = _admin_pages_str(item.get('bio'))
    notable = bool(item.get('notable')) or bool(internet_links) or bool(achievements) or bool(bio)

    coords_from_location_geo = loc_clean_str(
        ((location.get('geo') or {}).get('coords'))
        if isinstance(location.get('geo'), dict)
        else ''
    )
    if not coords_from_location_geo:
        coordinates = (location.get('geo') or {}).get('coordinates') if isinstance(location.get('geo'), dict) else None
        if isinstance(coordinates, list) and len(coordinates) == 2:
            try:
                coords_from_location_geo = f'{float(coordinates[1]):.6f}, {float(coordinates[0]):.6f}'
            except Exception:
                coords_from_location_geo = ''

    return {
        'id': str(item.get('_id')),
        'index': int(index),
        'name': _admin_pages_str(item.get('name')),
        'birthYear': birth_year,
        'deathYear': death_year,
        'birthDate': birth_date,
        'deathDate': death_date,
        'yearsLabel': years_label,
        'avatarUrl': _admin_pages_str(item.get('avatarUrl') or item.get('portraitUrl')),
        'cemeteryName': cemetery_name,
        'cemeteryAddress': cemetery_address,
        'statusTags': status_tags,
        'statusLabel': ', '.join(status_tags),
        'pathKey': path_key,
        'pathLabel': _admin_pages_str(admin_page.get('pathLabel')),
        'qrCode': qr_code or None,
        'qrNumber': wired_qr_code_id or None,
        'wiredQrCodeId': wired_qr_code_id or None,
        'qrLabel': qr_code,
        'area': _admin_pages_str(item.get('area')),
        'areaId': _admin_pages_str(item.get('areaId')),
        'cemetery': _admin_pages_str(item.get('cemetery')),
        'phone': phone,
        'customerName': customer_name,
        'customerPhone': customer_phone,
        'companyName': _admin_pages_str(admin_page.get('companyName')) or _admin_pages_str(item.get('companyName')),
        'notable': notable,
        'internetLinks': internet_links,
        'achievements': achievements,
        'sourceLink': internet_links,
        'link': internet_links,
        'bio': bio,
        'burialSiteCoords': burial_site_coords,
        'burialSitePhotoUrl': lead_photo,
        'burialSitePhotoUrls': burial_photo_urls,
        'lifeRangeText': life_range_text,
    }


@application.route('/api/admin/pages', methods=['GET'])
def admin_list_pages():
    search_query = _admin_pages_str(request.args.get('search'))
    query_filter = {}
    if search_query:
        pattern = {'$regex': re.escape(search_query), '$options': 'i'}
        query_filter = {
            '$or': [
                {'name': pattern},
                {'cemetery': pattern},
                {'burial.cemeteryRef.name': pattern},
                {'area': pattern},
                {'adminPage.phone': pattern},
                {'adminPage.customerName': pattern},
                {'adminPage.customerPhone': pattern},
                {'customerName': pattern},
                {'customerPhone': pattern},
                {'adminPage.companyName': pattern},
                {'adminPage.qrCode': pattern},
                {'adminPage.wiredQrCodeId': pattern},
                {'sourceLink': pattern},
                {'link': pattern},
                {'bio': pattern},
            ]
        }

    docs = list(people_collection.find(query_filter).sort([('createdAt', -1), ('_id', -1)]))

    person_ids = [str(doc.get('_id') or '') for doc in docs if doc.get('_id')]
    plaques_person_ids = set()
    wired_qr_by_person_and_path = {}
    if person_ids:
        wired_qr_cursor = admin_qr_codes_collection.find(
            {
                'wiredPersonId': {'$in': person_ids},
                'pathKey': {'$in': ['premium_qr', 'premium_qr_firma', 'plaques']},
            },
            {
                'wiredPersonId': 1,
                'pathKey': 1,
                'wiredQrCodeId': 1,
                'qrNumber': 1,
            },
        ).sort([('createdAt', -1), ('_id', -1)])
        for qr_doc in wired_qr_cursor:
            raw_person_id = qr_doc.get('wiredPersonId')
            if isinstance(raw_person_id, ObjectId):
                person_id = str(raw_person_id)
            else:
                person_id = _admin_pages_str(raw_person_id)
            path_key = _admin_pages_str(qr_doc.get('pathKey')).lower()
            wired_qr_code_id = _admin_pages_str(qr_doc.get('wiredQrCodeId')) or _admin_pages_str(qr_doc.get('qrNumber'))
            if not person_id or not path_key or not wired_qr_code_id:
                continue
            lookup_key = f'{person_id}::{path_key}'
            if lookup_key not in wired_qr_by_person_and_path:
                wired_qr_by_person_and_path[lookup_key] = wired_qr_code_id

        wired_cursor = admin_qr_codes_collection.find(
            {
                'pathKey': 'plaques',
                'wiredPersonId': {'$in': person_ids},
            },
            {'wiredPersonId': 1},
        )
        for qr_doc in wired_cursor:
            raw_person_id = qr_doc.get('wiredPersonId')
            if isinstance(raw_person_id, ObjectId):
                plaques_person_ids.add(str(raw_person_id))
                continue
            cleaned_person_id = _admin_pages_str(raw_person_id)
            if cleaned_person_id:
                plaques_person_ids.add(cleaned_person_id)

    extra_ctx = {
        'plaquesPersonIds': plaques_person_ids,
        'wiredQrByPersonAndPath': wired_qr_by_person_and_path,
    }
    pages = [_admin_pages_projection(person, index + 1, extra_ctx=extra_ctx) for index, person in enumerate(docs)]
    return jsonify({
        'total': len(pages),
        'pages': pages,
    })


@application.route('/api/admin/pages', methods=['POST'])
def admin_create_page():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        abort(400, description='Request must be a JSON object')

    name = _admin_pages_str(data.get('name'))
    if not name:
        abort(400, description='`name` is required')

    area = _admin_pages_str(data.get('area'))
    area_id = _admin_pages_str(data.get('areaId'))
    cemetery = _admin_pages_str(data.get('cemetery'))
    phone = _admin_pages_str(data.get('phone'))
    customer_name = _admin_pages_str(data.get('customerName'))
    customer_phone = _admin_pages_str(data.get('customerPhone'))
    company_name = _admin_pages_str(data.get('companyName'))
    generated_password = _admin_pages_str(data.get('generatedPassword'))
    qr_code = _admin_pages_str(data.get('qrCode'))
    wired_qr_code_id = _admin_pages_str(data.get('wiredQrCodeId'))
    path_key = _admin_pages_str(data.get('pathKey'))
    path_label = _admin_pages_str(data.get('pathLabel'))
    internet_links = _admin_pages_str(data.get('internetLinks') or data.get('sourceLink') or data.get('link'))
    achievements = _admin_pages_str(data.get('achievements'))
    bio = _admin_pages_str(data.get('bio'))
    burial_site_coords = _admin_pages_str(data.get('burialSiteCoords'))
    burial_site_photo_url = _admin_pages_str(data.get('burialSitePhotoUrl'))
    burial_site_photo_urls = data.get('burialSitePhotoUrls') if isinstance(data.get('burialSitePhotoUrls'), list) else []
    burial_site_photo_urls = [_admin_pages_str(url) for url in burial_site_photo_urls if _admin_pages_str(url)]
    if not burial_site_photo_urls and burial_site_photo_url:
        burial_site_photo_urls = [burial_site_photo_url]

    birth_year = _admin_pages_parse_year(data.get('birthYear'))
    death_year = _admin_pages_parse_year(data.get('deathYear'))
    birth_date = _admin_pages_parse_iso_date(data.get('birthDate'))
    death_date = _admin_pages_parse_iso_date(data.get('deathDate'))
    range_birth, range_death = _admin_pages_parse_life_range(data.get('lifeRangeText'))
    if birth_date:
        birth_year = int(birth_date[:4])
    elif birth_year is None and range_birth is not None:
        birth_year = range_birth
    if death_date:
        death_year = int(death_date[:4])
    elif death_year is None and range_death is not None:
        death_year = range_death

    burial, legacy = _admin_pages_build_burial_from_fields(
        area_id,
        area,
        cemetery,
        burial_site_coords=burial_site_coords,
        burial_site_photo_urls=burial_site_photo_urls,
    )

    notable = bool(data.get('notable')) or bool(internet_links) or bool(achievements) or bool(bio)

    admin_page_payload = {}
    if phone:
        admin_page_payload['phone'] = phone
    if customer_name:
        admin_page_payload['customerName'] = customer_name
    if customer_phone:
        admin_page_payload['customerPhone'] = customer_phone
    if company_name:
        admin_page_payload['companyName'] = company_name
    if generated_password:
        admin_page_payload['generatedPassword'] = generated_password
    if qr_code:
        admin_page_payload['qrCode'] = qr_code
    if wired_qr_code_id:
        admin_page_payload['wiredQrCodeId'] = wired_qr_code_id
    if path_key:
        admin_page_payload['pathKey'] = path_key
    if path_label:
        admin_page_payload['pathLabel'] = path_label
    admin_page_payload['updatedAt'] = datetime.utcnow()

    doc = {
        'name': name,
        'notable': notable,
        'adminPage': admin_page_payload,
        'createdAt': datetime.utcnow(),
        'updatedAt': datetime.utcnow(),
    }
    if any(key in data for key in ('internetLinks', 'sourceLink', 'link', 'achievements', 'bio', 'notable')):
        doc['sourceLink'] = internet_links
        doc['link'] = internet_links
        doc['internetLinks'] = internet_links
        doc['achievements'] = achievements
        doc['bio'] = bio

    if _location_write_is_canonical() or _location_write_is_dual():
        doc['burial'] = burial
    if _location_write_is_legacy() or _location_write_is_dual():
        doc['areaId'] = legacy['areaId']
        doc['area'] = legacy['area']
        doc['cemetery'] = legacy['cemetery']
        doc['location'] = legacy['location']

    if birth_year is not None:
        doc['birthYear'] = birth_year
    if birth_date:
        doc['birthDate'] = birth_date
    if death_year is not None:
        doc['deathYear'] = death_year
    if death_date:
        doc['deathDate'] = death_date
    if generated_password:
        premium_password_payload = _premium_password_set_payload(generated_password)
        doc['premium'] = {
            'password': premium_password_payload.get('premium.password'),
            'passwordReveal': premium_password_payload.get('premium.passwordReveal'),
            'updatedAt': datetime.utcnow(),
        }

    inserted = people_collection.insert_one(doc)
    created = people_collection.find_one({'_id': inserted.inserted_id})
    return jsonify(_admin_pages_projection(created, 1)), 201


@application.route('/api/admin/pages/<string:person_id>', methods=['PATCH'])
def admin_update_page(person_id):
    try:
        oid = ObjectId(person_id)
    except Exception:
        abort(400, description='Invalid person id')

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        abort(400, description='Request must be a JSON object')

    existing = people_collection.find_one({'_id': oid})
    if not existing:
        abort(404, description='Person not found')

    update_fields = {}
    if 'name' in data:
        name = _admin_pages_str(data.get('name'))
        if not name:
            abort(400, description='`name` cannot be empty')
        update_fields['name'] = name

    existing_admin_page = existing.get('adminPage') if isinstance(existing.get('adminPage'), dict) else {}
    effective_path_key = _admin_pages_str(data.get('pathKey') or existing_admin_page.get('pathKey')).lower()
    effective_path_label = _admin_pages_str(data.get('pathLabel') or existing_admin_page.get('pathLabel')).lower()
    is_premium_qr_type = (
        effective_path_key in {'premium_qr', 'premium_qr_firma'}
        or 'преміум qr' in effective_path_label
    )
    internet_links = _admin_pages_str(data.get('internetLinks') or data.get('sourceLink') or data.get('link'))
    achievements = _admin_pages_str(data.get('achievements'))
    bio = _admin_pages_str(data.get('bio'))
    if any(key in data for key in ('internetLinks', 'sourceLink', 'link', 'achievements', 'bio', 'notable')):
        notable = bool(data.get('notable')) if 'notable' in data else bool(existing.get('notable'))
        notable = bool(notable) or bool(internet_links) or bool(achievements) or bool(bio)
        update_fields['notable'] = notable
        update_fields['sourceLink'] = internet_links
        update_fields['link'] = internet_links
        update_fields['internetLinks'] = internet_links
        update_fields['achievements'] = achievements
        update_fields['bio'] = bio
    elif 'notable' in data:
        update_fields['notable'] = bool(data.get('notable'))

    birth_year = _admin_pages_parse_year(data.get('birthYear')) if 'birthYear' in data else None
    death_year = _admin_pages_parse_year(data.get('deathYear')) if 'deathYear' in data else None
    birth_date = _admin_pages_parse_iso_date(data.get('birthDate')) if 'birthDate' in data else None
    death_date = _admin_pages_parse_iso_date(data.get('deathDate')) if 'deathDate' in data else None
    if 'lifeRangeText' in data:
        range_birth, range_death = _admin_pages_parse_life_range(data.get('lifeRangeText'))
        if 'birthYear' not in data and 'birthDate' not in data:
            birth_year = range_birth
        if 'deathYear' not in data and 'deathDate' not in data:
            death_year = range_death
    if birth_date:
        birth_year = int(birth_date[:4])
    if death_date:
        death_year = int(death_date[:4])

    if 'birthDate' in data:
        if birth_date is None:
            update_fields['birthDate'] = None
        else:
            update_fields['birthDate'] = birth_date
    if 'deathDate' in data:
        if death_date is None:
            update_fields['deathDate'] = None
        else:
            update_fields['deathDate'] = death_date

    if 'birthYear' in data or 'lifeRangeText' in data or 'birthDate' in data:
        if birth_year is None:
            update_fields['birthYear'] = None
        else:
            update_fields['birthYear'] = birth_year
    if 'deathYear' in data or 'lifeRangeText' in data or 'deathDate' in data:
        if death_year is None:
            update_fields['deathYear'] = None
        else:
            update_fields['deathYear'] = death_year

    merged_admin_page = dict(existing.get('adminPage') or {})
    if 'phone' in data:
        phone = _admin_pages_str(data.get('phone'))
        if phone:
            merged_admin_page['phone'] = phone
        else:
            merged_admin_page.pop('phone', None)
    if 'customerName' in data:
        customer_name = _admin_pages_str(data.get('customerName'))
        if customer_name:
            merged_admin_page['customerName'] = customer_name
        else:
            merged_admin_page.pop('customerName', None)
    if 'customerPhone' in data:
        customer_phone = _admin_pages_str(data.get('customerPhone'))
        if customer_phone:
            merged_admin_page['customerPhone'] = customer_phone
        else:
            merged_admin_page.pop('customerPhone', None)
    if 'companyName' in data:
        company_name = _admin_pages_str(data.get('companyName'))
        if company_name:
            merged_admin_page['companyName'] = company_name
        else:
            merged_admin_page.pop('companyName', None)
    if 'generatedPassword' in data:
        generated_password = _admin_pages_str(data.get('generatedPassword'))
        if generated_password:
            update_fields.update(_premium_password_set_payload(generated_password))
            merged_admin_page['generatedPassword'] = generated_password
        else:
            merged_admin_page.pop('generatedPassword', None)
    if 'qrCode' in data:
        qr_code = _admin_pages_str(data.get('qrCode'))
        if qr_code:
            merged_admin_page['qrCode'] = qr_code
        else:
            merged_admin_page.pop('qrCode', None)
    if 'wiredQrCodeId' in data:
        wired_qr_code_id = _admin_pages_str(data.get('wiredQrCodeId'))
        if wired_qr_code_id:
            merged_admin_page['wiredQrCodeId'] = wired_qr_code_id
        else:
            merged_admin_page.pop('wiredQrCodeId', None)
    if 'pathKey' in data:
        path_key = _admin_pages_str(data.get('pathKey'))
        if path_key:
            merged_admin_page['pathKey'] = path_key
        else:
            merged_admin_page.pop('pathKey', None)
    if 'pathLabel' in data:
        path_label = _admin_pages_str(data.get('pathLabel'))
        if path_label:
            merged_admin_page['pathLabel'] = path_label
        else:
            merged_admin_page.pop('pathLabel', None)
    if any(key in data for key in ('phone', 'customerName', 'customerPhone', 'companyName', 'generatedPassword', 'qrCode', 'wiredQrCodeId', 'pathKey', 'pathLabel')):
        merged_admin_page['updatedAt'] = datetime.utcnow()
        update_fields['adminPage'] = merged_admin_page

    location_change_keys = {'area', 'areaId', 'cemetery', 'burialSiteCoords', 'burialSitePhotoUrl', 'burialSitePhotoUrls'}
    has_location_changes = any(key in data for key in location_change_keys)
    if has_location_changes:
        next_area_id = _admin_pages_str(data.get('areaId')) if 'areaId' in data else _admin_pages_str(existing.get('areaId'))
        next_area = _admin_pages_str(data.get('area')) if 'area' in data else _admin_pages_str(existing.get('area'))
        next_cemetery = _admin_pages_str(data.get('cemetery')) if 'cemetery' in data else _admin_pages_str(existing.get('cemetery'))
        next_burial_site_coords = (
            _admin_pages_str(data.get('burialSiteCoords'))
            if 'burialSiteCoords' in data
            else _admin_pages_str(existing.get('burialSiteCoords'))
        )
        next_burial_site_photo_url = (
            _admin_pages_str(data.get('burialSitePhotoUrl'))
            if 'burialSitePhotoUrl' in data
            else _admin_pages_str(existing.get('burialSitePhotoUrl'))
        )
        if 'burialSitePhotoUrls' in data and isinstance(data.get('burialSitePhotoUrls'), list):
            next_burial_site_photo_urls = [
                _admin_pages_str(url)
                for url in data.get('burialSitePhotoUrls')
                if _admin_pages_str(url)
            ]
        else:
            raw_existing_photo_urls = existing.get('burialSitePhotoUrls')
            next_burial_site_photo_urls = []
            if isinstance(raw_existing_photo_urls, list):
                next_burial_site_photo_urls = [_admin_pages_str(url) for url in raw_existing_photo_urls if _admin_pages_str(url)]
            if not next_burial_site_photo_urls:
                existing_burial = normalize_person_burial(existing)
                existing_location = existing_burial.get('location') if isinstance(existing_burial.get('location'), dict) else {}
                existing_location_photos = existing_location.get('photos') if isinstance(existing_location, dict) else []
                if isinstance(existing_location_photos, list):
                    next_burial_site_photo_urls = [
                        _admin_pages_str(url)
                        for url in existing_location_photos
                        if _admin_pages_str(url)
                    ]
        if not next_burial_site_photo_urls and next_burial_site_photo_url:
            next_burial_site_photo_urls = [next_burial_site_photo_url]
        burial, legacy = _admin_pages_build_burial_from_fields(
            next_area_id,
            next_area,
            next_cemetery,
            burial_site_coords=next_burial_site_coords,
            burial_site_photo_urls=next_burial_site_photo_urls,
        )

        if _location_write_is_canonical() or _location_write_is_dual():
            update_fields['burial'] = burial
        if _location_write_is_legacy() or _location_write_is_dual():
            update_fields['areaId'] = legacy['areaId']
            update_fields['area'] = legacy['area']
            update_fields['cemetery'] = legacy['cemetery']
            update_fields['location'] = legacy['location']

    if not update_fields:
        abort(400, description='No fields to update')

    update_fields['updatedAt'] = datetime.utcnow()
    people_collection.update_one({'_id': oid}, {'$set': update_fields})
    updated = people_collection.find_one({'_id': oid})
    return jsonify(_admin_pages_projection(updated, 1))


@application.route('/api/admin/pages/<string:person_id>/password', methods=['GET'])
def admin_get_page_password(person_id):
    try:
        oid = ObjectId(person_id)
    except Exception:
        abort(400, description='Invalid person id')

    person = people_collection.find_one(
        {'_id': oid},
        {'adminPage.generatedPassword': 1, 'premium.passwordReveal': 1, 'premium.password': 1},
    )
    if not person:
        abort(404, description='Person not found')

    admin_page = person.get('adminPage') if isinstance(person.get('adminPage'), dict) else {}
    generated_password = _admin_pages_str(admin_page.get('generatedPassword'))

    if not generated_password:
        premium = person.get('premium') if isinstance(person.get('premium'), dict) else {}
        generated_password = _password_reveal_decrypt(premium.get('passwordReveal'))

    if not generated_password:
        premium = person.get('premium') if isinstance(person.get('premium'), dict) else {}
        if _admin_pages_str(premium.get('password')):
            abort(404, description='Password cannot be recovered. Use reset.')
        abort(404, description='Password not found')

    return jsonify({
        'generatedPassword': generated_password,
        'password': generated_password,
    })


@application.route('/api/admin/pages/<string:person_id>/password/reset', methods=['POST'])
def admin_reset_page_password(person_id):
    try:
        oid = ObjectId(person_id)
    except Exception:
        abort(400, description='Invalid person id')

    person = people_collection.find_one({'_id': oid}, {'_id': 1})
    if not person:
        abort(404, description='Person not found')

    data = request.get_json(silent=True) or {}
    raw_password = _admin_pages_str(data.get('password') or data.get('newPassword'))
    generated_password = raw_password or _generate_premium_order_password()
    validate_new_password(generated_password)

    people_collection.update_one(
        {'_id': oid},
        {
            '$set': {
                **_premium_password_set_payload(generated_password),
                'adminPage.generatedPassword': generated_password,
                'adminPage.updatedAt': datetime.utcnow(),
            },
            '$unset': {
                'premium.reset': '',
                'premium.resetSms': '',
            },
        },
    )

    return jsonify({
        'ok': True,
        'generatedPassword': generated_password,
        'password': generated_password,
    })


@application.route('/api/admin/pages/<string:person_id>', methods=['DELETE'])
def admin_delete_page(person_id):
    try:
        oid = ObjectId(person_id)
    except Exception:
        abort(400, description='Invalid person id')

    result = people_collection.delete_one({'_id': oid})
    if result.deleted_count == 0:
        abort(404, description='Person not found')

    # Delete all QR rows wired to this person (across all paths), while leaving unwired inventory intact.
    qr_delete_query = {
        '$or': [
            {'wiredPersonId': person_id},
            {'wiredPersonId': oid},
        ]
    }
    qr_deleted = admin_qr_codes_collection.delete_many(qr_delete_query)
    application.logger.info(
        'admin_delete_page: deleted person_id=%s, person_docs=%s, wired_qr_docs=%s',
        person_id,
        result.deleted_count,
        qr_deleted.deleted_count,
    )

    return ('', 204)


def _admin_moderation_parse_iso_date(value):
    raw = loc_clean_str(value)
    if not raw:
        return ''
    try:
        return datetime.strptime(raw, '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        return ''


def _admin_moderation_parse_year(value):
    if value is None or value == '' or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    raw = loc_clean_str(value)
    if not raw or not raw.isdigit():
        return None
    return int(raw)


_CEMETERY_NAME_NOISE_TOKENS = {
    'кладовище',
    'кладовища',
    'цвинтар',
    'цвинтаря',
    'цвинтарі',
    'кладбише',
    'cemetery',
}

_AREA_NOISE_TOKENS = {
    'україна',
    'область',
    'обл',
    'район',
    'р',
    'місто',
    'м',
}


def _admin_norm_text(value):
    raw = loc_clean_str(value).lower()
    if not raw:
        return ''
    normalized = re.sub(r"[`'\"’ʼ]+", '', raw)
    normalized = re.sub(r'[\(\)\[\]\{\}\.,;:!?\-_/\\]+', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized


def _admin_norm_tokens(value, noise_tokens=None):
    tokens = [token for token in re.split(r'\W+', _admin_norm_text(value), flags=re.UNICODE) if token]
    if not noise_tokens:
        return tokens
    return [token for token in tokens if token not in noise_tokens]


def _admin_cemetery_name_matches(request_name, candidate_name):
    req_norm = _admin_norm_text(request_name)
    cand_norm = _admin_norm_text(candidate_name)
    if not req_norm or not cand_norm:
        return False
    if req_norm == cand_norm:
        return True

    req_tokens = _admin_norm_tokens(req_norm, _CEMETERY_NAME_NOISE_TOKENS)
    cand_tokens = _admin_norm_tokens(cand_norm, _CEMETERY_NAME_NOISE_TOKENS)
    if not req_tokens or not cand_tokens:
        return False

    req_compact = ''.join(req_tokens)
    cand_compact = ''.join(cand_tokens)
    if req_compact and cand_compact:
        if req_compact == cand_compact:
            return True
        if len(req_compact) >= 7 and req_compact in cand_compact:
            return True
        if len(cand_compact) >= 7 and cand_compact in req_compact:
            return True

    req_set = set(req_tokens)
    cand_set = set(cand_tokens)
    overlap = req_set & cand_set
    if not overlap:
        return False

    smaller_size = min(len(req_set), len(cand_set))
    if smaller_size <= 2:
        return len(overlap) >= 1
    return len(overlap) >= (smaller_size - 1)


def _admin_area_matches(request_area, candidate_area):
    req_norm = _admin_norm_text(request_area)
    if not req_norm:
        return True

    cand_norm = _admin_norm_text(candidate_area)
    if not cand_norm:
        return False

    if req_norm == cand_norm or req_norm in cand_norm or cand_norm in req_norm:
        return True

    req_tokens = set(_admin_norm_tokens(req_norm, _AREA_NOISE_TOKENS))
    cand_tokens = set(_admin_norm_tokens(cand_norm, _AREA_NOISE_TOKENS))
    if not req_tokens or not cand_tokens:
        return False

    overlap = req_tokens & cand_tokens
    if not overlap:
        return False

    if min(len(req_tokens), len(cand_tokens)) <= 2:
        return True
    return len(overlap) >= 2


def _admin_moderation_cemetery_exists(name, area=''):
    cleaned_name = loc_clean_str(name)
    if not cleaned_name:
        return False

    # Fast path: exact name match, case-insensitive.
    name_regex = {'$regex': f'^{re.escape(cleaned_name)}$', '$options': 'i'}
    candidates = list(cemeteries_collection.find({'name': name_regex}).limit(60))

    # Fallback path: tolerate minor punctuation/spacing/suffix variations.
    if not candidates:
        search_tokens = _admin_norm_tokens(cleaned_name, _CEMETERY_NAME_NOISE_TOKENS)
        loose_token = next((token for token in search_tokens if len(token) >= 4), '')
        if not loose_token and search_tokens:
            loose_token = search_tokens[0]
        if loose_token:
            loose_regex = {'$regex': re.escape(loose_token), '$options': 'i'}
            loose_candidates = list(cemeteries_collection.find({'name': loose_regex}).limit(120))
            candidates = [
                doc for doc in loose_candidates
                if _admin_cemetery_name_matches(cleaned_name, loc_clean_str(doc.get('name')))
            ]
        if not candidates:
            return False

    area_text = loc_clean_str(area)
    if not area_text:
        return True

    for doc in candidates:
        option = cemetery_option_from_doc(doc)
        candidate_name = loc_clean_str(option.get('name') or doc.get('name'))
        if not _admin_cemetery_name_matches(cleaned_name, candidate_name):
            continue
        area_candidate = loc_clean_str(option.get('area') or doc.get('locality'))
        if _admin_area_matches(area_text, area_candidate):
            return True
    return False


def _admin_moderation_projection(raw_doc):
    item = _person_with_location_projection(raw_doc)
    burial = normalize_person_burial(item)
    location = burial.get('location') if isinstance(burial.get('location'), dict) else {}
    address = location.get('address') if isinstance(location.get('address'), dict) else {}
    cemetery_ref = burial.get('cemeteryRef') if isinstance(burial.get('cemeteryRef'), dict) else {}

    birth_date = _admin_moderation_parse_iso_date(item.get('birthDate'))
    death_date = _admin_moderation_parse_iso_date(item.get('deathDate'))
    birth_year = _admin_moderation_parse_year(item.get('birthYear'))
    death_year = _admin_moderation_parse_year(item.get('deathYear'))
    if birth_year is None and birth_date:
        birth_year = int(birth_date[:4])
    if death_year is None and death_date:
        death_year = int(death_date[:4])

    burial_photo_urls = loc_clean_str_list(
        item.get('burialSitePhotoUrls') or ((burial.get('photos') if isinstance(burial, dict) else []) or [])
    )
    if not burial_photo_urls:
        single_photo = loc_clean_str(item.get('burialSitePhotoUrl'))
        if single_photo:
            burial_photo_urls = [single_photo]

    cemetery_name = loc_clean_str(cemetery_ref.get('name')) or loc_clean_str(item.get('cemetery'))
    area = loc_clean_str(item.get('area')) or loc_clean_str(address.get('display'))
    cemetery_exists = _admin_moderation_cemetery_exists(cemetery_name, area)

    link = loc_clean_str(item.get('sourceLink') or item.get('link') or item.get('internetLinks'))
    achievements = loc_clean_str(item.get('achievements'))
    bio = loc_clean_str(item.get('bio'))
    occupation = loc_clean_str(item.get('occupation'))
    notable = bool(item.get('notable')) or bool(link) or bool(achievements) or bool(bio)

    return {
        'id': str(item.get('_id')),
        'name': loc_clean_str(item.get('name')),
        'birthYear': birth_year,
        'deathYear': death_year,
        'birthDate': birth_date,
        'deathDate': death_date,
        'area': area,
        'areaId': loc_clean_str(item.get('areaId')),
        'cemetery': cemetery_name,
        'notable': notable,
        'phone': loc_clean_str(item.get('phone')),
        'internetLinks': link,
        'achievements': achievements,
        'sourceLink': link,
        'link': link,
        'bio': bio,
        'occupation': occupation,
        'burialSiteCoords': loc_clean_str(item.get('burialSiteCoords')) or coords_from_location_geo,
        'burialSitePhotoUrls': burial_photo_urls,
        'burialSitePhotoUrl': burial_photo_urls[0] if burial_photo_urls else '',
        'cemeteryExists': cemetery_exists,
        'createdAt': item.get('createdAt'),
        'updatedAt': item.get('updatedAt'),
    }


def _admin_build_person_from_moderation(moderation_doc, incoming):
    source = moderation_doc or {}
    payload = incoming if isinstance(incoming, dict) else {}

    name = loc_clean_str(payload.get('name') if 'name' in payload else source.get('name'))
    if not name:
        abort(400, description='`name` is required')

    birth_date = _admin_moderation_parse_iso_date(payload.get('birthDate') if 'birthDate' in payload else source.get('birthDate'))
    death_date = _admin_moderation_parse_iso_date(payload.get('deathDate') if 'deathDate' in payload else source.get('deathDate'))
    birth_year = _admin_moderation_parse_year(payload.get('birthYear') if 'birthYear' in payload else source.get('birthYear'))
    death_year = _admin_moderation_parse_year(payload.get('deathYear') if 'deathYear' in payload else source.get('deathYear'))
    if birth_year is None and birth_date:
        birth_year = int(birth_date[:4])
    if death_year is None and death_date:
        death_year = int(death_date[:4])

    area = loc_clean_str(payload.get('area') if 'area' in payload else source.get('area'))
    area_id = loc_clean_str(payload.get('areaId') if 'areaId' in payload else source.get('areaId'))
    cemetery = loc_clean_str(payload.get('cemetery') if 'cemetery' in payload else source.get('cemetery'))

    incoming_burial = payload.get('burial') if isinstance(payload.get('burial'), dict) else None
    source_burial = source.get('burial') if isinstance(source.get('burial'), dict) else None
    burial_source = incoming_burial if incoming_burial is not None else source_burial

    incoming_burial_coords = loc_clean_str(
        ((incoming_burial or {}).get('location') or {}).get('geo', {}).get('coords')
        if isinstance(((incoming_burial or {}).get('location') or {}).get('geo'), dict)
        else ''
    )
    source_burial_coords = loc_clean_str(
        ((source_burial or {}).get('location') or {}).get('geo', {}).get('coords')
        if isinstance(((source_burial or {}).get('location') or {}).get('geo'), dict)
        else ''
    )
    incoming_payload_coords = loc_clean_str(payload.get('burialSiteCoords')) if 'burialSiteCoords' in payload else ''
    source_payload_coords = loc_clean_str(source.get('burialSiteCoords'))
    burial_site_coords = incoming_payload_coords or incoming_burial_coords or source_payload_coords or source_burial_coords

    incoming_payload_photo_urls = (
        loc_clean_str_list(payload.get('burialSitePhotoUrls'))
        if 'burialSitePhotoUrls' in payload
        else []
    )
    incoming_single_photo = (
        loc_clean_str(payload.get('burialSitePhotoUrl'))
        if 'burialSitePhotoUrl' in payload
        else ''
    )
    source_photo_urls = loc_clean_str_list(source.get('burialSitePhotoUrls'))
    source_single_photo = loc_clean_str(source.get('burialSitePhotoUrl'))
    source_burial_photos = loc_clean_str_list(source_burial.get('photos') if isinstance(source_burial, dict) else [])

    if incoming_payload_photo_urls:
        burial_photo_urls = incoming_payload_photo_urls
    elif incoming_single_photo:
        burial_photo_urls = [incoming_single_photo]
    elif source_photo_urls:
        burial_photo_urls = source_photo_urls
    elif source_single_photo:
        burial_photo_urls = [source_single_photo]
    else:
        burial_photo_urls = source_burial_photos
    burial_site_photo_url = burial_photo_urls[0] if burial_photo_urls else ''

    burial = normalize_person_burial({
        'burial': burial_source,
        'area': area,
        'areaId': area_id,
        'cemetery': cemetery,
        'location': source.get('location'),
        'burialSiteCoords': burial_site_coords,
        'burialSitePhotoUrls': burial_photo_urls,
    })
    legacy = person_burial_to_legacy_fields(burial)

    internet_links = loc_clean_str(
        payload.get('internetLinks') if 'internetLinks' in payload
        else payload.get('sourceLink') if 'sourceLink' in payload
        else payload.get('link') if 'link' in payload
        else source.get('internetLinks') if 'internetLinks' in source
        else source.get('sourceLink') if 'sourceLink' in source
        else source.get('link')
    )
    achievements = loc_clean_str(
        payload.get('achievements') if 'achievements' in payload
        else source.get('achievements')
    )
    bio = loc_clean_str(
        payload.get('bio') if 'bio' in payload
        else source.get('bio')
    )
    occupation = loc_clean_str(payload.get('occupation') if 'occupation' in payload else source.get('occupation'))
    notable_flag = payload.get('notable') if 'notable' in payload else source.get('notable')
    notable = bool(notable_flag) or bool(internet_links) or bool(achievements) or bool(bio) or bool(occupation)

    phone = loc_clean_str(payload.get('phone') if 'phone' in payload else source.get('phone'))

    doc = {
        'name': name,
        'notable': notable,
        'achievements': achievements,
        'bio': bio,
        'sourceLink': internet_links,
        'burialSiteCoords': burial_site_coords,
        'burialSitePhotoUrls': burial_photo_urls,
        'burialSitePhotoUrl': burial_site_photo_url,
        'burialSitePhotoCount': len(burial_photo_urls),
        'createdAt': datetime.utcnow(),
        'updatedAt': datetime.utcnow(),
    }

    if _location_write_is_canonical() or _location_write_is_dual():
        doc['burial'] = burial
    if _location_write_is_legacy() or _location_write_is_dual():
        doc['areaId'] = legacy.get('areaId', '')
        doc['area'] = legacy.get('area', '')
        doc['cemetery'] = legacy.get('cemetery', '')
        doc['location'] = legacy.get('location', [])

    if birth_year is not None:
        doc['birthYear'] = birth_year
    if death_year is not None:
        doc['deathYear'] = death_year
    if birth_date:
        doc['birthDate'] = birth_date
    if death_date:
        doc['deathDate'] = death_date

    if internet_links:
        doc['sourceLink'] = internet_links
    if occupation:
        doc['occupation'] = occupation

    if phone:
        doc['adminPage'] = {
            'phone': phone,
            'updatedAt': datetime.utcnow(),
        }

    return doc


@application.route('/api/admin/pages/moderation/people', methods=['GET'])
def admin_list_person_moderation():
    search = loc_clean_str(request.args.get('search'))
    query = {}
    if search:
        pattern = {'$regex': re.escape(search), '$options': 'i'}
        query = {
            '$or': [
                {'name': pattern},
                {'cemetery': pattern},
                {'area': pattern},
                {'bio': pattern},
                {'sourceLink': pattern},
                {'link': pattern},
                {'achievements': pattern},
                {'internetLinks': pattern},
            ]
        }

    docs = list(people_moderation_collection.find(query).sort([('createdAt', -1), ('_id', -1)]))
    items = [_admin_moderation_projection(doc) for doc in docs]
    return jsonify({
        'total': len(items),
        'items': items,
    })


@application.route('/api/admin/pages/moderation/people/<string:moderation_id>/verify', methods=['POST'])
def admin_verify_person_moderation(moderation_id):
    try:
        oid = ObjectId(moderation_id)
    except Exception:
        abort(400, description='Invalid moderation id')

    moderation_doc = people_moderation_collection.find_one({'_id': oid})
    if not moderation_doc:
        abort(404, description='Moderation record not found')

    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        abort(400, description='Request must be a JSON object')

    cemetery_name = loc_clean_str(payload.get('cemetery') or moderation_doc.get('cemetery'))
    area = loc_clean_str(payload.get('area') or moderation_doc.get('area'))
    if not _admin_moderation_cemetery_exists(cemetery_name, area):
        abort(400, description='Активація недоступна: кладовище має статус "Не додано".')
    person_doc = _admin_build_person_from_moderation(moderation_doc, payload)

    created = people_collection.insert_one(person_doc)
    people_moderation_collection.delete_one({'_id': oid})

    return jsonify({
        'success': True,
        'personId': str(created.inserted_id),
    })


@application.route('/api/admin/pages/moderation/people/<string:moderation_id>/reject', methods=['POST'])
def admin_reject_person_moderation(moderation_id):
    try:
        oid = ObjectId(moderation_id)
    except Exception:
        abort(400, description='Invalid moderation id')

    deleted = people_moderation_collection.delete_one({'_id': oid})
    if deleted.deleted_count == 0:
        abort(404, description='Moderation record not found')

    return jsonify({'success': True})


def _admin_plaques_path_label(item):
    source_type = loc_clean_str(item.get('sourceType')).lower()
    activation_type = loc_clean_str(item.get('activationType')).lower()
    company_name = loc_clean_str(item.get('companyName'))
    website_url = loc_clean_str(item.get('websiteUrl'))
    if (
        'firma' in source_type
        or 'firma' in activation_type
        or bool(company_name)
        or bool(website_url)
    ):
        return 'Фірма'
    return 'Сайт'


def _admin_plaques_life_range_text(item):
    birth_date = _admin_moderation_parse_iso_date(item.get('birthDate'))
    death_date = _admin_moderation_parse_iso_date(item.get('deathDate'))
    birth_year = _admin_moderation_parse_year(item.get('birthYear'))
    death_year = _admin_moderation_parse_year(item.get('deathYear'))
    if birth_year is None and birth_date:
        birth_year = int(birth_date[:4])
    if death_year is None and death_date:
        death_year = int(death_date[:4])

    birth_date_ua = _admin_pages_ua_date(birth_date)
    death_date_ua = _admin_pages_ua_date(death_date)
    if birth_date_ua and death_date_ua:
        return f'{birth_date_ua} - {death_date_ua}'

    if birth_year and death_year:
        return f'{birth_year}-{death_year}'
    if birth_year:
        return f'{birth_year}-'
    if death_year:
        return f'-{death_year}'
    return ''


def _admin_plaques_projection(raw_doc):
    item = raw_doc if isinstance(raw_doc, dict) else {}
    burial = normalize_person_burial(item)
    location = burial.get('location') if isinstance(burial.get('location'), dict) else {}
    address = location.get('address') if isinstance(location.get('address'), dict) else {}
    cemetery_ref = burial.get('cemeteryRef') if isinstance(burial.get('cemeteryRef'), dict) else {}
    burial_photo_urls = loc_clean_str_list(
        item.get('burialSitePhotoUrls') or ((burial.get('photos') if isinstance(burial, dict) else []) or [])
    )
    if not burial_photo_urls:
        single_photo = loc_clean_str(item.get('burialSitePhotoUrl'))
        if single_photo:
            burial_photo_urls = [single_photo]

    cemetery_name = loc_clean_str(cemetery_ref.get('name')) or loc_clean_str(item.get('cemetery'))
    area = loc_clean_str(item.get('area')) or loc_clean_str(address.get('display'))
    cemetery_exists = _admin_moderation_cemetery_exists(cemetery_name, area)
    link = loc_clean_str(item.get('sourceLink') or item.get('link') or item.get('internetLinks'))
    achievements = loc_clean_str(item.get('bio') or item.get('achievements'))
    occupation = loc_clean_str(item.get('occupation'))
    notable = bool(item.get('notable')) or bool(link) or bool(achievements) or bool(occupation)
    birth_date = _admin_moderation_parse_iso_date(item.get('birthDate'))
    death_date = _admin_moderation_parse_iso_date(item.get('deathDate'))

    return {
        'id': str(item.get('_id')),
        'name': loc_clean_str(item.get('name')),
        'birthDate': birth_date,
        'deathDate': death_date,
        'lifeRangeText': _admin_plaques_life_range_text(item),
        'area': area,
        'areaId': loc_clean_str(item.get('areaId')),
        'cemetery': cemetery_name,
        'cemeteryExists': cemetery_exists,
        'cemeteryStatusLabel': 'Додане' if cemetery_exists else 'Не додане',
        'notable': notable,
        'notableLabel': 'Так' if notable else 'Ні',
        'phone': loc_clean_str(item.get('phone')),
        'internetLinks': link,
        'achievements': achievements,
        'sourceLink': link,
        'link': link,
        'bio': achievements,
        'occupation': occupation,
        'burialSiteCoords': loc_clean_str(item.get('burialSiteCoords')),
        'burialSitePhotoUrls': burial_photo_urls,
        'burialSitePhotoUrl': burial_photo_urls[0] if burial_photo_urls else '',
        'qrCode': loc_clean_str(item.get('qrCode')),
        'qrDocId': loc_clean_str(item.get('qrDocId')),
        'qrNumber': loc_clean_str(item.get('qrNumber')),
        'wiredQrCodeId': loc_clean_str(item.get('wiredQrCodeId')),
        'pathLabel': _admin_plaques_path_label(item),
        'sourceType': loc_clean_str(item.get('sourceType')),
        'activationType': loc_clean_str(item.get('activationType')),
        'createdAt': item.get('createdAt'),
        'updatedAt': item.get('updatedAt'),
    }


def _admin_plaques_build_updated_document(current_doc, payload):
    source = current_doc if isinstance(current_doc, dict) else {}
    incoming = payload if isinstance(payload, dict) else {}

    name = loc_clean_str(incoming.get('name') if 'name' in incoming else source.get('name'))
    if not name:
        abort(400, description='`name` is required')

    birth_date = _admin_moderation_parse_iso_date(
        incoming.get('birthDate') if 'birthDate' in incoming else source.get('birthDate')
    )
    death_date = _admin_moderation_parse_iso_date(
        incoming.get('deathDate') if 'deathDate' in incoming else source.get('deathDate')
    )
    birth_year = _admin_moderation_parse_year(
        incoming.get('birthYear') if 'birthYear' in incoming else source.get('birthYear')
    )
    death_year = _admin_moderation_parse_year(
        incoming.get('deathYear') if 'deathYear' in incoming else source.get('deathYear')
    )
    if birth_year is None and birth_date:
        birth_year = int(birth_date[:4])
    if death_year is None and death_date:
        death_year = int(death_date[:4])

    area = loc_clean_str(incoming.get('area') if 'area' in incoming else source.get('area'))
    area_id = loc_clean_str(incoming.get('areaId') if 'areaId' in incoming else source.get('areaId'))
    cemetery = loc_clean_str(incoming.get('cemetery') if 'cemetery' in incoming else source.get('cemetery'))

    incoming_burial = incoming.get('burial') if isinstance(incoming.get('burial'), dict) else None
    source_burial = source.get('burial') if isinstance(source.get('burial'), dict) else None
    burial_source = incoming_burial if incoming_burial is not None else source_burial

    burial_site_coords = loc_clean_str(
        incoming.get('burialSiteCoords') if 'burialSiteCoords' in incoming else source.get('burialSiteCoords')
    )
    burial_photo_urls = loc_clean_str_list(
        incoming.get('burialSitePhotoUrls')
        if 'burialSitePhotoUrls' in incoming
        else source.get('burialSitePhotoUrls') or (source_burial.get('photos') if isinstance(source_burial, dict) else [])
    )
    if not burial_photo_urls:
        one_photo = loc_clean_str(
            incoming.get('burialSitePhotoUrl')
            if 'burialSitePhotoUrl' in incoming
            else source.get('burialSitePhotoUrl')
        )
        if one_photo:
            burial_photo_urls = [one_photo]

    burial = normalize_person_burial({
        'burial': burial_source,
        'area': area,
        'areaId': area_id,
        'cemetery': cemetery,
        'location': source.get('location'),
        'burialSiteCoords': burial_site_coords,
        'burialSitePhotoUrls': burial_photo_urls,
    })
    legacy = person_burial_to_legacy_fields(burial)

    internet_links = loc_clean_str(
        incoming.get('internetLinks')
        if 'internetLinks' in incoming
        else incoming.get('sourceLink')
        if 'sourceLink' in incoming
        else incoming.get('link')
        if 'link' in incoming
        else source.get('internetLinks')
        if 'internetLinks' in source
        else source.get('sourceLink')
        if 'sourceLink' in source
        else source.get('link')
    )
    achievements = loc_clean_str(
        incoming.get('achievements')
        if 'achievements' in incoming
        else incoming.get('bio')
        if 'bio' in incoming
        else source.get('achievements')
        if 'achievements' in source
        else source.get('bio')
    )
    occupation = loc_clean_str(incoming.get('occupation') if 'occupation' in incoming else source.get('occupation'))
    notable_flag = incoming.get('notable') if 'notable' in incoming else source.get('notable')
    notable = bool(notable_flag) or bool(internet_links) or bool(achievements) or bool(occupation)
    phone = loc_clean_str(incoming.get('phone') if 'phone' in incoming else source.get('phone'))

    set_fields = {
        'name': name,
        'notable': notable,
        'bio': achievements,
        'sourceLink': internet_links,
        'link': internet_links,
        'internetLinks': internet_links,
        'achievements': achievements,
        'occupation': occupation,
        'phone': phone,
        'burialSiteCoords': burial_site_coords,
        'burialSitePhotoUrls': burial_photo_urls,
        'burialSitePhotoUrl': burial_photo_urls[0] if burial_photo_urls else '',
        'burialSitePhotoCount': len(burial_photo_urls),
        'updatedAt': datetime.utcnow(),
    }

    if _location_write_is_canonical() or _location_write_is_dual():
        set_fields['burial'] = burial
    if _location_write_is_legacy() or _location_write_is_dual():
        set_fields['areaId'] = legacy.get('areaId', '')
        set_fields['area'] = legacy.get('area', '')
        set_fields['cemetery'] = legacy.get('cemetery', '')
        set_fields['location'] = legacy.get('location', [])

    unset_fields = {}
    if birth_year is not None:
        set_fields['birthYear'] = birth_year
    else:
        unset_fields['birthYear'] = ''
    if death_year is not None:
        set_fields['deathYear'] = death_year
    else:
        unset_fields['deathYear'] = ''
    if birth_date:
        set_fields['birthDate'] = birth_date
    else:
        unset_fields['birthDate'] = ''
    if death_date:
        set_fields['deathDate'] = death_date
    else:
        unset_fields['deathDate'] = ''

    return set_fields, unset_fields


def _admin_plaques_pick_qr_for_activation(moderation_doc):
    doc = moderation_doc if isinstance(moderation_doc, dict) else {}
    qr_doc = None
    raw_qr_doc_id = loc_clean_str(doc.get('qrDocId'))
    if raw_qr_doc_id:
        try:
            qr_doc = admin_qr_codes_collection.find_one({'_id': ObjectId(raw_qr_doc_id)})
        except Exception:
            qr_doc = None

    if not qr_doc:
        qr_token = loc_clean_str(doc.get('qrCode'))
        if qr_token:
            qr_doc = admin_qr_codes_collection.find_one({'qrToken': qr_token})

    if not qr_doc:
        wired_qr_code_id = loc_clean_str(doc.get('wiredQrCodeId')) or loc_clean_str(doc.get('qrNumber'))
        if wired_qr_code_id:
            qr_doc = admin_qr_codes_collection.find_one(
                {'pathKey': 'plaques', 'wiredQrCodeId': wired_qr_code_id},
                sort=[('createdAt', ASCENDING), ('_id', ASCENDING)],
            )

    if not qr_doc:
        abort(409, description='No QR code available for activation')

    qr_doc = _qr_ensure_doc_runtime_fields(qr_doc, persist=True)
    if loc_clean_str(qr_doc.get('pathKey')) != 'plaques':
        abort(409, description='QR path does not match plaques flow')
    if bool(qr_doc.get('isConnected') and loc_clean_str(qr_doc.get('wiredPersonId'))):
        abort(409, description='QR code is already activated')

    return qr_doc


@application.route('/api/admin/pages/moderation/plaques', methods=['GET'])
def admin_list_plaques_moderation():
    search = loc_clean_str(request.args.get('search'))
    query = {}
    if search:
        pattern = {'$regex': re.escape(search), '$options': 'i'}
        query = {
            '$or': [
                {'name': pattern},
                {'cemetery': pattern},
                {'area': pattern},
                {'phone': pattern},
                {'sourceLink': pattern},
                {'link': pattern},
                {'bio': pattern},
                {'internetLinks': pattern},
                {'achievements': pattern},
                {'companyName': pattern},
                {'websiteUrl': pattern},
            ]
        }

    docs = list(plaques_moderation_collection.find(query).sort([('createdAt', -1), ('_id', -1)]))
    items = [_admin_plaques_projection(doc) for doc in docs]
    return jsonify({
        'total': len(items),
        'items': items,
    })


@application.route('/api/admin/pages/moderation/plaques/<string:moderation_id>', methods=['GET'])
def admin_get_plaques_moderation(moderation_id):
    try:
        oid = ObjectId(moderation_id)
    except Exception:
        abort(400, description='Invalid moderation id')

    doc = plaques_moderation_collection.find_one({'_id': oid})
    if not doc:
        abort(404, description='Moderation record not found')
    return jsonify(_admin_plaques_projection(doc))


@application.route('/api/admin/pages/moderation/plaques/<string:moderation_id>', methods=['PATCH'])
def admin_update_plaques_moderation(moderation_id):
    try:
        oid = ObjectId(moderation_id)
    except Exception:
        abort(400, description='Invalid moderation id')

    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        abort(400, description='Request must be a JSON object')

    current_doc = plaques_moderation_collection.find_one({'_id': oid})
    if not current_doc:
        abort(404, description='Moderation record not found')

    set_fields, unset_fields = _admin_plaques_build_updated_document(current_doc, data)
    update_query = {'$set': set_fields}
    if unset_fields:
        update_query['$unset'] = unset_fields
    plaques_moderation_collection.update_one({'_id': oid}, update_query)

    updated = plaques_moderation_collection.find_one({'_id': oid}) or {}
    return jsonify(_admin_plaques_projection(updated))


@application.route('/api/admin/pages/moderation/plaques/<string:moderation_id>/reject', methods=['POST'])
def admin_reject_plaques_moderation(moderation_id):
    try:
        oid = ObjectId(moderation_id)
    except Exception:
        abort(400, description='Invalid moderation id')

    deleted = plaques_moderation_collection.delete_one({'_id': oid})
    if deleted.deleted_count == 0:
        abort(404, description='Moderation record not found')
    return jsonify({'success': True})


@application.route('/api/admin/pages/moderation/plaques/<string:moderation_id>/activate', methods=['POST'])
def admin_activate_plaques_moderation(moderation_id):
    try:
        oid = ObjectId(moderation_id)
    except Exception:
        abort(400, description='Invalid moderation id')

    moderation_doc = plaques_moderation_collection.find_one({'_id': oid})
    if not moderation_doc:
        abort(404, description='Moderation record not found')

    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        abort(400, description='Request must be a JSON object')

    cemetery_name = loc_clean_str(payload.get('cemetery') or moderation_doc.get('cemetery'))
    area = loc_clean_str(payload.get('area') or moderation_doc.get('area'))
    if not _admin_moderation_cemetery_exists(cemetery_name, area):
        abort(400, description='Активація недоступна: кладовище має статус "Не додано".')

    person_doc = _admin_build_person_from_moderation(moderation_doc, payload)
    qr_doc = _admin_plaques_pick_qr_for_activation(moderation_doc)
    qr_doc_id = qr_doc.get('_id')
    wired_qr_code_id = _qr_str(qr_doc.get('wiredQrCodeId')) or _qr_str(qr_doc.get('qrNumber'))
    if not wired_qr_code_id:
        abort(409, description='QR code cannot be activated')

    admin_page_payload = person_doc.get('adminPage') if isinstance(person_doc.get('adminPage'), dict) else {}
    admin_page_payload['qrCode'] = wired_qr_code_id
    admin_page_payload['pathKey'] = 'plaques'
    admin_page_payload['pathLabel'] = 'табличка'
    admin_page_payload['updatedAt'] = datetime.utcnow()
    person_doc['adminPage'] = admin_page_payload

    created = people_collection.insert_one(person_doc)
    person_id = str(created.inserted_id)
    now = datetime.utcnow()

    if qr_doc_id:
        admin_qr_codes_collection.update_one(
            {'_id': qr_doc_id},
            {
                '$set': {
                    'status': 'connected',
                    'isConnected': True,
                    'wiredPersonId': person_id,
                    'wiredQrCodeId': wired_qr_code_id,
                    'wiredAt': now,
                    'updatedAt': now,
                }
            }
        )

    plaques_orders_collection.update_one(
        {'moderationId': str(oid)},
        {
            '$set': {
                'personId': person_id,
                'personIds': [person_id],
                'personName': _qr_str(person_doc.get('name')),
                'personNames': [_qr_str(person_doc.get('name'))] if _qr_str(person_doc.get('name')) else [],
                'qrDocId': str(qr_doc_id) if qr_doc_id else '',
                'qrNumber': _qr_str(qr_doc.get('qrNumber')),
                'wiredQrCodeId': wired_qr_code_id,
                'updatedAt': now,
            }
        }
    )

    plaques_moderation_collection.delete_one({'_id': oid})
    return jsonify({
        'success': True,
        'personId': person_id,
        'wiredQrCodeId': wired_qr_code_id,
        'qrCode': _qr_str(moderation_doc.get('qrCode')),
        'qrDocId': str(qr_doc_id) if qr_doc_id else '',
    })


@application.route('/api/admin/cemeteries/exists', methods=['GET'])
def admin_cemetery_exists():
    name = loc_clean_str(request.args.get('name'))
    area = loc_clean_str(request.args.get('area'))
    if not name:
        abort(400, description='`name` is required')

    exists = _admin_moderation_cemetery_exists(name, area)
    return jsonify({
        'exists': bool(exists),
    })


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


def _resolve_premium_partner_by_firma_id(raw_firma_id):
    firma_id = _clean_nonempty_string(raw_firma_id)
    if not firma_id:
        return None, ""

    try:
        oid = ObjectId(firma_id)
    except Exception:
        return None, ""

    firma = premium_qr_firmas_collection.find_one(
        {"_id": oid},
        {"name": 1, "address": 1, "logo": 1, "website": 1}
    )
    if not firma:
        return None, ""

    partner_payload = _build_premium_partner_payload(firma)
    partner_link = _clean_nonempty_string(firma.get("website"))
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
        if partner_service_id.startswith('firma:'):
            partner_payload, partner_link = _resolve_premium_partner_by_firma_id(
                partner_service_id.split(':', 1)[1]
            )
        else:
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
    premium_payload = sanitize_premium_payload(person.get('premium'))
    admin_page = person.get('adminPage') if isinstance(person.get('adminPage'), dict) else {}
    admin_path_key = _clean_str(admin_page.get('pathKey')).lower()
    cemetery_id = ''
    burial = person.get('burial') if isinstance(person.get('burial'), dict) else {}
    cemetery_ref = burial.get('cemeteryRef') if isinstance(burial.get('cemeteryRef'), dict) else {}
    raw_cemetery_id = _clean_str(cemetery_ref.get('id') or person.get('cemeteryId'))
    if raw_cemetery_id:
        try:
            cemetery_id = str(ObjectId(raw_cemetery_id))
        except Exception:
            cemetery_id = ''
    if not cemetery_id:
        cemetery_name = _clean_str(cemetery_ref.get('name')) or _clean_str(person.get('cemetery'))
        if cemetery_name:
            cemetery_doc = cemeteries_collection.find_one(
                {'name': {'$regex': f'^{re.escape(cemetery_name)}$', '$options': 'i'}},
                {'_id': 1},
            )
            if cemetery_doc and isinstance(cemetery_doc.get('_id'), ObjectId):
                cemetery_id = str(cemetery_doc.get('_id'))

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
        "cemeteryId": cemetery_id,
        "location": person.get('location'),
        "burial": person.get('burial') if _location_read_uses_canonical() else None,
        "bio": person.get('bio'),
        "achievements": person.get('achievements') or person.get('bio') or '',
        "sourceLink": person.get('sourceLink') or person.get('internetLinks') or person.get('link') or '',
        "photos": photos_norm,
        "sharedPending": person.get('sharedPending', []),
        "sharedPhotos": person.get('sharedPhotos', []),
        "comments": _sort_comments(person.get('comments', [])),
        "isPremiumProfile": bool(premium_payload),
        "isPlaqueProfile": admin_path_key == 'plaques',
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
        full_address = _clean_str(location.get('display'))
        cemeteries_list.append({
            "id": str(cemetery.get('_id')),
            "name": cemetery.get('name'),
            "image": cemetery.get('image'),
            "address": full_address or legacy_loc.get("address") or cemetery.get('address'),
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
    full_address = _clean_str(location.get('display'))
    return jsonify({
        "id": str(cemetery.get('_id')),
        "name": cemetery.get('name'),
        "image": cemetery.get('image'),
        "address": full_address or legacy_loc.get("address") or cemetery.get('address'),
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


def _calculate_admin_cemetery_fill_percent(cemetery):
    location = normalize_location_core(cemetery.get('location') if isinstance(cemetery.get('location'), dict) else {})
    legacy_loc = cemetery_location_to_legacy(location)

    name = _clean_str(cemetery.get('name'))
    locality = _clean_str(cemetery.get('locality') or legacy_loc.get('locality'))
    street = _clean_str(cemetery.get('addressLine') or legacy_loc.get('addressLine') or cemetery.get('address'))
    description = _clean_str(cemetery.get('description'))
    page_image = _clean_str(cemetery.get('pageImage') or cemetery.get('image'))

    church_refs_raw = cemetery.get('churchRefs')
    churches_list_raw = cemetery.get('churchesList')
    church_refs = normalize_refs_list(church_refs_raw if isinstance(church_refs_raw, list) else churches_list_raw)

    phone_contacts = _clean_str_list(cemetery.get('phoneContacts'), 'phoneContacts')
    legacy_phone = _clean_str(cemetery.get('phone'))
    if not phone_contacts and legacy_phone:
        phone_contacts = [legacy_phone]
    contact_persons = _clean_str_list(cemetery.get('contactPersons'), 'contactPersons')

    score = 0
    if name:
        score += 15
    if locality:
        score += 15
    if street:
        score += 15
    if church_refs:
        score += 15
    if page_image:
        score += 15
    if phone_contacts or contact_persons:
        score += 15
    if description:
        score += 10
    return max(0, min(100, score))


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
    fill_percent = _calculate_admin_cemetery_fill_percent(cemetery)

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
    payload['fillPercent'] = _calculate_admin_cemetery_fill_percent(payload)
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
    existing = cemeteries_collection.find_one({'_id': oid})
    if not existing:
        abort(404, description='Cemetery not found')

    update_fields = _build_admin_cemetery_payload(data, partial=True)
    if not update_fields:
        abort(400, description='Nothing to update')

    merged_for_fill = dict(existing)
    merged_for_fill.update(update_fields)
    update_fields['fillPercent'] = _calculate_admin_cemetery_fill_percent(merged_for_fill)
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


ADS_APPLICATION_STATUSES = {'pending', 'verified', 'rejected'}
ADS_CAMPAIGN_STATUSES = {'active', 'inactive'}
ADS_SURFACE_TYPES = {'columns', 'pages', 'plaques'}
ADS_ALLOWED_CAMPAIGN_FIELDS = {
    'cemeteryId',
    'companyName',
    'websiteUrl',
    'creativeUrl',
    'address',
    'phone',
    'periodStart',
    'periodEnd',
    'daysTotal',
    'displayOn',
    'status',
    'payments',
    'plaques',
    'surfaceType',
}
ADS_ALLOWED_APPLICATION_FIELDS = {
    'cemeteryId',
    'cemeteryName',
    'companyName',
    'websiteUrl',
    'creativeUrl',
    'pdfUrl',
    'address',
    'phone',
    'periodStart',
    'periodEnd',
    'daysTotal',
    'displayOn',
    'status',
    'reason',
    'plaques',
    'surfaceType',
}


def _ads_parse_iso_date(value, field_name, required=False):
    text = _clean_str(value)
    if not text:
        if required:
            abort(400, description=f'`{field_name}` is required')
        return ''
    try:
        return datetime.strptime(text, '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        abort(400, description=f'`{field_name}` must be in YYYY-MM-DD format')


def _ads_days_between(start_iso, end_iso):
    if not start_iso or not end_iso:
        return 0
    start_dt = datetime.strptime(start_iso, '%Y-%m-%d')
    end_dt = datetime.strptime(end_iso, '%Y-%m-%d')
    diff = (end_dt - start_dt).days
    if diff < 0:
        abort(400, description='`periodEnd` must be greater than or equal to `periodStart`')
    return diff


def _ads_clean_int(value, field_name, default=0):
    if value is None or value == '':
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        abort(400, description=f'`{field_name}` must be an integer')
    return parsed


def _ads_parse_cemetery_object_id(value, required=False):
    text = _clean_str(value)
    if not text:
        if required:
            abort(400, description='`cemeteryId` is required')
        return None
    try:
        return ObjectId(text)
    except Exception:
        abort(400, description='Invalid `cemeteryId`')


def _ads_find_cemetery(cemetery_oid):
    if not cemetery_oid:
        return None
    return cemeteries_collection.find_one({'_id': cemetery_oid}, {'name': 1, 'address': 1, 'addressLine': 1, 'locality': 1, 'location': 1})


def _ads_extract_cemetery_address(cemetery):
    if not isinstance(cemetery, dict):
        return ''
    location = normalize_cemetery_location(cemetery)
    legacy_loc = cemetery_location_to_legacy(location)
    return _clean_str(
        cemetery.get('addressLine')
        or legacy_loc.get('addressLine')
        or cemetery.get('address')
        or cemetery.get('locality')
        or legacy_loc.get('locality')
    )


def _ads_normalize_campaign_status(value):
    return 'inactive' if _clean_str(value).lower() in {'inactive', 'disabled', 'archived'} else 'active'


def _ads_normalize_application_status(value):
    normalized = _clean_str(value).lower()
    if normalized in ADS_APPLICATION_STATUSES:
        return normalized
    return 'pending'


def _ads_normalize_surface_type(value, default='columns'):
    normalized = _clean_str(value).lower()
    if normalized in ADS_SURFACE_TYPES:
        return normalized
    return default


def _ads_mode_to_surface_type(mode):
    normalized = _clean_str(mode).lower()
    if normalized in ADS_SURFACE_TYPES:
        return normalized
    abort(400, description='`mode` must be one of: columns, pages, plaques')


def _ads_surface_query_for_mode(mode):
    surface_type = _ads_mode_to_surface_type(mode)
    if surface_type == 'columns':
        return {
            '$or': [
                {'surfaceType': 'columns'},
                {'surfaceType': {'$exists': False}},
                {'surfaceType': ''},
                {'surfaceType': None},
            ]
        }
    return {'surfaceType': surface_type}


def _ads_campaign_status_label(status):
    return 'Неактивний' if status == 'inactive' else 'Активний'


def _ads_application_status_label(status):
    if status == 'verified':
        return 'Верифіковано'
    if status == 'rejected':
        return 'Відхилено'
    return 'На модерації'


def _ads_normalize_payments(value):
    if value is None:
        return []
    if not isinstance(value, list):
        abort(400, description='`payments` must be an array')

    payments = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            abort(400, description=f'`payments[{idx}]` must be an object')
        start = _ads_parse_iso_date(item.get('periodStart'), f'payments[{idx}].periodStart', required=False)
        end = _ads_parse_iso_date(item.get('periodEnd'), f'payments[{idx}].periodEnd', required=False)
        days = _ads_clean_int(item.get('daysTotal'), f'payments[{idx}].daysTotal', default=0)
        if (start and end) and days <= 0:
            days = _ads_days_between(start, end)
        pdf_url = _clean_str(item.get('pdfUrl'))
        status = _clean_str(item.get('status'))
        if not start and not end and not pdf_url:
            continue
        payments.append({
            'periodStart': start,
            'periodEnd': end,
            'daysTotal': max(days, 0),
            'pdfUrl': pdf_url,
            'status': status,
        })
    return payments


def _ads_normalize_plaques(value, strict=False):
    if value is None:
        return []
    if not isinstance(value, list):
        if strict:
            abort(400, description='`plaques` must be an array')
        return []

    plaques = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            if strict:
                abort(400, description=f'`plaques[{idx}]` must be an object')
            continue
        person_id = _clean_str(item.get('personId') or item.get('id'))
        name = _clean_str(item.get('name'))
        life_range_text = _clean_str(item.get('lifeRangeText'))
        avatar_url = _clean_str(item.get('avatarUrl'))
        if not person_id and not name and not life_range_text and not avatar_url:
            continue
        plaques.append({
            'personId': person_id,
            'name': name,
            'lifeRangeText': life_range_text,
            'avatarUrl': avatar_url,
        })
    return plaques


def _ads_is_standard_account_person(person):
    if not isinstance(person, dict):
        return False
    if _admin_pages_has_premium(person) or _admin_pages_is_premium_firma(person):
        return False
    admin_page = person.get('adminPage') if isinstance(person.get('adminPage'), dict) else {}
    path_key = _admin_pages_str(admin_page.get('pathKey')).lower()
    path_label = _admin_pages_str(admin_page.get('pathLabel')).lower()
    if not path_key:
        if path_label == 'табличка':
            path_key = 'plaques'
        elif path_label == 'преміум qr':
            path_key = 'premium_qr'
        elif path_label == 'преміум qr | фірма':
            path_key = 'premium_qr_firma'
    return path_key not in {'plaques', 'premium_qr', 'premium_qr_firma'}


def _ads_collect_standard_accounts_counts():
    by_cemetery_oid = {}
    by_cemetery_name = {}
    projection = {
        'premium': 1,
        'adminPage.pathKey': 1,
        'adminPage.pathLabel': 1,
        'burial.cemeteryRef.id': 1,
        'burial.cemeteryRef.name': 1,
        'cemetery': 1,
    }
    for person in people_collection.find({}, projection):
        if not _ads_is_standard_account_person(person):
            continue
        cemetery_id = (
            person.get('burial', {}).get('cemeteryRef', {}).get('id')
            if isinstance(person.get('burial'), dict)
            else None
        )
        if cemetery_id:
            try:
                cemetery_oid = ObjectId(str(cemetery_id))
            except Exception:
                cemetery_oid = None
            if cemetery_oid is not None:
                by_cemetery_oid[cemetery_oid] = by_cemetery_oid.get(cemetery_oid, 0) + 1
                continue

        cemetery_name = _clean_str(
            person.get('burial', {}).get('cemeteryRef', {}).get('name')
            if isinstance(person.get('burial'), dict)
            else ''
        ) or _clean_str(person.get('cemetery'))
        if not cemetery_name:
            continue
        key = cemetery_name.lower()
        by_cemetery_name[key] = by_cemetery_name.get(key, 0) + 1
    return by_cemetery_oid, by_cemetery_name


def _ads_columns_meta_for_cemetery(cemetery_oid):
    if not cemetery_oid:
        return None
    return ads_columns_meta_collection.find_one(
        {'cemeteryId': cemetery_oid},
        {
            'totalColumns': 1,
            'priceUah': 1,
            'maxColumnsAds': 1,
            'maxPagesAds': 1,
            'adsQrToken': 1,
            'adsQrScanUrl': 1,
            'adsQrImageUrl': 1,
            'adsQrUpdatedAt': 1,
        }
    )


def _ads_public_base_url():
    if PUBLIC_WEB_BASE_URL:
        return PUBLIC_WEB_BASE_URL.rstrip('/')
    try:
        host = _clean_str(request.host_url)
    except RuntimeError:
        host = ''
    return host.rstrip('/')


def _ads_build_cemetery_qr_scan_url(cemetery_oid):
    base = _ads_public_base_url()
    path = f"/?cemeteryId={quote(str(cemetery_oid), safe='')}"
    return f'{base}{path}' if base else path


def _ads_generate_unique_qr_token():
    for _ in range(12):
        token = secrets.token_urlsafe(9).rstrip('=')
        if not ads_columns_meta_collection.find_one({'adsQrToken': token}, {'_id': 1}):
            return token
    abort(500, description='Failed to allocate unique ads QR token')


def _ads_build_qr_image_url(scan_url):
    cleaned = _clean_str(scan_url)
    if not cleaned or segno is None:
        return ''
    out = io.BytesIO()
    qr = segno.make(cleaned, error='m')
    qr.save(out, kind='svg', scale=6, border=2, dark='#111111', light='#ffffff')
    encoded = base64.b64encode(out.getvalue()).decode('ascii')
    return f'data:image/svg+xml;base64,{encoded}'


def _ads_build_qr_svg_bytes(scan_url):
    cleaned = _clean_str(scan_url)
    if not cleaned or segno is None:
        return b''
    out = io.BytesIO()
    qr = segno.make(cleaned, error='m')
    qr.save(out, kind='svg', scale=6, border=2, dark='#111111', light='#ffffff')
    return out.getvalue()


def _ads_ensure_cemetery_qr(cemetery_oid, *, force_regenerate=False):
    meta = _ads_columns_meta_for_cemetery(cemetery_oid) or {}
    token = _clean_str(meta.get('adsQrToken'))
    scan_url = _clean_str(meta.get('adsQrScanUrl'))
    image_url = _clean_str(meta.get('adsQrImageUrl'))

    if force_regenerate or not token:
        token = _ads_generate_unique_qr_token()
    if force_regenerate or not scan_url:
        scan_url = _ads_build_cemetery_qr_scan_url(cemetery_oid)
    if force_regenerate or not image_url:
        image_url = _ads_build_qr_image_url(scan_url)

    now = datetime.utcnow()
    ads_columns_meta_collection.update_one(
        {'cemeteryId': cemetery_oid},
        {
            '$set': {
                'adsQrToken': token,
                'adsQrScanUrl': scan_url,
                'adsQrImageUrl': image_url,
                'adsQrUpdatedAt': now,
                'updatedAt': now,
            },
            '$setOnInsert': {
                'cemeteryId': cemetery_oid,
                'createdAt': now,
            },
        },
        upsert=True,
    )
    return {
        'token': token,
        'scanUrl': scan_url,
        'imageUrl': image_url,
    }


def _ads_build_settings_qr_payload(cemetery_oid, total_columns, meta):
    if int(total_columns or 0) <= 0:
        return {
            'enabled': False,
            'scanUrl': '',
            'imageUrl': '',
            'exportUrl': '',
        }
    qr_scan_url = _clean_str((meta or {}).get('adsQrScanUrl'))
    qr_image_url = _clean_str((meta or {}).get('adsQrImageUrl'))
    if not qr_scan_url or not qr_image_url:
        ensured = _ads_ensure_cemetery_qr(cemetery_oid)
        qr_scan_url = _clean_str(ensured.get('scanUrl'))
        qr_image_url = _clean_str(ensured.get('imageUrl'))
    return {
        'enabled': True,
        'scanUrl': qr_scan_url,
        'imageUrl': qr_image_url,
        'exportUrl': f"/api/admin/ads/settings/{quote(str(cemetery_oid), safe='')}/qr.svg",
    }


def _ads_limit_field_for_surface(surface_type):
    if surface_type == 'columns':
        return 'maxColumnsAds'
    if surface_type == 'pages':
        return 'maxPagesAds'
    return ''


def _ads_limit_label_for_surface(surface_type):
    if surface_type == 'columns':
        return 'стовпчиках'
    if surface_type == 'pages':
        return 'сторінках'
    return 'площинах'


def _ads_active_limit_for_surface(cemetery_oid, surface_type):
    limit_field = _ads_limit_field_for_surface(surface_type)
    if not limit_field:
        return None
    meta = _ads_columns_meta_for_cemetery(cemetery_oid) or {}
    return max(_ads_clean_int(meta.get(limit_field), limit_field, default=0), 0)


def _ads_enforce_active_limit(cemetery_oid, surface_type, ignore_campaign_id=None):
    limit = _ads_active_limit_for_surface(cemetery_oid, surface_type)
    if limit is None:
        return

    query_filter = {'cemeteryId': cemetery_oid, 'status': 'active'}
    query_filter.update(_ads_surface_query_for_mode(surface_type))
    if ignore_campaign_id is not None:
        query_filter['_id'] = {'$ne': ignore_campaign_id}

    active_count = ads_campaigns_collection.count_documents(query_filter)
    if active_count >= limit:
        label = _ads_limit_label_for_surface(surface_type)
        abort(409, description=f'Ліміт реклами на {label} для цього кладовища вичерпано ({limit}).')


def _ads_build_campaign_payload(data, partial=False, existing=None):
    if not isinstance(data, dict):
        abort(400, description='Body must be a JSON object')

    unknown = set(data.keys()) - ADS_ALLOWED_CAMPAIGN_FIELDS
    if unknown:
        abort(400, description=f'Unsupported fields: {", ".join(sorted(unknown))}')

    merged = dict(existing or {})
    merged.update(data)

    cemetery_oid = _ads_parse_cemetery_object_id(merged.get('cemeteryId'), required=True)
    cemetery_doc = _ads_find_cemetery(cemetery_oid)
    if not cemetery_doc:
        abort(404, description='Cemetery not found')

    company_name = _clean_str(merged.get('companyName'))
    address = _clean_str(merged.get('address'))
    phone = _clean_str(merged.get('phone'))
    period_start = _ads_parse_iso_date(merged.get('periodStart'), 'periodStart', required=True)
    period_end = _ads_parse_iso_date(merged.get('periodEnd'), 'periodEnd', required=True)
    days_total = _ads_clean_int(merged.get('daysTotal'), 'daysTotal', default=0)
    if days_total <= 0:
        days_total = _ads_days_between(period_start, period_end)
    if days_total <= 0:
        abort(400, description='`daysTotal` must be greater than 0')

    if not company_name:
        abort(400, description='`companyName` is required')
    if not address:
        abort(400, description='`address` is required')
    if not phone:
        abort(400, description='`phone` is required')

    meta = _ads_columns_meta_for_cemetery(cemetery_oid) or {}
    normalized = {
        'cemeteryId': cemetery_oid,
        'cemeteryName': _clean_str(cemetery_doc.get('name')),
        'cemeteryAddress': _ads_extract_cemetery_address(cemetery_doc),
        'surfaceType': _ads_normalize_surface_type(merged.get('surfaceType'), default='columns'),
        'companyName': company_name,
        'websiteUrl': _clean_str(merged.get('websiteUrl')),
        'creativeUrl': _clean_str(merged.get('creativeUrl')),
        'address': address,
        'phone': phone,
        'periodStart': period_start,
        'periodEnd': period_end,
        'daysTotal': days_total,
        'displayOn': _clean_str(merged.get('displayOn')) or 'Головна сторінка, профіль',
        'status': _ads_normalize_campaign_status(merged.get('status')),
        'payments': _ads_normalize_payments(merged.get('payments')),
        'plaques': _ads_normalize_plaques(merged.get('plaques'), strict=True),
        'priceUah': _ads_clean_int(merged.get('priceUah') if isinstance(merged, dict) else None, 'priceUah', default=_ads_clean_int(meta.get('priceUah'), 'priceUah', default=0)),
    }

    if not partial:
        return normalized

    payload = {}
    for key in data.keys():
        if key == 'cemeteryId':
            payload['cemeteryId'] = normalized['cemeteryId']
            payload['cemeteryName'] = normalized['cemeteryName']
            payload['cemeteryAddress'] = normalized['cemeteryAddress']
            payload['priceUah'] = normalized['priceUah']
            continue
        if key in normalized:
            payload[key] = normalized[key]

    if {'periodStart', 'periodEnd', 'daysTotal'} & set(data.keys()):
        payload['periodStart'] = normalized['periodStart']
        payload['periodEnd'] = normalized['periodEnd']
        payload['daysTotal'] = normalized['daysTotal']

    return payload


def _ads_build_application_payload(data):
    if not isinstance(data, dict):
        abort(400, description='Body must be a JSON object')

    unknown = set(data.keys()) - ADS_ALLOWED_APPLICATION_FIELDS
    if unknown:
        abort(400, description=f'Unsupported fields: {", ".join(sorted(unknown))}')

    cemetery_oid = _ads_parse_cemetery_object_id(data.get('cemeteryId'), required=False)
    cemetery_doc = _ads_find_cemetery(cemetery_oid) if cemetery_oid else None
    if cemetery_oid and not cemetery_doc:
        abort(404, description='Cemetery not found')

    company_name = _clean_str(data.get('companyName'))
    address = _clean_str(data.get('address'))
    phone = _clean_str(data.get('phone'))
    period_start = _ads_parse_iso_date(data.get('periodStart'), 'periodStart', required=True)
    period_end = _ads_parse_iso_date(data.get('periodEnd'), 'periodEnd', required=True)
    days_total = _ads_clean_int(data.get('daysTotal'), 'daysTotal', default=0)
    if days_total <= 0:
        days_total = _ads_days_between(period_start, period_end)
    if days_total <= 0:
        abort(400, description='`daysTotal` must be greater than 0')

    if not company_name:
        abort(400, description='`companyName` is required')
    if not address:
        abort(400, description='`address` is required')
    if not phone:
        abort(400, description='`phone` is required')

    normalized_surface_type = _ads_normalize_surface_type(data.get('surfaceType'), default='columns')
    if normalized_surface_type == 'pages' and cemetery_oid:
        meta = _ads_columns_meta_for_cemetery(cemetery_oid) or {}
        total_columns = _ads_clean_int(meta.get('totalColumns'), 'totalColumns', default=0)
        if total_columns <= 0:
            abort(400, description='Для реклами на сторінках обране кладовище має мати кількість стовбчиків більше 0')

    return {
        'cemeteryId': cemetery_oid,
        'cemeteryName': _clean_str(data.get('cemeteryName')) or _clean_str(cemetery_doc.get('name') if cemetery_doc else ''),
        'surfaceType': normalized_surface_type,
        'companyName': company_name,
        'websiteUrl': _clean_str(data.get('websiteUrl')),
        'creativeUrl': _clean_str(data.get('creativeUrl')),
        'pdfUrl': _clean_str(data.get('pdfUrl')),
        'address': address,
        'phone': phone,
        'periodStart': period_start,
        'periodEnd': period_end,
        'daysTotal': days_total,
        'displayOn': _clean_str(data.get('displayOn')) or 'Головна сторінка, профіль',
        'status': _ads_normalize_application_status(data.get('status')),
        'reason': _clean_str(data.get('reason')),
        'plaques': _ads_normalize_plaques(data.get('plaques'), strict=True),
    }


def _ads_days_left(period_end):
    try:
        end_dt = datetime.strptime(_clean_str(period_end), '%Y-%m-%d')
    except Exception:
        return 0
    return max((end_dt.date() - datetime.utcnow().date()).days, 0)


def _ads_serialize_campaign(doc):
    status = _ads_normalize_campaign_status(doc.get('status'))
    created_at = doc.get('createdAt')
    updated_at = doc.get('updatedAt')
    plaques = _ads_normalize_plaques(doc.get('plaques'))
    source_application_id = doc.get('sourceApplicationId')
    if isinstance(source_application_id, ObjectId):
        source_application_id = str(source_application_id)
    return {
        'id': str(doc.get('_id')),
        'cemeteryId': str(doc.get('cemeteryId')) if isinstance(doc.get('cemeteryId'), ObjectId) else _clean_str(doc.get('cemeteryId')),
        'cemeteryName': _clean_str(doc.get('cemeteryName')),
        'surfaceType': _ads_normalize_surface_type(doc.get('surfaceType'), default='columns'),
        'companyName': _clean_str(doc.get('companyName')),
        'websiteUrl': _clean_str(doc.get('websiteUrl')),
        'creativeUrl': _clean_str(doc.get('creativeUrl')),
        'pdfUrl': _clean_str(doc.get('pdfUrl')),
        'address': _clean_str(doc.get('address')),
        'phone': _clean_str(doc.get('phone')),
        'periodStart': _clean_str(doc.get('periodStart')),
        'periodEnd': _clean_str(doc.get('periodEnd')),
        'daysTotal': _ads_clean_int(doc.get('daysTotal'), 'daysTotal', default=0),
        'daysLeft': _ads_days_left(doc.get('periodEnd')),
        'priceUah': _ads_clean_int(doc.get('priceUah'), 'priceUah', default=0),
        'status': status,
        'statusLabel': _ads_campaign_status_label(status),
        'displayOn': _clean_str(doc.get('displayOn')) or 'Головна сторінка, профіль',
        'payments': _ads_normalize_payments(doc.get('payments')),
        'plaques': plaques,
        'plaquesCount': len(plaques),
        'sourceApplicationId': _clean_str(source_application_id),
        'createdAt': created_at.isoformat() if isinstance(created_at, datetime) else None,
        'updatedAt': updated_at.isoformat() if isinstance(updated_at, datetime) else None,
    }


def _ads_serialize_application(doc):
    status = _ads_normalize_application_status(doc.get('status'))
    created_at = doc.get('createdAt')
    updated_at = doc.get('updatedAt')
    plaques = _ads_normalize_plaques(doc.get('plaques'))
    return {
        'id': str(doc.get('_id')),
        'cemeteryId': str(doc.get('cemeteryId')) if isinstance(doc.get('cemeteryId'), ObjectId) else _clean_str(doc.get('cemeteryId')),
        'cemeteryName': _clean_str(doc.get('cemeteryName')),
        'surfaceType': _ads_normalize_surface_type(doc.get('surfaceType'), default='columns'),
        'companyName': _clean_str(doc.get('companyName')),
        'websiteUrl': _clean_str(doc.get('websiteUrl')),
        'creativeUrl': _clean_str(doc.get('creativeUrl')),
        'pdfUrl': _clean_str(doc.get('pdfUrl')),
        'address': _clean_str(doc.get('address')),
        'phone': _clean_str(doc.get('phone')),
        'periodStart': _clean_str(doc.get('periodStart')),
        'periodEnd': _clean_str(doc.get('periodEnd')),
        'daysTotal': _ads_clean_int(doc.get('daysTotal'), 'daysTotal', default=0),
        'displayOn': _clean_str(doc.get('displayOn')) or 'Головна сторінка, профіль',
        'status': status,
        'statusLabel': _ads_application_status_label(status),
        'reason': _clean_str(doc.get('reason')),
        'plaques': plaques,
        'plaquesCount': len(plaques),
        'createdAt': created_at.isoformat() if isinstance(created_at, datetime) else None,
        'updatedAt': updated_at.isoformat() if isinstance(updated_at, datetime) else None,
    }


@application.route('/api/admin/ads/columns', methods=['GET'])
def admin_ads_list_columns_cemeteries():
    mode = _clean_str(request.args.get('mode', 'columns')).lower()
    surface_filter = _ads_surface_query_for_mode(mode)
    search = _clean_str(request.args.get('search', ''))
    regex = {'$regex': re.escape(search), '$options': 'i'} if search else None

    meta_docs = list(ads_columns_meta_collection.find({}, {'cemeteryId': 1, 'totalColumns': 1}))
    meta_by_cemetery = {}
    related_ids = set()
    for meta in meta_docs:
        cemetery_oid = meta.get('cemeteryId')
        if not isinstance(cemetery_oid, ObjectId):
            continue
        related_ids.add(cemetery_oid)
        meta_by_cemetery[cemetery_oid] = _ads_clean_int(meta.get('totalColumns'), 'totalColumns', default=0)

    campaigns_count = {}
    campaigns_busy_count = {}
    campaigns_query = {'status': 'active'}
    campaigns_query.update(surface_filter)
    for campaign in ads_campaigns_collection.find(campaigns_query, {'cemeteryId': 1, 'plaques': 1}):
        cemetery_oid = campaign.get('cemeteryId')
        if not isinstance(cemetery_oid, ObjectId):
            continue
        related_ids.add(cemetery_oid)
        campaigns_count[cemetery_oid] = campaigns_count.get(cemetery_oid, 0) + 1
        plaques_count = len(_ads_normalize_plaques(campaign.get('plaques')))
        if plaques_count > 0:
            campaigns_busy_count[cemetery_oid] = campaigns_busy_count.get(cemetery_oid, 0) + 1

    applications_count = {}
    plaques_applications_count = {}
    applications_query = {'status': 'pending'}
    applications_query.update(surface_filter)
    for app in ads_applications_collection.find(applications_query, {'cemeteryId': 1, 'plaques': 1}):
        cemetery_oid = app.get('cemeteryId')
        if not isinstance(cemetery_oid, ObjectId):
            continue
        related_ids.add(cemetery_oid)
        applications_count[cemetery_oid] = applications_count.get(cemetery_oid, 0) + 1
        plaques_count = len(_ads_normalize_plaques(app.get('plaques')))
        if plaques_count > 0:
            plaques_applications_count[cemetery_oid] = plaques_applications_count.get(cemetery_oid, 0) + 1

    activated_plaques_count = {}
    activated_plaques_count_by_name = {}
    people_query = {
        'adminPage.pathKey': 'plaques',
    }
    for person in people_collection.find(people_query, {'burial.cemeteryRef.id': 1, 'cemetery': 1, 'burial.cemeteryRef.name': 1}):
        cemetery_id = (
            person.get('burial', {}).get('cemeteryRef', {}).get('id')
            if isinstance(person.get('burial'), dict)
            else None
        )
        if cemetery_id:
            try:
                cemetery_oid = ObjectId(str(cemetery_id))
            except Exception:
                cemetery_oid = None
            if cemetery_oid is not None:
                related_ids.add(cemetery_oid)
                activated_plaques_count[cemetery_oid] = activated_plaques_count.get(cemetery_oid, 0) + 1
                continue

        cemetery_name = _clean_str(
            person.get('burial', {}).get('cemeteryRef', {}).get('name')
            if isinstance(person.get('burial'), dict)
            else ''
        ) or _clean_str(person.get('cemetery'))
        if not cemetery_name:
            continue
        key = cemetery_name.lower()
        activated_plaques_count_by_name[key] = activated_plaques_count_by_name.get(key, 0) + 1

    standard_accounts_by_oid = {}
    standard_accounts_by_name = {}
    if mode == 'pages':
        standard_accounts_by_oid, standard_accounts_by_name = _ads_collect_standard_accounts_counts()

    query_filter = {}
    if related_ids:
        query_filter['_id'] = {'$in': list(related_ids)}
    if regex:
        query_filter['$or'] = [
            {'name': regex},
            {'address': regex},
            {'addressLine': regex},
            {'locality': regex},
            {'location.area.display': regex},
            {'location.display': regex},
        ]

    cemetery_docs = list(cemeteries_collection.find(query_filter, {'name': 1, 'address': 1, 'addressLine': 1, 'locality': 1, 'location': 1}).sort([('name', 1), ('_id', 1)]))
    items = []
    for cemetery in cemetery_docs:
        cemetery_oid = cemetery.get('_id')
        total_columns = meta_by_cemetery.get(cemetery_oid, 0)
        if mode == 'pages':
            total_columns = (
                standard_accounts_by_oid.get(cemetery_oid, 0)
                + standard_accounts_by_name.get(_clean_str(cemetery.get('name')).lower(), 0)
            )
        if mode == 'columns' and total_columns <= 0:
            continue
        busy_plaques = campaigns_busy_count.get(cemetery_oid, 0)
        legacy_plaques_count = activated_plaques_count_by_name.get(_clean_str(cemetery.get('name')).lower(), 0)
        items.append({
            'cemeteryId': str(cemetery_oid),
            'name': _clean_str(cemetery.get('name')),
            'address': _ads_extract_cemetery_address(cemetery),
            'columnsCount': total_columns,
            'adsCount': campaigns_count.get(cemetery_oid, 0),
            'applicationsCount': applications_count.get(cemetery_oid, 0),
            'plaquesCount': activated_plaques_count.get(cemetery_oid, 0) + legacy_plaques_count,
            'busyPlaquesCount': busy_plaques,
            'freePlaquesCount': max(total_columns - busy_plaques, 0),
            'plaquesApplicationsCount': plaques_applications_count.get(cemetery_oid, 0),
        })

    return jsonify({
        'total': len(items),
        'items': items,
    })


@application.route('/api/admin/ads/columns/<string:cemetery_id>', methods=['GET'])
def admin_ads_get_columns_cemetery(cemetery_id):
    mode = _clean_str(request.args.get('mode', 'columns')).lower()
    surface_filter = _ads_surface_query_for_mode(mode)
    search = _clean_str(request.args.get('search', ''))
    regex = {'$regex': re.escape(search), '$options': 'i'} if search else None
    cemetery_oid = _ads_parse_cemetery_object_id(cemetery_id, required=True)
    cemetery_doc = _ads_find_cemetery(cemetery_oid)
    if not cemetery_doc:
        abort(404, description='Cemetery not found')

    applications_filter = {'cemeteryId': cemetery_oid, 'status': 'pending'}
    campaigns_filter = {'cemeteryId': cemetery_oid, 'status': 'active'}
    applications_filter.update(surface_filter)
    campaigns_filter.update(surface_filter)
    if regex:
        search_or = [
            {'companyName': regex},
            {'address': regex},
            {'phone': regex},
            {'websiteUrl': regex},
        ]
        applications_filter['$or'] = search_or
        campaigns_filter['$or'] = search_or

    application_docs = list(ads_applications_collection.find(applications_filter).sort([('updatedAt', -1), ('createdAt', -1), ('_id', -1)]))
    campaign_docs = list(ads_campaigns_collection.find(campaigns_filter).sort([('updatedAt', -1), ('createdAt', -1), ('_id', -1)]))
    if mode == 'plaques':
        application_docs = [doc for doc in application_docs if len(_ads_normalize_plaques(doc.get('plaques'))) > 0]
        campaign_docs = [doc for doc in campaign_docs if len(_ads_normalize_plaques(doc.get('plaques'))) > 0]

    meta = _ads_columns_meta_for_cemetery(cemetery_oid) or {}
    total_columns = _ads_clean_int(meta.get('totalColumns'), 'totalColumns', default=0)
    busy_columns = len(campaign_docs)
    free_columns = max(total_columns - busy_columns, 0)

    if mode == 'plaques':
        cemetery_name = _clean_str(cemetery_doc.get('name'))
        people_query = {
            'adminPage.pathKey': 'plaques',
            '$or': [
                {'burial.cemeteryRef.id': str(cemetery_oid)},
            ],
        }
        if cemetery_name:
            people_query['$or'].extend([
                {'burial.cemeteryRef.name': cemetery_name},
                {'cemetery': cemetery_name},
            ])
        total_plaques = people_collection.count_documents(people_query)

        busy_person_ids = set()
        for campaign in campaign_docs:
            for plaque in _ads_normalize_plaques(campaign.get('plaques')):
                person_id = _clean_str(plaque.get('personId'))
                if person_id:
                    busy_person_ids.add(person_id)

        total_columns = total_plaques
        busy_columns = len(busy_person_ids)
        free_columns = max(total_columns - busy_columns, 0)
    elif mode == 'pages':
        standard_accounts_by_oid, standard_accounts_by_name = _ads_collect_standard_accounts_counts()
        cemetery_name = _clean_str(cemetery_doc.get('name')).lower()
        total_columns = (
            standard_accounts_by_oid.get(cemetery_oid, 0)
            + standard_accounts_by_name.get(cemetery_name, 0)
        )
        busy_columns = len(campaign_docs)
        free_columns = max(total_columns - busy_columns, 0)

    return jsonify({
        'cemeteryId': str(cemetery_oid),
        'cemeteryName': _clean_str(cemetery_doc.get('name')),
        'address': _ads_extract_cemetery_address(cemetery_doc),
        'totalColumns': total_columns,
        'freeColumns': free_columns,
        'busyColumns': busy_columns,
        'priceUah': _ads_clean_int(meta.get('priceUah'), 'priceUah', default=0),
        'applications': [_ads_serialize_application(doc) for doc in application_docs],
        'activeCampaigns': [_ads_serialize_campaign(doc) for doc in campaign_docs],
    })


@application.route('/api/admin/ads/campaigns', methods=['POST'])
def admin_ads_create_campaign():
    data = request.get_json(silent=True) or {}
    payload = _ads_build_campaign_payload(data, partial=False, existing=None)
    if _ads_normalize_campaign_status(payload.get('status')) == 'active':
        _ads_enforce_active_limit(payload.get('cemeteryId'), _ads_normalize_surface_type(payload.get('surfaceType'), default='columns'))
    now = datetime.utcnow()
    payload['createdAt'] = now
    payload['updatedAt'] = now

    inserted = ads_campaigns_collection.insert_one(payload)
    created = ads_campaigns_collection.find_one({'_id': inserted.inserted_id})
    return jsonify(_ads_serialize_campaign(created)), 201


@application.route('/api/admin/ads/campaigns/<string:campaign_id>', methods=['PATCH'])
def admin_ads_update_campaign(campaign_id):
    try:
        campaign_oid = ObjectId(campaign_id)
    except Exception:
        abort(400, description='Invalid campaign id')

    existing = ads_campaigns_collection.find_one({'_id': campaign_oid})
    if not existing:
        abort(404, description='Campaign not found')

    data = request.get_json(silent=True) or {}
    update_fields = _ads_build_campaign_payload(data, partial=True, existing=existing)
    if not update_fields:
        abort(400, description='Nothing to update')

    next_status = _ads_normalize_campaign_status(update_fields.get('status', existing.get('status')))
    next_surface_type = _ads_normalize_surface_type(update_fields.get('surfaceType', existing.get('surfaceType')), default='columns')
    next_cemetery_oid = update_fields.get('cemeteryId', existing.get('cemeteryId'))
    if next_status == 'active':
        _ads_enforce_active_limit(next_cemetery_oid, next_surface_type, ignore_campaign_id=campaign_oid)

    update_fields['updatedAt'] = datetime.utcnow()

    ads_campaigns_collection.update_one({'_id': campaign_oid}, {'$set': update_fields})
    updated = ads_campaigns_collection.find_one({'_id': campaign_oid})
    return jsonify(_ads_serialize_campaign(updated))


@application.route('/api/admin/ads/campaigns/<string:campaign_id>/status', methods=['POST'])
def admin_ads_set_campaign_status(campaign_id):
    try:
        campaign_oid = ObjectId(campaign_id)
    except Exception:
        abort(400, description='Invalid campaign id')

    existing = ads_campaigns_collection.find_one({'_id': campaign_oid})
    if not existing:
        abort(404, description='Campaign not found')

    data = request.get_json(silent=True) or {}
    status = _ads_normalize_campaign_status(data.get('status'))
    if status == 'active':
        _ads_enforce_active_limit(
            existing.get('cemeteryId'),
            _ads_normalize_surface_type(existing.get('surfaceType'), default='columns'),
            ignore_campaign_id=campaign_oid,
        )
    ads_campaigns_collection.update_one(
        {'_id': campaign_oid},
        {'$set': {'status': status, 'updatedAt': datetime.utcnow()}}
    )
    updated = ads_campaigns_collection.find_one({'_id': campaign_oid})
    return jsonify(_ads_serialize_campaign(updated))


@application.route('/api/admin/ads/applications', methods=['GET'])
def admin_ads_list_applications():
    search = _clean_str(request.args.get('search', ''))
    cemetery_id = _clean_str(request.args.get('cemeteryId', ''))
    status_filter = _clean_str(request.args.get('status', '')).lower()
    mode = _clean_str(request.args.get('mode', '')).lower()

    query_filter = {}
    if cemetery_id:
        query_filter['cemeteryId'] = _ads_parse_cemetery_object_id(cemetery_id, required=True)
    if status_filter:
        if status_filter not in ADS_APPLICATION_STATUSES:
            abort(400, description='`status` must be one of: pending, verified, rejected')
        query_filter['status'] = status_filter
    if mode:
        query_filter.update(_ads_surface_query_for_mode(mode))
    if search:
        regex = {'$regex': re.escape(search), '$options': 'i'}
        query_filter['$or'] = [
            {'companyName': regex},
            {'cemeteryName': regex},
            {'address': regex},
            {'phone': regex},
            {'websiteUrl': regex},
        ]

    docs = list(ads_applications_collection.find(query_filter).sort([('updatedAt', -1), ('createdAt', -1), ('_id', -1)]))
    items = [_ads_serialize_application(doc) for doc in docs]
    return jsonify({
        'total': len(items),
        'items': items,
    })


@application.route('/api/admin/ads/applications/<string:application_id>/verify', methods=['POST'])
def admin_ads_verify_application(application_id):
    try:
        application_oid = ObjectId(application_id)
    except Exception:
        abort(400, description='Invalid application id')

    existing = ads_applications_collection.find_one({'_id': application_oid})
    if not existing:
        abort(404, description='Application not found')
    if _ads_normalize_application_status(existing.get('status')) != 'pending':
        abort(400, description='Only pending applications can be verified')

    data = request.get_json(silent=True) or {}
    source = {
        'cemeteryId': str(existing.get('cemeteryId')) if isinstance(existing.get('cemeteryId'), ObjectId) else existing.get('cemeteryId'),
        'surfaceType': _ads_normalize_surface_type(existing.get('surfaceType'), default='columns'),
        'companyName': existing.get('companyName'),
        'websiteUrl': existing.get('websiteUrl'),
        'creativeUrl': existing.get('creativeUrl'),
        'address': existing.get('address'),
        'phone': existing.get('phone'),
        'periodStart': existing.get('periodStart'),
        'periodEnd': existing.get('periodEnd'),
        'daysTotal': existing.get('daysTotal'),
        'displayOn': existing.get('displayOn'),
        'plaques': existing.get('plaques'),
        'status': 'active',
        'payments': [],
    }
    if isinstance(data, dict):
        source.update(data)

    campaign_payload = _ads_build_campaign_payload(source, partial=False, existing=None)
    if _ads_normalize_campaign_status(campaign_payload.get('status')) == 'active':
        _ads_enforce_active_limit(
            campaign_payload.get('cemeteryId'),
            _ads_normalize_surface_type(campaign_payload.get('surfaceType'), default='columns'),
        )
    campaign_payload['sourceApplicationId'] = application_oid
    now = datetime.utcnow()
    campaign_payload['createdAt'] = now
    campaign_payload['updatedAt'] = now
    inserted = ads_campaigns_collection.insert_one(campaign_payload)
    created_campaign = ads_campaigns_collection.find_one({'_id': inserted.inserted_id})

    ads_applications_collection.update_one(
        {'_id': application_oid},
        {
            '$set': {
                'status': 'verified',
                'reason': '',
                'linkedCampaignId': inserted.inserted_id,
                'updatedAt': now,
                'verifiedAt': now,
            }
        }
    )
    updated_application = ads_applications_collection.find_one({'_id': application_oid})

    return jsonify({
        'success': True,
        'campaign': _ads_serialize_campaign(created_campaign),
        'application': _ads_serialize_application(updated_application),
    })


@application.route('/api/admin/ads/applications/<string:application_id>/reject', methods=['POST'])
def admin_ads_reject_application(application_id):
    try:
        application_oid = ObjectId(application_id)
    except Exception:
        abort(400, description='Invalid application id')

    existing = ads_applications_collection.find_one({'_id': application_oid})
    if not existing:
        abort(404, description='Application not found')
    if _ads_normalize_application_status(existing.get('status')) != 'pending':
        abort(400, description='Only pending applications can be rejected')

    data = request.get_json(silent=True) or {}
    reason = _clean_str(data.get('reason'))
    now = datetime.utcnow()
    ads_applications_collection.update_one(
        {'_id': application_oid},
        {'$set': {'status': 'rejected', 'reason': reason, 'updatedAt': now, 'rejectedAt': now}}
    )
    updated = ads_applications_collection.find_one({'_id': application_oid})
    return jsonify({
        'success': True,
        'application': _ads_serialize_application(updated),
    })


@application.route('/api/ads/applications', methods=['POST'])
def create_public_ads_application():
    data = request.get_json(silent=True) or {}
    payload = _ads_build_application_payload(data)
    payload['status'] = 'pending'
    now = datetime.utcnow()
    payload['createdAt'] = now
    payload['updatedAt'] = now

    inserted = ads_applications_collection.insert_one(payload)
    created = ads_applications_collection.find_one({'_id': inserted.inserted_id})
    return jsonify({
        'success': True,
        'application': _ads_serialize_application(created),
    }), 201


def _ads_pick_random_active_campaign(query_filter):
    docs = list(
        ads_campaigns_collection
        .find(query_filter)
        .sort([('updatedAt', -1), ('createdAt', -1), ('_id', -1)])
    )
    if not docs:
        return None
    return random.choice(docs)


@application.route('/api/ads/columns/random', methods=['GET'])
def get_public_random_columns_ad():
    cemetery_oid = _ads_parse_cemetery_object_id(request.args.get('cemeteryId'), required=True)
    campaign = _ads_pick_random_active_campaign({
        'cemeteryId': cemetery_oid,
        'status': 'active',
        **_ads_surface_query_for_mode('columns'),
    })
    if not campaign:
        return make_response('', 204)
    return jsonify(_ads_serialize_campaign(campaign))


@application.route('/api/ads/pages/random', methods=['GET'])
def get_public_random_pages_ad():
    cemetery_oid = _ads_parse_cemetery_object_id(request.args.get('cemeteryId'), required=True)
    campaign = _ads_pick_random_active_campaign({
        'cemeteryId': cemetery_oid,
        'status': 'active',
        **_ads_surface_query_for_mode('pages'),
    })
    if not campaign:
        return make_response('', 204)
    return jsonify(_ads_serialize_campaign(campaign))


@application.route('/api/ads/plaques/random', methods=['GET'])
def get_public_random_plaques_ad():
    person_id = _clean_str(request.args.get('personId'))
    if not person_id:
        abort(400, description='`personId` is required')
    campaign = _ads_pick_random_active_campaign({
        'status': 'active',
        **_ads_surface_query_for_mode('plaques'),
        'plaques.personId': person_id,
    })
    if not campaign:
        return make_response('', 204)
    return jsonify(_ads_serialize_campaign(campaign))


@application.route('/api/admin/ads/settings/<string:cemetery_id>', methods=['GET'])
def admin_ads_get_settings(cemetery_id):
    cemetery_oid = _ads_parse_cemetery_object_id(cemetery_id, required=True)
    cemetery_doc = _ads_find_cemetery(cemetery_oid)
    if not cemetery_doc:
        abort(404, description='Cemetery not found')

    meta = _ads_columns_meta_for_cemetery(cemetery_oid) or {}
    total_columns = _ads_clean_int(meta.get('totalColumns'), 'totalColumns', default=0)
    return jsonify({
        'cemeteryId': str(cemetery_oid),
        'cemeteryName': _clean_str(cemetery_doc.get('name')),
        'address': _ads_extract_cemetery_address(cemetery_doc),
        'totalColumns': total_columns,
        'priceUah': _ads_clean_int(meta.get('priceUah'), 'priceUah', default=0),
        'maxColumnsAds': _ads_clean_int(meta.get('maxColumnsAds'), 'maxColumnsAds', default=0),
        'maxPagesAds': _ads_clean_int(meta.get('maxPagesAds'), 'maxPagesAds', default=0),
        'adsQr': _ads_build_settings_qr_payload(cemetery_oid, total_columns, meta),
    })


@application.route('/api/admin/ads/settings/<string:cemetery_id>', methods=['PUT'])
def admin_ads_upsert_settings(cemetery_id):
    cemetery_oid = _ads_parse_cemetery_object_id(cemetery_id, required=True)
    cemetery_doc = _ads_find_cemetery(cemetery_oid)
    if not cemetery_doc:
        abort(404, description='Cemetery not found')

    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        abort(400, description='Body must be a JSON object')

    allowed_fields = {'totalColumns', 'maxColumnsAds', 'maxPagesAds'}
    unknown = set(data.keys()) - allowed_fields
    if unknown:
        abort(400, description=f'Unsupported fields: {", ".join(sorted(unknown))}')

    update_fields = {}
    next_total_columns = None
    if 'totalColumns' in data:
        next_total_columns = max(_ads_clean_int(data.get('totalColumns'), 'totalColumns', default=0), 0)
        update_fields['totalColumns'] = next_total_columns
    if 'maxColumnsAds' in data:
        update_fields['maxColumnsAds'] = max(_ads_clean_int(data.get('maxColumnsAds'), 'maxColumnsAds', default=0), 0)
    if 'maxPagesAds' in data:
        update_fields['maxPagesAds'] = max(_ads_clean_int(data.get('maxPagesAds'), 'maxPagesAds', default=0), 0)
    if not update_fields:
        abort(400, description='Nothing to update')

    update_fields['updatedAt'] = datetime.utcnow()
    ads_columns_meta_collection.update_one(
        {'cemeteryId': cemetery_oid},
        {'$set': update_fields, '$setOnInsert': {'cemeteryId': cemetery_oid, 'createdAt': datetime.utcnow()}},
        upsert=True
    )
    if next_total_columns is not None and next_total_columns > 0:
        _ads_ensure_cemetery_qr(cemetery_oid)

    meta = _ads_columns_meta_for_cemetery(cemetery_oid) or {}
    total_columns = _ads_clean_int(meta.get('totalColumns'), 'totalColumns', default=0)
    return jsonify({
        'cemeteryId': str(cemetery_oid),
        'cemeteryName': _clean_str(cemetery_doc.get('name')),
        'address': _ads_extract_cemetery_address(cemetery_doc),
        'totalColumns': total_columns,
        'priceUah': _ads_clean_int(meta.get('priceUah'), 'priceUah', default=0),
        'maxColumnsAds': _ads_clean_int(meta.get('maxColumnsAds'), 'maxColumnsAds', default=0),
        'maxPagesAds': _ads_clean_int(meta.get('maxPagesAds'), 'maxPagesAds', default=0),
        'adsQr': _ads_build_settings_qr_payload(cemetery_oid, total_columns, meta),
    })


@application.route('/api/admin/ads/settings/<string:cemetery_id>/qr.svg', methods=['GET'])
def admin_ads_get_settings_qr_svg(cemetery_id):
    cemetery_oid = _ads_parse_cemetery_object_id(cemetery_id, required=True)
    cemetery_doc = _ads_find_cemetery(cemetery_oid)
    if not cemetery_doc:
        abort(404, description='Cemetery not found')

    meta = _ads_columns_meta_for_cemetery(cemetery_oid) or {}
    total_columns = _ads_clean_int(meta.get('totalColumns'), 'totalColumns', default=0)
    if total_columns <= 0:
        abort(404, description='QR is unavailable for cemetery with zero columns')

    qr_scan_url = _clean_str(meta.get('adsQrScanUrl'))
    if not qr_scan_url:
        ensured = _ads_ensure_cemetery_qr(cemetery_oid)
        qr_scan_url = _clean_str(ensured.get('scanUrl'))

    svg_bytes = _ads_build_qr_svg_bytes(qr_scan_url)
    if not svg_bytes:
        abort(500, description='Failed to build QR svg')

    response = make_response(svg_bytes)
    response.headers['Content-Type'] = 'image/svg+xml; charset=utf-8'
    safe_name = re.sub(r'[^a-zA-Z0-9_-]+', '-', _clean_str(cemetery_doc.get('name')) or str(cemetery_oid))
    response.headers['Content-Disposition'] = f'attachment; filename="ads-qr-{safe_name}.svg"'
    return response


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
    'description',
    'pageImage',
    'iban',
    'taxCode',
    'recipientName',
    'botCode',
    'telegramChatId',
    'telegramChatIds',
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


def _clean_telegram_chat_ids(value):
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        cleaned = _clean_str(item)
        if cleaned and cleaned not in out:
            out.append(cleaned)
    return out


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
        phone = ''

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
    telegram_chat_ids = _clean_telegram_chat_ids(church.get('telegramChatIds'))
    legacy_chat_id = _clean_str(church.get('telegramChatId'))
    if legacy_chat_id and legacy_chat_id not in telegram_chat_ids:
        telegram_chat_ids.append(legacy_chat_id)

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
        'description': _clean_str(church.get('description')),
        'pageImage': _clean_str(church.get('pageImage') or church.get('image')),
        'iban': _clean_str(church.get('iban')),
        'taxCode': _clean_str(church.get('taxCode')),
        'recipientName': _clean_str(church.get('recipientName')),
        'botCode': _clean_str(church.get('botCode')),
        'telegramChatId': telegram_chat_ids[0] if telegram_chat_ids else legacy_chat_id,
        'telegramChatIds': telegram_chat_ids,
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

    if 'description' in data:
        payload['description'] = _clean_str(data.get('description'))

    if 'pageImage' in data:
        payload['pageImage'] = _clean_str(data.get('pageImage'))

    if 'iban' in data:
        payload['iban'] = _clean_str(data.get('iban'))

    if 'taxCode' in data:
        payload['taxCode'] = _clean_str(data.get('taxCode'))

    if 'recipientName' in data:
        payload['recipientName'] = _clean_str(data.get('recipientName'))

    if 'botCode' in data:
        payload['botCode'] = _clean_str(data.get('botCode'))

    if 'telegramChatId' in data:
        payload['telegramChatId'] = _clean_str(data.get('telegramChatId'))

    if 'telegramChatIds' in data:
        if not isinstance(data.get('telegramChatIds'), list):
            abort(400, description='`telegramChatIds` must be an array')
        payload['telegramChatIds'] = _clean_telegram_chat_ids(data.get('telegramChatIds'))

    if 'telegramChatIds' in payload:
        legacy_chat_id = _clean_str(payload.get('telegramChatId'))
        if legacy_chat_id and legacy_chat_id not in payload['telegramChatIds']:
            payload['telegramChatIds'].append(legacy_chat_id)
        payload['telegramChatId'] = payload['telegramChatIds'][0] if payload['telegramChatIds'] else ''
    elif 'telegramChatId' in payload:
        payload['telegramChatIds'] = [payload['telegramChatId']] if payload['telegramChatId'] else []

    if 'contacts' in payload and payload['contacts']:
        if not payload.get('contactPerson'):
            payload['contactPerson'] = payload['contacts'][0].get('person', '')
        if not payload.get('phone'):
            payload['phone'] = payload['contacts'][0].get('phone', '')

    if 'cemeteryRefs' in payload and 'cemeteries' not in payload:
        payload['cemeteries'] = refs_to_legacy_names(payload['cemeteryRefs'])
    if 'cemeteries' in payload and 'cemeteryRefs' not in payload:
        payload['cemeteryRefs'] = normalize_refs_list(payload['cemeteries'])

    if 'pageImage' in payload:
        payload['image'] = payload['pageImage']

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
        payload.setdefault('description', '')
        payload.setdefault('pageImage', '')
        payload.setdefault('image', payload.get('pageImage', ''))
        payload.setdefault('iban', '')
        payload.setdefault('taxCode', '')
        payload.setdefault('recipientName', '')
        payload.setdefault('botCode', '')
        payload.setdefault('telegramChatId', '')
        payload.setdefault('telegramChatIds', [payload['telegramChatId']] if payload['telegramChatId'] else [])

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


def _generate_admin_church_bot_code():
    for _ in range(20):
        candidate = f"MEM-{secrets.token_hex(3).upper()}"
        if not churches_collection.find_one({'botCode': candidate}):
            return candidate
    return f"MEM-{secrets.token_hex(4).upper()}"


@application.route('/api/admin/churches/<string:church_id>/bot-code/regenerate', methods=['POST'])
def admin_regenerate_church_bot_code(church_id):
    try:
        oid = ObjectId(church_id)
    except Exception:
        abort(400, description='Invalid church id')

    now = datetime.utcnow()
    bot_code = _generate_admin_church_bot_code()
    result = churches_collection.update_one(
        {'_id': oid},
        {'$set': {'botCode': bot_code, 'updatedAt': now}}
    )
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


def _admin_notes_parse_graph_date(value):
    raw = _clean_str(value)
    if not raw:
        return None
    parsed_ua = _admin_notes_parse_ua_date(raw)
    if parsed_ua:
        return parsed_ua
    try:
        return datetime.strptime(raw, '%Y-%m-%d').date()
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
    if period not in {'day', 'week', 'month', 'custom'}:
        abort(400, description='`period` must be one of: day, week, month, custom')

    search = _clean_str(request.args.get('search')).casefold()
    date_from_raw = _clean_str(request.args.get('dateFrom'))
    date_to_raw = _clean_str(request.args.get('dateTo'))
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

    elif period == 'month':
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
    else:
        if not date_from_raw or not date_to_raw:
            abort(400, description='`dateFrom` and `dateTo` are required for custom period')
        date_from = _admin_notes_parse_graph_date(date_from_raw)
        date_to = _admin_notes_parse_graph_date(date_to_raw)
        if date_from is None:
            abort(400, description='`dateFrom` must be in format DD.MM.YYYY or YYYY-MM-DD')
        if date_to is None:
            abort(400, description='`dateTo` must be in format DD.MM.YYYY or YYYY-MM-DD')
        if date_from > date_to:
            abort(400, description='`dateFrom` must be less or equal to `dateTo`')
        if (date_to - date_from).days > 366:
            abort(400, description='Custom period cannot exceed 367 days')

        day_list = [date_from + timedelta(days=delta) for delta in range((date_to - date_from).days + 1)]
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


ADMIN_NOTE_PAYMENTS_STATUS_IN_PROGRESS = 'in_progress'
ADMIN_NOTE_PAYMENTS_STATUS_AWAITING_CONFIRMATION = 'awaiting_confirmation'
ADMIN_NOTE_PAYMENTS_STATUS_FINALIZABLE = 'finalizable'
ADMIN_NOTE_PAYMENTS_STATUS_COMPLETED = 'completed'
ADMIN_NOTE_PAYMENTS_DEFAULT_BALANCE = 48500
ADMIN_NOTE_PAYMENTS_MONTH_LABELS = {
    1: 'Січень',
    2: 'Лютий',
    3: 'Березень',
    4: 'Квітень',
    5: 'Травень',
    6: 'Червень',
    7: 'Липень',
    8: 'Серпень',
    9: 'Вересень',
    10: 'Жовтень',
    11: 'Листопад',
    12: 'Грудень',
}
TELEGRAM_BOT_TOKEN = _clean_str(os.environ.get('TELEGRAM_BOT_TOKEN'))


def _admin_note_payments_month_range(year, month):
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = datetime(year, month + 1, 1) - timedelta(days=1)
    return start.date(), end.date()


def _admin_note_payments_label(year, month):
    return f"{ADMIN_NOTE_PAYMENTS_MONTH_LABELS.get(month, str(month))} {year}"


def _admin_note_payments_parse_amount(value):
    if value is None:
        return 0
    if isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        num = float(value)
        if num < 0:
            return 0
        return int(round(num))
    cleaned = _clean_str(value).replace('грн', '').replace('ГРН', '').replace(' ', '').replace(',', '.')
    if not cleaned:
        return 0
    match = re.search(r'-?\d+(\.\d+)?', cleaned)
    if not match:
        return 0
    try:
        parsed = float(match.group(0))
    except Exception:
        return 0
    if parsed < 0:
        return 0
    return int(round(parsed))


def _admin_note_payments_resolve_liturgy_month_date(liturgy_doc, tzinfo):
    service_local = _admin_notes_to_local_datetime(liturgy_doc.get('serviceDate'), tzinfo)
    if isinstance(service_local, datetime):
        return service_local.date()
    created_local = _admin_notes_to_local_datetime(liturgy_doc.get('createdAt'), tzinfo)
    if isinstance(created_local, datetime):
        return created_local.date()
    return None


def _admin_note_payments_build_church_maps_for_period(start_date, end_date, tzinfo):
    church_projection = {'name': 1, 'address': 1, 'locality': 1, 'location': 1, 'telegramChatId': 1, 'status': 1}
    church_docs = list(churches_collection.find({}, church_projection).sort([('name', 1), ('_id', 1)]))
    church_by_id = {str(doc.get('_id')): doc for doc in church_docs}
    church_by_name = {}
    for doc in church_docs:
        normalized_name = _clean_str(doc.get('name')).casefold()
        if normalized_name and normalized_name not in church_by_name:
            church_by_name[normalized_name] = doc

    sums_by_church_id = {str(doc.get('_id')): 0 for doc in church_docs}
    projection = {'churchId': 1, 'churchName': 1, 'paymentStatus': 1, 'serviceDate': 1, 'createdAt': 1, 'price': 1}
    cursor = liturgies_collection.find({'paymentStatus': {'$in': ADMIN_NOTES_SUCCESS_QUERY_VALUES}}, projection)
    for liturgy_doc in cursor:
        if not _admin_notes_is_success_payment(liturgy_doc.get('paymentStatus')):
            continue
        liturgy_day = _admin_note_payments_resolve_liturgy_month_date(liturgy_doc, tzinfo)
        if liturgy_day is None or liturgy_day < start_date or liturgy_day > end_date:
            continue
        church_doc = _admin_notes_resolve_church_for_liturgy(liturgy_doc, church_by_id, church_by_name)
        if not church_doc:
            continue
        church_id = str(church_doc.get('_id'))
        sums_by_church_id[church_id] = sums_by_church_id.get(church_id, 0) + _admin_note_payments_parse_amount(liturgy_doc.get('price'))

    return church_docs, sums_by_church_id


def _admin_note_payments_row_output(row):
    confirmation = row.get('confirmation') if isinstance(row.get('confirmation'), dict) else {}
    receipt_urls = []
    raw_receipt_urls = row.get('receiptUrls')
    if isinstance(raw_receipt_urls, list):
        for item in raw_receipt_urls:
            normalized_url = _clean_str(item)
            if normalized_url and normalized_url not in receipt_urls:
                receipt_urls.append(normalized_url)
    legacy_receipt_url = _clean_str(row.get('receiptUrl'))
    if legacy_receipt_url and legacy_receipt_url not in receipt_urls:
        receipt_urls.append(legacy_receipt_url)
    receipt_preview_urls = []
    raw_preview_urls = row.get('receiptPreviewUrls')
    if isinstance(raw_preview_urls, list):
        for item in raw_preview_urls:
            receipt_preview_urls.append(_clean_str(item))
    legacy_preview_url = _clean_str(row.get('receiptPreviewUrl'))
    if legacy_preview_url:
        if receipt_preview_urls:
            if not _clean_str(receipt_preview_urls[0]):
                receipt_preview_urls[0] = legacy_preview_url
        else:
            receipt_preview_urls.append(legacy_preview_url)
    while len(receipt_preview_urls) < len(receipt_urls):
        receipt_preview_urls.append('')
    if len(receipt_preview_urls) > len(receipt_urls):
        receipt_preview_urls = receipt_preview_urls[:len(receipt_urls)]
    telegram_chat_ids = _clean_telegram_chat_ids(row.get('telegramChatIds'))
    legacy_chat_id = _clean_str(row.get('telegramChatId'))
    if legacy_chat_id and legacy_chat_id not in telegram_chat_ids:
        telegram_chat_ids.append(legacy_chat_id)
    return {
        'id': _clean_str(row.get('id')),
        'number': int(row.get('number') or 0),
        'churchId': _clean_str(row.get('churchId')),
        'churchName': _clean_str(row.get('churchName')),
        'churchAddress': _clean_str(row.get('churchAddress')),
        'telegramChatId': telegram_chat_ids[0] if telegram_chat_ids else legacy_chat_id,
        'telegramChatIds': telegram_chat_ids,
        'sumAmount': int(row.get('sumAmount') or 0),
        'receiptUrls': receipt_urls,
        'receiptUrl': receipt_urls[0] if receipt_urls else '',
        'receiptPreviewUrls': receipt_preview_urls,
        'receiptPreviewUrl': receipt_preview_urls[0] if receipt_preview_urls else '',
        'paid': bool(row.get('paid')),
        'paidAt': _clean_str(row.get('paidAt')),
        'unlocked': bool(row.get('unlocked')),
        'alert': bool(row.get('alert')),
        'alertMessage': _clean_str(row.get('alertMessage')),
        'unlockRequired': bool(row.get('unlockRequired')),
        'confirmation': {
            'status': _clean_str(confirmation.get('status')) or 'pending',
            'deadlineAt': _clean_str(confirmation.get('deadlineAt')),
            'respondedAt': _clean_str(confirmation.get('respondedAt')),
            'token': _clean_str(confirmation.get('token')),
            'autoConfirmed': bool(confirmation.get('autoConfirmed')),
        },
    }


def _admin_note_payments_get_receipt_key(receipt_url):
    cleaned_url = _clean_str(receipt_url)
    if not cleaned_url:
        return ''
    try:
        parsed = urlparse(cleaned_url)
        host = (parsed.netloc or '').lower()
        expected_host = f"{(SPACES_BUCKET or '').lower()}.{(SPACES_REGION or '').lower()}.digitaloceanspaces.com"
        if expected_host and host and host != expected_host:
            return ''
        key = parsed.path.lstrip('/')
        return key if key else ''
    except Exception:
        return ''


def _admin_note_payments_preview_key_from_receipt_key(receipt_key):
    cleaned_key = _clean_str(receipt_key)
    if not cleaned_key:
        return ''
    if '.' in cleaned_key.rsplit('/', 1)[-1]:
        base, _ = cleaned_key.rsplit('.', 1)
        return f"{base}__thumb.png"
    return f"{cleaned_key}__thumb.png"


def _admin_note_payments_generate_pdf_preview_png(pdf_bytes):
    if not pdf_bytes or pdfium is None:
        return None
    document = None
    page = None
    bitmap = None
    try:
        document = pdfium.PdfDocument(pdf_bytes)
        if len(document) < 1:
            return None
        page = document[0]
        bitmap = page.render(scale=1.2)
        pil_image = bitmap.to_pil()
        output = io.BytesIO()
        pil_image.save(output, format='PNG')
        return output.getvalue()
    except Exception as exc:
        application.logger.warning('Failed to render receipt PDF preview: %s', exc)
        return None
    finally:
        try:
            if bitmap is not None:
                bitmap.close()
        except Exception:
            pass
        try:
            if page is not None:
                page.close()
        except Exception:
            pass
        try:
            if document is not None:
                document.close()
        except Exception:
            pass


def _admin_note_payments_build_preview_url(receipt_url):
    receipt_key = _admin_note_payments_get_receipt_key(receipt_url)
    if not receipt_key:
        return ''
    preview_key = _admin_note_payments_preview_key_from_receipt_key(receipt_key)
    if not preview_key:
        return ''
    return f"https://{SPACES_BUCKET}.{SPACES_REGION}.digitaloceanspaces.com/{preview_key}"


def _admin_note_payments_generate_and_upload_preview(receipt_url):
    if not SPACES_BUCKET or not SPACES_REGION:
        return ''
    receipt_key = _admin_note_payments_get_receipt_key(receipt_url)
    if not receipt_key:
        return ''
    preview_key = _admin_note_payments_preview_key_from_receipt_key(receipt_key)
    if not preview_key:
        return ''
    try:
        response = requests.get(receipt_url, timeout=25)
        response.raise_for_status()
        preview_png = _admin_note_payments_generate_pdf_preview_png(response.content)
        if not preview_png:
            return ''
        s3.put_object(
            Bucket=SPACES_BUCKET,
            Key=preview_key,
            Body=preview_png,
            ACL='public-read',
            ContentType='image/png',
            CacheControl='public, max-age=31536000, immutable',
        )
        return f"https://{SPACES_BUCKET}.{SPACES_REGION}.digitaloceanspaces.com/{preview_key}"
    except Exception as exc:
        application.logger.warning('Failed to generate/upload receipt preview: %s', exc)
        return ''


def _admin_note_payments_delete_preview_object(preview_url):
    preview_key = _admin_note_payments_get_receipt_key(preview_url)
    if not preview_key:
        return
    try:
        s3.delete_object(Bucket=SPACES_BUCKET, Key=preview_key)
    except Exception as exc:
        application.logger.warning('Failed to delete receipt preview object: %s', exc)


def _admin_note_payments_recompute_status(doc):
    rows = doc.get('rows') if isinstance(doc.get('rows'), list) else []
    if _clean_str(doc.get('status')) == ADMIN_NOTE_PAYMENTS_STATUS_COMPLETED:
        return ADMIN_NOTE_PAYMENTS_STATUS_COMPLETED
    if not rows:
        return ADMIN_NOTE_PAYMENTS_STATUS_IN_PROGRESS
    if not all(bool(row.get('paid')) for row in rows):
        return ADMIN_NOTE_PAYMENTS_STATUS_IN_PROGRESS
    paid_requested_at = _clean_str(doc.get('paidRequestedAt'))
    if not paid_requested_at:
        return ADMIN_NOTE_PAYMENTS_STATUS_IN_PROGRESS
    confirmations = [
        _clean_str((row.get('confirmation') or {}).get('status')).lower()
        for row in rows
    ]
    if confirmations and all(status == 'yes' for status in confirmations):
        return ADMIN_NOTE_PAYMENTS_STATUS_FINALIZABLE
    return ADMIN_NOTE_PAYMENTS_STATUS_AWAITING_CONFIRMATION


def _admin_note_payments_apply_auto_confirm(doc):
    rows = doc.get('rows') if isinstance(doc.get('rows'), list) else []
    now_utc = datetime.utcnow()
    changed = False
    for row in rows:
        confirmation = row.get('confirmation')
        if not isinstance(confirmation, dict):
            continue
        status = _clean_str(confirmation.get('status')).lower() or 'pending'
        if status != 'pending':
            continue
        deadline = _admin_notes_parse_datetime(confirmation.get('deadlineAt'))
        if not isinstance(deadline, datetime):
            continue
        if deadline <= now_utc:
            confirmation['status'] = 'yes'
            confirmation['respondedAt'] = now_utc.isoformat()
            confirmation['autoConfirmed'] = True
            changed = True
    if changed:
        doc['status'] = _admin_note_payments_recompute_status(doc)
        doc['updatedAt'] = now_utc
    return changed


def _admin_note_payments_fetch_doc_or_404(payment_id):
    try:
        oid = ObjectId(payment_id)
    except Exception:
        abort(400, description='Invalid payment id')
    doc = admin_note_payments_collection.find_one({'_id': oid})
    if not doc:
        abort(404, description='Payment month not found')
    has_auto_confirm_changes = _admin_note_payments_apply_auto_confirm(doc)
    current_status = _clean_str(doc.get('status')) or ADMIN_NOTE_PAYMENTS_STATUS_IN_PROGRESS
    recomputed_status = _admin_note_payments_recompute_status(doc)
    has_status_changes = recomputed_status != current_status
    if has_status_changes:
        doc['status'] = recomputed_status
        doc['updatedAt'] = datetime.utcnow()
    if has_auto_confirm_changes or has_status_changes:
        admin_note_payments_collection.update_one(
            {'_id': oid},
            {'$set': {'rows': doc.get('rows', []), 'status': doc.get('status'), 'updatedAt': doc.get('updatedAt')}}
        )
    return doc


def _admin_note_payments_summary_output(doc):
    period = doc.get('period') if isinstance(doc.get('period'), dict) else {}
    status = _clean_str(doc.get('status')) or ADMIN_NOTE_PAYMENTS_STATUS_IN_PROGRESS
    rows = doc.get('rows') if isinstance(doc.get('rows'), list) else []
    has_alert = any(bool(row.get('alert')) for row in rows)
    if status == ADMIN_NOTE_PAYMENTS_STATUS_COMPLETED:
        status_label = 'Завершено'
    else:
        status_label = 'В процесі'
    return {
        'id': str(doc.get('_id')),
        'period': {
            'year': int(period.get('year') or 0),
            'month': int(period.get('month') or 0),
            'label': _clean_str(period.get('label')),
            'startDate': _clean_str(period.get('startDate')),
            'endDate': _clean_str(period.get('endDate')),
        },
        'status': status,
        'statusLabel': status_label,
        'hasAlert': has_alert,
        'rowsTotal': len(rows),
    }


def _admin_note_payments_detail_output(doc):
    period = doc.get('period') if isinstance(doc.get('period'), dict) else {}
    rows = doc.get('rows') if isinstance(doc.get('rows'), list) else []
    status = _clean_str(doc.get('status')) or ADMIN_NOTE_PAYMENTS_STATUS_IN_PROGRESS
    can_mark_paid = status == ADMIN_NOTE_PAYMENTS_STATUS_IN_PROGRESS and bool(rows) and all(bool(row.get('paid')) for row in rows)
    can_finalize = status == ADMIN_NOTE_PAYMENTS_STATUS_FINALIZABLE and bool(rows)
    fop_balance = doc.get('fopBalance') if isinstance(doc.get('fopBalance'), dict) else {}
    return {
        'id': str(doc.get('_id')),
        'period': {
            'year': int(period.get('year') or 0),
            'month': int(period.get('month') or 0),
            'label': _clean_str(period.get('label')),
            'startDate': _clean_str(period.get('startDate')),
            'endDate': _clean_str(period.get('endDate')),
        },
        'status': status,
        'rows': [_admin_note_payments_row_output(row) for row in rows],
        'fopBalance': {
            'initial': int(fop_balance.get('initial') or 0),
            'current': int(fop_balance.get('current') or 0),
            'lastCheckedAt': _clean_str(fop_balance.get('lastCheckedAt')),
        },
        'canMarkPaid': can_mark_paid,
        'canFinalize': can_finalize,
        'paidRequestedAt': _clean_str(doc.get('paidRequestedAt')),
        'completedAt': _clean_str(doc.get('completedAt')),
    }


def _admin_note_payments_refresh_rows_from_liturgies(doc, tzinfo):
    period = doc.get('period') if isinstance(doc.get('period'), dict) else {}
    year = int(period.get('year') or 0)
    month = int(period.get('month') or 0)
    if year < 1900 or month < 1 or month > 12:
        return
    start_date, end_date = _admin_note_payments_month_range(year, month)
    church_docs, sums_by_church_id = _admin_note_payments_build_church_maps_for_period(start_date, end_date, tzinfo)
    church_by_id = {str(doc_item.get('_id')): doc_item for doc_item in church_docs}

    rows = doc.get('rows') if isinstance(doc.get('rows'), list) else []
    for row in rows:
        church_id = _clean_str(row.get('churchId'))
        row['sumAmount'] = int(sums_by_church_id.get(church_id, 0))
        church_doc = church_by_id.get(church_id)
        if church_doc:
            display = _admin_notes_church_display(church_doc)
            row['churchName'] = display.get('name', '')
            row['churchAddress'] = display.get('address', '')
            telegram_chat_ids = _clean_telegram_chat_ids(church_doc.get('telegramChatIds'))
            legacy_chat_id = _clean_str(church_doc.get('telegramChatId'))
            if legacy_chat_id and legacy_chat_id not in telegram_chat_ids:
                telegram_chat_ids.append(legacy_chat_id)
            row['telegramChatIds'] = telegram_chat_ids
            row['telegramChatId'] = telegram_chat_ids[0] if telegram_chat_ids else legacy_chat_id


def _admin_note_payments_row_chat_ids(row):
    chat_ids = _clean_telegram_chat_ids(row.get('telegramChatIds'))
    legacy_chat_id = _clean_str(row.get('telegramChatId'))
    if legacy_chat_id and legacy_chat_id not in chat_ids:
        chat_ids.append(legacy_chat_id)
    return chat_ids


def _admin_note_payments_send_telegram_message(chat_id, text, reply_markup=None):
    if not TELEGRAM_BOT_TOKEN:
        return False, 'TELEGRAM_BOT_TOKEN is not configured'
    if not chat_id:
        return False, 'telegramChatId is empty'

    endpoint = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    payload = {'chat_id': chat_id, 'text': text}
    if isinstance(reply_markup, dict) and reply_markup:
        payload['reply_markup'] = reply_markup
    try:
        response = requests.post(endpoint, json=payload, timeout=8)
        if response.ok:
            return True, ''
        return False, f'Telegram API error ({response.status_code})'
    except requests.RequestException as exc:
        return False, str(exc)


def _admin_note_payments_build_confirmation_message(doc, row, token):
    yes_callback = f"payconf:{token}:yes"
    no_callback = f"payconf:{token}:no"
    text = (
        f"Проплата за {_clean_str((doc.get('period') or {}).get('label'))}: "
        f"{int(row.get('sumAmount') or 0)} грн.\n"
        f"Церква: {_clean_str(row.get('churchName'))}\n"
        "Підтвердити отримання:"
    )
    reply_markup = {
        'inline_keyboard': [[
            {'text': 'Так', 'callback_data': yes_callback},
            {'text': 'Ні', 'callback_data': no_callback},
        ]]
    }
    return text, reply_markup


def _admin_note_payments_build_resolved_message(doc, row, answer):
    status_label = 'Підтверджено' if answer == 'yes' else 'Відхилено'
    return (
        f"Проплата за {_clean_str((doc.get('period') or {}).get('label'))}: "
        f"{int(row.get('sumAmount') or 0)} грн.\n"
        f"Церква: {_clean_str(row.get('churchName'))}\n"
        f"Статус: {status_label}"
    )


def _admin_note_payments_confirm_by_token(token, answer):
    cleaned_token = _clean_str(token)
    if not cleaned_token:
        raise ValueError('Invalid token')

    normalized_answer = _clean_str(answer).lower()
    if normalized_answer not in {'yes', 'no'}:
        raise ValueError('`answer` must be yes or no')

    doc = admin_note_payments_collection.find_one({'rows.confirmation.token': cleaned_token})
    if not doc:
        raise LookupError('Confirmation token not found')

    rows = doc.get('rows') if isinstance(doc.get('rows'), list) else []
    resolved_row = None
    for row in rows:
        confirmation = row.get('confirmation')
        if not isinstance(confirmation, dict):
            continue
        if _clean_str(confirmation.get('token')) != cleaned_token:
            continue
        confirmation['status'] = normalized_answer
        confirmation['respondedAt'] = datetime.utcnow().isoformat()
        confirmation['autoConfirmed'] = False
        resolved_row = row
        break

    if resolved_row is None:
        raise LookupError('Confirmation token not found')

    doc['rows'] = rows
    doc['status'] = _admin_note_payments_recompute_status(doc)
    doc['updatedAt'] = datetime.utcnow()
    admin_note_payments_collection.update_one(
        {'_id': doc.get('_id')},
        {'$set': {'rows': rows, 'status': doc.get('status'), 'updatedAt': doc.get('updatedAt')}}
    )

    return {
        'doc': doc,
        'row': resolved_row,
        'answer': normalized_answer,
        'status': doc.get('status'),
        'message_text': _admin_note_payments_build_resolved_message(doc, resolved_row, normalized_answer),
    }


@application.route('/api/admin/notes/payments', methods=['GET'])
def admin_note_payments_list():
    year_raw = _clean_str(request.args.get('year'))
    query = {}
    if year_raw:
        try:
            year = int(year_raw)
        except Exception:
            abort(400, description='`year` must be an integer')
        query['period.year'] = year

    docs = list(admin_note_payments_collection.find(query).sort([('period.year', -1), ('period.month', -1)]))
    out = []
    for doc in docs:
        if _admin_note_payments_apply_auto_confirm(doc):
            admin_note_payments_collection.update_one(
                {'_id': doc.get('_id')},
                {'$set': {'rows': doc.get('rows', []), 'status': doc.get('status'), 'updatedAt': doc.get('updatedAt')}}
            )
        out.append(_admin_note_payments_summary_output(doc))
    return jsonify({'months': out})


@application.route('/api/admin/notes/payments/create-previous-month', methods=['POST'])
def admin_note_payments_create_previous_month():
    now = datetime.utcnow()
    previous_month_anchor = now.replace(day=1) - timedelta(days=1)
    year = previous_month_anchor.year
    month = previous_month_anchor.month
    start_date, end_date = _admin_note_payments_month_range(year, month)
    if admin_note_payments_collection.find_one({'period.year': year, 'period.month': month}):
        abort(409, description='Payment for this month already exists')

    tzinfo = ZoneInfo('Europe/Kyiv') if ZoneInfo else timezone.utc
    church_docs, sums_by_church_id = _admin_note_payments_build_church_maps_for_period(start_date, end_date, tzinfo)

    rows = []
    for index, church_doc in enumerate(church_docs, start=1):
        display = _admin_notes_church_display(church_doc)
        telegram_chat_ids = _clean_telegram_chat_ids(church_doc.get('telegramChatIds'))
        legacy_chat_id = _clean_str(church_doc.get('telegramChatId'))
        if legacy_chat_id and legacy_chat_id not in telegram_chat_ids:
            telegram_chat_ids.append(legacy_chat_id)
        rows.append({
            'id': secrets.token_hex(8),
            'number': index,
            'churchId': str(church_doc.get('_id')),
            'churchName': display.get('name', ''),
            'churchAddress': display.get('address', ''),
            'telegramChatId': telegram_chat_ids[0] if telegram_chat_ids else legacy_chat_id,
            'telegramChatIds': telegram_chat_ids,
            'sumAmount': int(sums_by_church_id.get(str(church_doc.get('_id')), 0)),
            'receiptUrls': [],
            'receiptUrl': '',
            'receiptPreviewUrls': [],
            'receiptPreviewUrl': '',
            'paid': False,
            'paidAt': None,
            'unlocked': index == 1,
            'alert': False,
            'alertMessage': '',
            'unlockRequired': False,
            'confirmation': {
                'status': 'pending',
                'deadlineAt': None,
                'respondedAt': None,
                'token': '',
                'autoConfirmed': False,
            },
        })

    doc = {
        'period': {
            'year': year,
            'month': month,
            'label': _admin_note_payments_label(year, month),
            'startDate': start_date.isoformat(),
            'endDate': end_date.isoformat(),
        },
        'status': ADMIN_NOTE_PAYMENTS_STATUS_IN_PROGRESS,
        'rows': rows,
        'fopBalance': {
            'initial': ADMIN_NOTE_PAYMENTS_DEFAULT_BALANCE,
            'current': ADMIN_NOTE_PAYMENTS_DEFAULT_BALANCE,
            'lastCheckedAt': now.isoformat(),
        },
        'paidRequestedAt': None,
        'completedAt': None,
        'createdAt': now,
        'updatedAt': now,
    }
    inserted = admin_note_payments_collection.insert_one(doc)
    created = admin_note_payments_collection.find_one({'_id': inserted.inserted_id})
    return jsonify({'month': _admin_note_payments_summary_output(created)}), 201


@application.route('/api/admin/notes/payments/<string:payment_id>', methods=['GET'])
def admin_note_payments_get(payment_id):
    doc = _admin_note_payments_fetch_doc_or_404(payment_id)
    return jsonify({'month': _admin_note_payments_detail_output(doc)})


@application.route('/api/admin/notes/payments/<string:payment_id>/refresh-sums', methods=['POST'])
def admin_note_payments_refresh_sums(payment_id):
    doc = _admin_note_payments_fetch_doc_or_404(payment_id)
    tzinfo = ZoneInfo('Europe/Kyiv') if ZoneInfo else timezone.utc
    _admin_note_payments_refresh_rows_from_liturgies(doc, tzinfo)
    doc['updatedAt'] = datetime.utcnow()
    admin_note_payments_collection.update_one(
        {'_id': doc.get('_id')},
        {'$set': {'rows': doc.get('rows', []), 'updatedAt': doc.get('updatedAt')}}
    )
    return jsonify({'month': _admin_note_payments_detail_output(doc)})


def _admin_note_payments_find_row(doc, row_id):
    rows = doc.get('rows') if isinstance(doc.get('rows'), list) else []
    for index, row in enumerate(rows):
        if _clean_str(row.get('id')) == row_id:
            return rows, index, row
    abort(404, description='Payment row not found')


@application.route('/api/admin/notes/payments/<string:payment_id>/rows/<string:row_id>/mark-paid', methods=['POST'])
def admin_note_payments_mark_row_paid(payment_id, row_id):
    doc = _admin_note_payments_fetch_doc_or_404(payment_id)
    if _clean_str(doc.get('status')) == ADMIN_NOTE_PAYMENTS_STATUS_COMPLETED:
        abort(400, description='Completed month cannot be changed')

    payload = request.get_json(silent=True) or {}
    checked = payload.get('checked')
    checked = True if checked is None else bool(checked)
    if not checked:
        abort(400, description='Unchecking is not supported')

    rows, index, row = _admin_note_payments_find_row(doc, row_id)
    if not bool(row.get('unlocked')):
        abort(400, description='Row is locked')

    receipt_urls = []
    raw_receipt_urls = row.get('receiptUrls')
    if isinstance(raw_receipt_urls, list):
        for item in raw_receipt_urls:
            normalized_url = _clean_str(item)
            if normalized_url and normalized_url not in receipt_urls:
                receipt_urls.append(normalized_url)
    legacy_receipt_url = _clean_str(row.get('receiptUrl'))
    if legacy_receipt_url and legacy_receipt_url not in receipt_urls:
        receipt_urls.append(legacy_receipt_url)
    if not receipt_urls:
        abort(400, description='Receipt is required before marking row as paid')

    balance = doc.get('fopBalance') if isinstance(doc.get('fopBalance'), dict) else {}
    current_balance = int(balance.get('current') or 0)
    expected_balance = current_balance - int(row.get('sumAmount') or 0)
    provided_current_balance = payload.get('currentBalance')
    has_mismatch = False

    if provided_current_balance is not None and str(provided_current_balance).strip() != '':
        try:
            provided_numeric = int(round(float(provided_current_balance)))
        except Exception:
            abort(400, description='`currentBalance` must be numeric')
        balance['current'] = provided_numeric
        has_mismatch = provided_numeric != expected_balance
    else:
        balance['current'] = expected_balance

    balance['lastCheckedAt'] = datetime.utcnow().isoformat()
    row['paid'] = True
    row['paidAt'] = datetime.utcnow().isoformat()

    if has_mismatch:
        row['alert'] = True
        row['unlockRequired'] = True
        row['alertMessage'] = 'Баланс не відповідає очікуваному списанню'
    else:
        row['alert'] = False
        row['unlockRequired'] = False
        row['alertMessage'] = ''
        if index + 1 < len(rows):
            rows[index + 1]['unlocked'] = True

    doc['fopBalance'] = balance
    doc['status'] = _admin_note_payments_recompute_status(doc)
    doc['updatedAt'] = datetime.utcnow()
    admin_note_payments_collection.update_one(
        {'_id': doc.get('_id')},
        {'$set': {
            'rows': rows,
            'fopBalance': doc.get('fopBalance'),
            'status': doc.get('status'),
            'updatedAt': doc.get('updatedAt'),
        }}
    )
    return jsonify({'month': _admin_note_payments_detail_output(doc)})


@application.route('/api/admin/notes/payments/<string:payment_id>/rows/<string:row_id>/unlock', methods=['POST'])
def admin_note_payments_unlock_row(payment_id, row_id):
    doc = _admin_note_payments_fetch_doc_or_404(payment_id)
    rows, index, row = _admin_note_payments_find_row(doc, row_id)
    row['unlockRequired'] = False
    row['alert'] = False
    row['alertMessage'] = ''
    if index + 1 < len(rows):
        rows[index + 1]['unlocked'] = True
    doc['updatedAt'] = datetime.utcnow()
    admin_note_payments_collection.update_one(
        {'_id': doc.get('_id')},
        {'$set': {'rows': rows, 'updatedAt': doc.get('updatedAt')}}
    )
    return jsonify({'month': _admin_note_payments_detail_output(doc)})


@application.route('/api/admin/notes/payments/<string:payment_id>/rows/<string:row_id>/resend-confirmation', methods=['POST'])
def admin_note_payments_resend_row_confirmation(payment_id, row_id):
    doc = _admin_note_payments_fetch_doc_or_404(payment_id)
    rows, _, row = _admin_note_payments_find_row(doc, row_id)

    confirmation = row.get('confirmation')
    if not isinstance(confirmation, dict):
        confirmation = {}
        row['confirmation'] = confirmation

    status = _clean_str(confirmation.get('status')).lower() or 'pending'
    if status not in {'pending', 'no'}:
        abort(400, description='Confirmation can be resent only for no or pending status')

    token = _clean_str(confirmation.get('token'))
    if not token:
        token = secrets.token_urlsafe(20)
        confirmation['token'] = token

    now = datetime.utcnow()
    confirmation['deadlineAt'] = (now + timedelta(days=7)).isoformat()
    doc['updatedAt'] = now

    text, reply_markup = _admin_note_payments_build_confirmation_message(doc, row, token)
    telegram_errors = []
    chat_ids = _admin_note_payments_row_chat_ids(row)
    if not chat_ids:
        telegram_errors.append({
            'rowId': _clean_str(row.get('id')),
            'churchName': _clean_str(row.get('churchName')),
            'chatId': '',
            'error': 'telegramChatId is empty',
        })
    for chat_id in chat_ids:
        ok, error = _admin_note_payments_send_telegram_message(
            chat_id,
            text,
            reply_markup=reply_markup,
        )
        if not ok:
            telegram_errors.append({
                'rowId': _clean_str(row.get('id')),
                'churchName': _clean_str(row.get('churchName')),
                'chatId': chat_id,
                'error': error,
            })

    admin_note_payments_collection.update_one(
        {'_id': doc.get('_id')},
        {'$set': {'rows': rows, 'updatedAt': doc.get('updatedAt')}}
    )
    return jsonify({'month': _admin_note_payments_detail_output(doc), 'telegramErrors': telegram_errors})


@application.route('/api/admin/notes/payments/<string:payment_id>/rows/<string:row_id>/receipt', methods=['POST'])
def admin_note_payments_set_receipt(payment_id, row_id):
    doc = _admin_note_payments_fetch_doc_or_404(payment_id)
    payload = request.get_json(silent=True) or {}
    receipt_url = _clean_str(payload.get('receiptUrl'))
    if not receipt_url:
        abort(400, description='`receiptUrl` is required')
    rows, _, row = _admin_note_payments_find_row(doc, row_id)
    receipt_urls = []
    raw_receipt_urls = row.get('receiptUrls')
    if isinstance(raw_receipt_urls, list):
        for item in raw_receipt_urls:
            normalized_url = _clean_str(item)
            if normalized_url and normalized_url not in receipt_urls:
                receipt_urls.append(normalized_url)
    legacy_receipt_url = _clean_str(row.get('receiptUrl'))
    if legacy_receipt_url and legacy_receipt_url not in receipt_urls:
        receipt_urls.append(legacy_receipt_url)
    receipt_preview_urls = []
    raw_preview_urls = row.get('receiptPreviewUrls')
    if isinstance(raw_preview_urls, list):
        for item in raw_preview_urls:
            receipt_preview_urls.append(_clean_str(item))
    legacy_preview_url = _clean_str(row.get('receiptPreviewUrl'))
    if legacy_preview_url:
        if receipt_preview_urls:
            if not _clean_str(receipt_preview_urls[0]):
                receipt_preview_urls[0] = legacy_preview_url
        else:
            receipt_preview_urls.append(legacy_preview_url)
    while len(receipt_preview_urls) < len(receipt_urls):
        receipt_preview_urls.append('')
    if len(receipt_preview_urls) > len(receipt_urls):
        receipt_preview_urls = receipt_preview_urls[:len(receipt_urls)]
    if receipt_url not in receipt_urls:
        preview_url = _admin_note_payments_generate_and_upload_preview(receipt_url)
        if not preview_url:
            preview_url = _admin_note_payments_build_preview_url(receipt_url)
        receipt_urls.append(receipt_url)
        receipt_preview_urls.append(preview_url)
    row['receiptUrls'] = receipt_urls
    row['receiptUrl'] = receipt_urls[0] if receipt_urls else ''
    row['receiptPreviewUrls'] = receipt_preview_urls
    row['receiptPreviewUrl'] = receipt_preview_urls[0] if receipt_preview_urls else ''
    doc['updatedAt'] = datetime.utcnow()
    admin_note_payments_collection.update_one(
        {'_id': doc.get('_id')},
        {'$set': {'rows': rows, 'updatedAt': doc.get('updatedAt')}}
    )
    return jsonify({'month': _admin_note_payments_detail_output(doc)})


@application.route('/api/admin/notes/payments/<string:payment_id>/rows/<string:row_id>/receipt', methods=['DELETE'])
def admin_note_payments_delete_receipt(payment_id, row_id):
    doc = _admin_note_payments_fetch_doc_or_404(payment_id)
    payload = request.get_json(silent=True) or {}
    receipt_url = _clean_str(payload.get('receiptUrl') or request.args.get('receiptUrl'))
    if not receipt_url:
        abort(400, description='`receiptUrl` is required')
    rows, _, row = _admin_note_payments_find_row(doc, row_id)
    receipt_urls = []
    raw_receipt_urls = row.get('receiptUrls')
    if isinstance(raw_receipt_urls, list):
        for item in raw_receipt_urls:
            normalized_url = _clean_str(item)
            if normalized_url and normalized_url not in receipt_urls:
                receipt_urls.append(normalized_url)
    legacy_receipt_url = _clean_str(row.get('receiptUrl'))
    if legacy_receipt_url and legacy_receipt_url not in receipt_urls:
        receipt_urls.append(legacy_receipt_url)
    receipt_preview_urls = []
    raw_preview_urls = row.get('receiptPreviewUrls')
    if isinstance(raw_preview_urls, list):
        for item in raw_preview_urls:
            receipt_preview_urls.append(_clean_str(item))
    legacy_preview_url = _clean_str(row.get('receiptPreviewUrl'))
    if legacy_preview_url:
        if receipt_preview_urls:
            if not _clean_str(receipt_preview_urls[0]):
                receipt_preview_urls[0] = legacy_preview_url
        else:
            receipt_preview_urls.append(legacy_preview_url)
    while len(receipt_preview_urls) < len(receipt_urls):
        receipt_preview_urls.append('')
    if len(receipt_preview_urls) > len(receipt_urls):
        receipt_preview_urls = receipt_preview_urls[:len(receipt_urls)]

    if receipt_url not in receipt_urls:
        abort(404, description='Receipt file not found')

    deleted_preview_urls = [receipt_preview_urls[idx] for idx, url in enumerate(receipt_urls) if url == receipt_url]
    filtered_urls = [url for url in receipt_urls if url != receipt_url]
    filtered_preview_urls = [preview for idx, preview in enumerate(receipt_preview_urls) if receipt_urls[idx] != receipt_url]
    row['receiptUrls'] = filtered_urls
    row['receiptUrl'] = filtered_urls[0] if filtered_urls else ''
    row['receiptPreviewUrls'] = filtered_preview_urls
    row['receiptPreviewUrl'] = filtered_preview_urls[0] if filtered_preview_urls else ''
    for preview_url in deleted_preview_urls:
        _admin_note_payments_delete_preview_object(preview_url)
    doc['updatedAt'] = datetime.utcnow()
    admin_note_payments_collection.update_one(
        {'_id': doc.get('_id')},
        {'$set': {'rows': rows, 'updatedAt': doc.get('updatedAt')}}
    )
    return jsonify({'month': _admin_note_payments_detail_output(doc)})


@application.route('/api/admin/notes/payments/<string:payment_id>/mark-paid', methods=['POST'])
def admin_note_payments_mark_paid(payment_id):
    doc = _admin_note_payments_fetch_doc_or_404(payment_id)
    if _clean_str(doc.get('status')) != ADMIN_NOTE_PAYMENTS_STATUS_IN_PROGRESS:
        abort(400, description='Month can be marked paid only in in_progress status')
    rows = doc.get('rows') if isinstance(doc.get('rows'), list) else []
    if not rows or not all(bool(row.get('paid')) for row in rows):
        abort(400, description='All rows must be marked as paid')

    now = datetime.utcnow()
    telegram_errors = []
    for row in rows:
        confirmation = row.get('confirmation')
        if not isinstance(confirmation, dict):
            confirmation = {}
            row['confirmation'] = confirmation
        if not _clean_str(confirmation.get('token')):
            confirmation['token'] = secrets.token_urlsafe(20)
        confirmation['status'] = _clean_str(confirmation.get('status')).lower() or 'pending'
        confirmation['deadlineAt'] = (now + timedelta(days=7)).isoformat()
        confirmation['respondedAt'] = None
        confirmation['autoConfirmed'] = False

        text, reply_markup = _admin_note_payments_build_confirmation_message(
            doc,
            row,
            confirmation['token'],
        )
        chat_ids = _admin_note_payments_row_chat_ids(row)
        if not chat_ids:
            telegram_errors.append({
                'rowId': _clean_str(row.get('id')),
                'churchName': _clean_str(row.get('churchName')),
                'chatId': '',
                'error': 'telegramChatId is empty',
            })
        for chat_id in chat_ids:
            ok, error = _admin_note_payments_send_telegram_message(
                chat_id,
                text,
                reply_markup=reply_markup,
            )
            if not ok:
                telegram_errors.append({
                    'rowId': _clean_str(row.get('id')),
                    'churchName': _clean_str(row.get('churchName')),
                    'chatId': chat_id,
                    'error': error,
                })

    doc['paidRequestedAt'] = now.isoformat()
    doc['status'] = _admin_note_payments_recompute_status(doc)
    doc['updatedAt'] = now
    admin_note_payments_collection.update_one(
        {'_id': doc.get('_id')},
        {'$set': {
            'rows': rows,
            'paidRequestedAt': doc.get('paidRequestedAt'),
            'status': doc.get('status'),
            'updatedAt': doc.get('updatedAt'),
            'telegramErrors': telegram_errors,
        }}
    )
    return jsonify({'month': _admin_note_payments_detail_output(doc), 'telegramErrors': telegram_errors})


@application.route('/api/admin/notes/payments/confirm/<string:token>', methods=['GET', 'POST'])
def admin_note_payments_confirm(token):
    answer = _clean_str(request.args.get('answer'))
    if request.method == 'POST':
        payload = request.get_json(silent=True) or {}
        answer = _clean_str(payload.get('answer') or answer)
    try:
        result = _admin_note_payments_confirm_by_token(token, answer)
    except ValueError as exc:
        abort(400, description=str(exc))
    except LookupError as exc:
        abort(404, description=str(exc))
    return jsonify({'ok': True, 'status': result.get('status')})


@application.route('/api/admin/notes/payments/<string:payment_id>/finalize', methods=['POST'])
def admin_note_payments_finalize(payment_id):
    doc = _admin_note_payments_fetch_doc_or_404(payment_id)
    status = _admin_note_payments_recompute_status(doc)
    if status != ADMIN_NOTE_PAYMENTS_STATUS_FINALIZABLE:
        abort(400, description='Month cannot be finalized until all confirmations are "yes"')
    now = datetime.utcnow()
    doc['status'] = ADMIN_NOTE_PAYMENTS_STATUS_COMPLETED
    doc['completedAt'] = now.isoformat()
    doc['updatedAt'] = now
    admin_note_payments_collection.update_one(
        {'_id': doc.get('_id')},
        {'$set': {
            'status': doc.get('status'),
            'completedAt': doc.get('completedAt'),
            'updatedAt': doc.get('updatedAt'),
        }}
    )
    return jsonify({'month': _admin_note_payments_detail_output(doc)})


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


@application.route('/api/admin/ritual-services/<string:service_id>/password', methods=['GET'])
def admin_get_ritual_service_password(service_id):
    try:
        oid = ObjectId(service_id)
    except Exception:
        abort(400, description='Invalid ritual service id')

    ritual_service = ritual_services_collection.find_one({'_id': oid}, {'password': 1})
    if not ritual_service:
        abort(404, description='Ritual service not found')

    password = _clean_str(ritual_service.get('password'))
    if not password:
        abort(404, description='Password not found')

    return jsonify({
        'password': password,
    })


def _derive_ritual_hq_location_from_payload(payload, current_doc=None):
    current_doc = current_doc if isinstance(current_doc, dict) else {}

    service_area_ids = payload.get('serviceAreaIds')
    if not isinstance(service_area_ids, list):
        service_area_ids = current_doc.get('serviceAreaIds') if isinstance(current_doc.get('serviceAreaIds'), list) else []
    service_area_ids = [loc_clean_str(v) for v in service_area_ids if loc_clean_str(v)]

    settlements = payload.get('settlements')
    if not isinstance(settlements, list):
        settlements = current_doc.get('settlements') if isinstance(current_doc.get('settlements'), list) else []
    settlements = [_clean_str(v) for v in settlements if _clean_str(v)]

    contacts = payload.get('contacts')
    if not isinstance(contacts, list):
        contacts = current_doc.get('contacts') if isinstance(current_doc.get('contacts'), list) else []
    address_seed = ''
    if contacts:
        address_seed = _clean_str((contacts[0] or {}).get('address'))

    if not address_seed:
        legacy_address = payload.get('address')
        if not isinstance(legacy_address, list):
            legacy_address = current_doc.get('address') if isinstance(current_doc.get('address'), list) else []
        address_seed = _clean_str(legacy_address[0]) if legacy_address else ''

    lat_value = payload.get('latitude', current_doc.get('latitude'))
    lng_value = payload.get('longitude', current_doc.get('longitude'))

    seed = {}
    area_id = service_area_ids[0] if service_area_ids else ''
    area_display = settlements[0] if settlements else ''
    if area_id or area_display:
        seed['area'] = {
            'id': area_id,
            'display': area_display,
            'city': area_display,
            'source': 'manual',
        }
    if address_seed:
        seed['address'] = {
            'raw': address_seed,
            'display': address_seed,
        }
    lat_float = None
    lng_float = None
    try:
        lat_float = float(lat_value) if lat_value is not None and str(lat_value).strip() != '' else None
    except Exception:
        lat_float = None
    try:
        lng_float = float(lng_value) if lng_value is not None and str(lng_value).strip() != '' else None
    except Exception:
        lng_float = None
    if lat_float is not None and lng_float is not None:
        seed['geo'] = {
            'lat': lat_float,
            'lng': lng_float,
        }

    if not seed:
        return None
    location = normalize_location_core(seed)
    if not (location.get('area') or location.get('address') or location.get('geo')):
        return None
    return location


def _build_admin_ritual_service_payload(data, partial=False, current_doc=None):
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

    should_attempt_derive_hq = (not partial) or bool(
        {'serviceAreaIds', 'settlements', 'contacts', 'latitude', 'longitude'}.intersection(set(data.keys()))
    )
    if 'hqLocation' not in payload and should_attempt_derive_hq:
        derived_hq_location = _derive_ritual_hq_location_from_payload(payload, current_doc=current_doc)
        if derived_hq_location is not None:
            if _location_write_is_canonical() or _location_write_is_dual():
                payload['hqLocation'] = derived_hq_location
            if _location_write_is_legacy() or _location_write_is_dual():
                legacy = ritual_location_to_legacy(
                    derived_hq_location,
                    current_doc={'address': payload.get('address', [])}
                )
                if 'address' not in payload and isinstance(legacy.get('address'), list):
                    payload['address'] = legacy.get('address', [])
                if payload.get('latitude') is None:
                    payload['latitude'] = legacy.get('latitude')
                if payload.get('longitude') is None:
                    payload['longitude'] = legacy.get('longitude')

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


ADMIN_PREMIUM_QR_FIRMA_ALLOWED_FIELDS = {
    'name',
    'logo',
    'address',
    'website',
    'contacts',
    'notes',
    'status',
}


def _normalize_premium_qr_firma_status(value):
    normalized = _clean_str(value).lower()
    return 'inactive' if normalized in {'inactive', 'disabled', 'archived'} else 'active'


def _clean_premium_qr_firma_contacts(value):
    if value is None:
        return []
    if not isinstance(value, list):
        abort(400, description='`contacts` must be an array')

    contacts = []
    for item in value:
        if not isinstance(item, dict):
            continue
        name = _clean_str(item.get('name'))
        phone = _clean_str(item.get('phone'))
        if not name and not phone:
            continue
        contacts.append({
            'name': name,
            'phone': phone,
        })
    return contacts


def _build_admin_premium_qr_firma_payload(data, partial=False):
    if not isinstance(data, dict):
        abort(400, description='Body must be a JSON object')

    unknown = set(data.keys()) - ADMIN_PREMIUM_QR_FIRMA_ALLOWED_FIELDS
    if unknown:
        unknown_list = ', '.join(sorted(unknown))
        abort(400, description=f'Unsupported fields: {unknown_list}')

    payload = {}
    if not partial:
        name = _clean_str(data.get('name'))
        if not name:
            abort(400, description='`name` is required')

    if 'name' in data:
        name = _clean_str(data.get('name'))
        if not name:
            abort(400, description='`name` is required')
        payload['name'] = name

    if 'logo' in data:
        payload['logo'] = _clean_str(data.get('logo'))
    if 'address' in data:
        payload['address'] = _clean_str(data.get('address'))
    if 'website' in data:
        payload['website'] = _clean_str(data.get('website'))
    if 'notes' in data:
        payload['notes'] = _clean_str(data.get('notes'))
    if 'contacts' in data:
        payload['contacts'] = _clean_premium_qr_firma_contacts(data.get('contacts'))
    if 'status' in data:
        payload['status'] = _normalize_premium_qr_firma_status(data.get('status'))

    if not partial:
        payload.setdefault('logo', '')
        payload.setdefault('address', '')
        payload.setdefault('website', '')
        payload.setdefault('notes', '')
        payload.setdefault('contacts', [])
        payload.setdefault('status', 'active')

    return payload


def _normalize_admin_premium_qr_firma(doc):
    contacts_raw = doc.get('contacts')
    contacts = _clean_premium_qr_firma_contacts(contacts_raw if isinstance(contacts_raw, list) else [])
    return {
        'id': str(doc.get('_id')),
        'name': _clean_str(doc.get('name')),
        'logo': _clean_str(doc.get('logo')),
        'address': _clean_str(doc.get('address')),
        'website': _clean_str(doc.get('website')),
        'notes': _clean_str(doc.get('notes')),
        'status': _normalize_premium_qr_firma_status(doc.get('status')),
        'contacts': contacts,
        'createdAt': doc.get('createdAt').isoformat() if isinstance(doc.get('createdAt'), datetime) else None,
        'updatedAt': doc.get('updatedAt').isoformat() if isinstance(doc.get('updatedAt'), datetime) else None,
    }


@application.route('/api/admin/premium-qr-firmas', methods=['GET'])
def admin_list_premium_qr_firmas():
    search = _clean_str(request.args.get('search', ''))
    query_filter = {}
    if search:
        regex = {'$regex': re.escape(search), '$options': 'i'}
        query_filter = {
            '$or': [
                {'name': regex},
                {'address': regex},
                {'website': regex},
                {'contacts.name': regex},
                {'contacts.phone': regex},
            ]
        }

    cursor = premium_qr_firmas_collection.find(query_filter).sort([('updatedAt', -1), ('createdAt', -1), ('_id', -1)])
    items = [_normalize_admin_premium_qr_firma(item) for item in cursor]
    return jsonify({
        'total': len(items),
        'items': items,
    })


@application.route('/api/admin/premium-qr-firmas', methods=['POST'])
def admin_create_premium_qr_firma():
    data = request.get_json(silent=True) or {}
    payload = _build_admin_premium_qr_firma_payload(data, partial=False)
    now = datetime.utcnow()
    payload['createdAt'] = now
    payload['updatedAt'] = now

    inserted = premium_qr_firmas_collection.insert_one(payload)
    created = premium_qr_firmas_collection.find_one({'_id': inserted.inserted_id})
    return jsonify(_normalize_admin_premium_qr_firma(created)), 201


@application.route('/api/admin/premium-qr-firmas/<string:firma_id>', methods=['PATCH'])
def admin_update_premium_qr_firma(firma_id):
    try:
        oid = ObjectId(firma_id)
    except Exception:
        abort(400, description='Invalid premium qr firma id')

    existing = premium_qr_firmas_collection.find_one({'_id': oid})
    if not existing:
        abort(404, description='Premium QR firma not found')

    data = request.get_json(silent=True) or {}
    update_fields = _build_admin_premium_qr_firma_payload(data, partial=True)
    if not update_fields:
        abort(400, description='Nothing to update')

    update_fields['updatedAt'] = datetime.utcnow()
    premium_qr_firmas_collection.update_one({'_id': oid}, {'$set': update_fields})
    updated = premium_qr_firmas_collection.find_one({'_id': oid})
    return jsonify(_normalize_admin_premium_qr_firma(updated))


@application.route('/api/admin/premium-qr-firmas/<string:firma_id>', methods=['DELETE'])
def admin_delete_premium_qr_firma(firma_id):
    try:
        oid = ObjectId(firma_id)
    except Exception:
        abort(400, description='Invalid premium qr firma id')

    result = premium_qr_firmas_collection.delete_one({'_id': oid})
    if result.deleted_count == 0:
        abort(404, description='Premium QR firma not found')
    return jsonify({'ok': True})


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
    payload = _build_admin_ritual_service_payload(data, partial=False, current_doc=None)
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

    ritual_service = ritual_services_collection.find_one({'_id': oid})
    if not ritual_service:
        abort(404, description='Ritual service not found')

    data = request.get_json(silent=True) or {}
    update_fields = _build_admin_ritual_service_payload(data, partial=True, current_doc=ritual_service)
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

    def _church_page_payload(church):
        location = normalize_church_location(church)
        legacy_loc = church_location_to_legacy(location)
        contacts = _clean_contacts_list_loose(church.get('contacts'))
        first_contact = contacts[0] if contacts else {}

        image = _clean_str(church.get('image') or church.get('pageImage'))
        address = _clean_str(
            church.get('address')
            or legacy_loc.get("address")
            or ((location.get('address') or {}).get('display') if isinstance(location, dict) else '')
            or (location.get('display') if isinstance(location, dict) else '')
        )
        phone = _clean_str(church.get('phone')) or _clean_str(first_contact.get('phone'))
        description = _clean_str(church.get('description')) or _clean_str(church.get('infoNotes'))

        return {
            "id": str(church.get('_id')),
            "name": _clean_str(church.get('name')),
            "image": image,
            "address": address,
            "phone": phone,
            "description": description,
            "location": location if _location_read_uses_canonical() else None,
        }

    churches_list = [_church_page_payload(church) for church in churches_cursor]

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
    contacts = _clean_contacts_list_loose(church.get('contacts'))
    first_contact = contacts[0] if contacts else {}

    image = _clean_str(church.get('image') or church.get('pageImage'))
    address = _clean_str(
        church.get('address')
        or legacy_loc.get("address")
        or ((location.get('address') or {}).get('display') if isinstance(location, dict) else '')
        or (location.get('display') if isinstance(location, dict) else '')
    )
    phone = _clean_str(church.get('phone')) or _clean_str(first_contact.get('phone'))
    description = _clean_str(church.get('description')) or _clean_str(church.get('infoNotes'))

    return jsonify({
        "id": str(church.get('_id')),
        "name": _clean_str(church.get('name')),
        "image": image,
        "address": address,
        "phone": phone,
        "description": description,
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
            photon_base_url=PHOTON_BASE_URL,
            nominatim_base_url=NOMINATIM_BASE_URL,
            user_agent=GEOCODING_USER_AGENT,
            language=LOCATION_ACCEPT_LANGUAGE,
            country_codes=LOCATION_COUNTRY_CODES,
            timeout_ms=GEOCODING_TIMEOUT_MS,
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
        photon_base_url=PHOTON_BASE_URL,
        nominatim_base_url=NOMINATIM_BASE_URL,
        user_agent=GEOCODING_USER_AGENT,
        language=LOCATION_ACCEPT_LANGUAGE,
        country_codes=LOCATION_COUNTRY_CODES,
        timeout_ms=GEOCODING_TIMEOUT_MS,
        max_rows=10,
    )
    return jsonify({"total": len(items), "items": items})


@application.route('/api/location/addresses', methods=['GET'])
def location_addresses():
    search = request.args.get('search', '').strip()
    area_id = request.args.get('areaId', '').strip()
    city = request.args.get('city', '').strip()
    lat = request.args.get('lat')
    lng = request.args.get('lng')
    if not search:
        return jsonify({"total": 0, "items": []})
    items = search_location_addresses(
        search,
        photon_base_url=PHOTON_BASE_URL,
        nominatim_base_url=NOMINATIM_BASE_URL,
        user_agent=GEOCODING_USER_AGENT,
        language=LOCATION_ACCEPT_LANGUAGE,
        country_codes=LOCATION_COUNTRY_CODES,
        timeout_ms=GEOCODING_TIMEOUT_MS,
        max_rows=20,
        area_id=area_id,
        city=city,
        bias_lat=lat,
        bias_lng=lng,
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
    location = normalize_location_input(
        payload,
        photon_base_url=PHOTON_BASE_URL,
        nominatim_base_url=NOMINATIM_BASE_URL,
        user_agent=GEOCODING_USER_AGENT,
        language=LOCATION_ACCEPT_LANGUAGE,
        country_codes=LOCATION_COUNTRY_CODES,
        timeout_ms=GEOCODING_TIMEOUT_MS,
    )
    return jsonify({"location": location})


@application.route('/api/location/reverse', methods=['GET'])
def location_reverse():
    lat = request.args.get('lat')
    lng = request.args.get('lng')
    location = reverse_geocode_location(
        lat=lat,
        lng=lng,
        photon_base_url=PHOTON_BASE_URL,
        nominatim_base_url=NOMINATIM_BASE_URL,
        user_agent=GEOCODING_USER_AGENT,
        language=LOCATION_ACCEPT_LANGUAGE,
        country_codes=LOCATION_COUNTRY_CODES,
        timeout_ms=GEOCODING_TIMEOUT_MS,
    )
    if not location:
        return jsonify({"location": normalize_location_core({})})
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
        full_address = _clean_str(hq_location.get('display'))
        raw_address = ritual_service.get('address')
        if isinstance(raw_address, list):
            address_value = [_clean_str(item) for item in raw_address if _clean_str(item)]
            if full_address:
                if address_value:
                    address_value[0] = full_address
                else:
                    address_value = [full_address]
        else:
            address_value = full_address or raw_address or legacy_loc.get("address")
        ritual_services_list.append({
            "id": str(ritual_service.get('_id')),
            "name": ritual_service.get('name'),
            "address": address_value,
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
        full_address = _clean_str(hq_location.get('display'))
        raw_address = ritual_service.get('address')
        if isinstance(raw_address, list):
            address_value = [_clean_str(item) for item in raw_address if _clean_str(item)]
            if full_address:
                if address_value:
                    address_value[0] = full_address
                else:
                    address_value = [full_address]
        else:
            address_value = full_address or raw_address or legacy_loc.get("address")
        return jsonify({
            "id": str(ritual_service['_id']),
            "name": ritual_service.get('name'),
            "address": address_value,
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


@application.route('/api/admin/login', methods=['POST'])
def admin_login():
    if not ADMIN_LOGIN or not ADMIN_PASSWORD:
        abort(503, description='Admin auth is not configured')

    data = request.get_json(silent=True) or {}
    login = _admin_auth_str(data.get('login'))
    password = _admin_auth_str(data.get('password'))

    if not login or not password:
        abort(400, description='`login` and `password` are required')

    if login != ADMIN_LOGIN or password != ADMIN_PASSWORD:
        abort(401, description='Invalid login or password')

    expires_at = datetime.utcnow() + timedelta(seconds=ADMIN_JWT_EXP_DELTA_SECONDS)
    payload = {
        'role': 'admin',
        'login': ADMIN_LOGIN,
        'exp': expires_at,
    }
    token = jwt.encode(payload, ADMIN_JWT_SECRET, algorithm=JWT_ALGORITHM)
    if isinstance(token, bytes):
        token = token.decode('utf-8')

    return jsonify({
        'token': token,
        'expiresAt': expires_at.isoformat() + 'Z',
    })


@application.route('/api/admin/verify_token', methods=['POST'])
def admin_verify_token():
    data = request.get_json(silent=True) or {}
    token = _admin_auth_str(data.get('token'))
    if not token:
        abort(400, description='`token` is required')

    try:
        payload = jwt.decode(token, ADMIN_JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        abort(401, description='Token expired')
    except jwt.InvalidTokenError:
        abort(401, description='Invalid token')

    if payload.get('role') != 'admin':
        abort(401, description='Invalid token')

    return jsonify({'valid': True})


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


def _build_person_moderation_base_document(data):
    if not isinstance(data, dict):
        abort(400, description='JSON object expected')

    def _parse_iso_date(value, field_name):
        if not isinstance(value, str):
            return None
        raw = value.strip()
        if not raw:
            return None
        try:
            dt = datetime.strptime(raw, '%Y-%m-%d')
        except ValueError:
            abort(400, description=f'{field_name} must be a valid date in YYYY-MM-DD format')
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

    birth_date = _parse_iso_date(data.get('birthDate'), 'birthDate')
    death_date = _parse_iso_date(data.get('deathDate'), 'deathDate')

    birth_year = _normalize_year(data.get('birthYear'))
    death_year = _normalize_year(data.get('deathYear'))

    if birth_year is None and birth_date:
        birth_year = int(birth_date[:4])
    if death_year is None and death_date:
        death_year = int(death_date[:4])

    area = loc_clean_str(data.get('area'))
    area_id = loc_clean_str(data.get('areaId'))
    cemetery = loc_clean_str(data.get('cemetery'))

    incoming_burial = data.get('burial') if isinstance(data.get('burial'), dict) else None
    incoming_burial_location = incoming_burial.get('location') if isinstance((incoming_burial or {}).get('location'), dict) else {}
    incoming_burial_geo = incoming_burial_location.get('geo') if isinstance((incoming_burial_location or {}).get('geo'), dict) else {}
    incoming_geo_coords_text = loc_clean_str(incoming_burial_geo.get('coords'))
    incoming_geo_coordinates = incoming_burial_geo.get('coordinates') if isinstance(incoming_burial_geo.get('coordinates'), list) else []
    incoming_geo_coordinates_text = ''
    if len(incoming_geo_coordinates) == 2:
        try:
            incoming_geo_coordinates_text = f'{float(incoming_geo_coordinates[1]):.6f}, {float(incoming_geo_coordinates[0]):.6f}'
        except Exception:
            incoming_geo_coordinates_text = ''

    burial_site_coords = (
        loc_clean_str(data.get('burialSiteCoords'))
        or incoming_geo_coords_text
        or incoming_geo_coordinates_text
    )
    burial_site_photo_url = loc_clean_str(data.get('burialSitePhotoUrl'))
    burial_site_photo_urls = loc_clean_str_list(data.get('burialSitePhotoUrls'))
    if not burial_site_photo_urls and burial_site_photo_url:
        burial_site_photo_urls = [burial_site_photo_url]

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

    link = loc_clean_str(data.get('link'))
    achievements = loc_clean_str(data.get('achievements'))
    bio = loc_clean_str(data.get('bio'))
    notable = bool(data.get('notable')) or bool(link) or bool(achievements) or bool(bio)

    document = {
        'name': loc_clean_str(data.get('name')),
        'areaId': area_id or legacy.get('areaId', ''),
        'area': area or legacy.get('area', ''),
        'cemetery': cemetery or legacy.get('cemetery', ''),
        'location': legacy.get('location', []),
        'burialSiteCoords': burial_site_coords,
        'burialSitePhotoUrl': burial_site_photo_urls[0] if burial_site_photo_urls else '',
        'burialSitePhotoUrls': burial_site_photo_urls,
        'burialSitePhotoCount': len(burial_site_photo_urls),
        'burial': burial,
        'notable': notable,
        'link': link,
        'achievements': achievements,
        'bio': bio,
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

    return document


def _qr_activation_find_doc_or_404(code, expected_path_key=''):
    token = _qr_str(code)
    if not token:
        abort(404, description='QR code not found')

    doc = admin_qr_codes_collection.find_one({'qrToken': token})
    if not doc:
        abort(404, description='QR code not found')

    doc = _qr_ensure_doc_runtime_fields(doc, persist=True)
    path_key = _qr_str(doc.get('pathKey'))
    if expected_path_key and path_key != expected_path_key:
        abort(409, description='QR code path does not match activation page')

    wired_person_id = _qr_str(doc.get('wiredPersonId'))
    if bool(doc.get('isConnected') and wired_person_id):
        abort(409, description='QR code is already activated')

    return doc


@application.route('/api/people/add_moderation', methods=['POST'])
def people_add_moderation():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({'error': 'invalid_payload', 'message': 'JSON object expected'}), 400
    document = _build_person_moderation_base_document(data)

    people_moderation_collection.insert_one(document)

    return jsonify({'success': True})


@application.route('/api/qr/activation/<string:code>', methods=['GET'])
def public_qr_activation_meta(code):
    token = _qr_str(code)
    if not token:
        return jsonify({
            'found': False,
            'pathKey': '',
            'isWired': False,
            'wiredPersonId': '',
            'wiredQrCodeId': '',
            'qrDocId': '',
            'qrNumber': '',
            'scanUrl': '',
        })

    doc = admin_qr_codes_collection.find_one({'qrToken': token})
    if not doc:
        return jsonify({
            'found': False,
            'pathKey': '',
            'isWired': False,
            'wiredPersonId': '',
            'wiredQrCodeId': '',
            'qrDocId': '',
            'qrNumber': '',
            'scanUrl': '',
        })

    doc = _qr_ensure_doc_runtime_fields(doc, persist=True)
    wired_person_id = _qr_str(doc.get('wiredPersonId'))
    is_wired = bool(doc.get('isConnected') and wired_person_id)
    return jsonify({
        'found': True,
        'pathKey': _qr_str(doc.get('pathKey')),
        'isWired': is_wired,
        'wiredPersonId': wired_person_id,
        'wiredQrCodeId': _qr_str(doc.get('wiredQrCodeId')),
        'qrDocId': str(doc.get('_id')) if doc.get('_id') else '',
        'qrNumber': _qr_str(doc.get('qrNumber')),
        'scanUrl': _qr_str(doc.get('scanUrl')),
    })


@application.route('/api/qr/activation/premium_qr_firma', methods=['POST'])
def public_qr_activation_premium_qr_firma():
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        abort(400, description='JSON object expected')

    qr_code = _qr_str(data.get('qrCode'))
    qr_doc = _qr_activation_find_doc_or_404(qr_code, 'premium_qr_firma')

    base_document = _build_person_moderation_base_document(data)
    company_name = loc_clean_str(data.get('companyName'))
    website_url = loc_clean_str(data.get('websiteUrl'))
    phone = loc_clean_str(data.get('phone'))

    if not company_name:
        abort(400, description='`companyName` is required')
    if not website_url:
        abort(400, description='`websiteUrl` is required')
    if not phone:
        abort(400, description='`phone` is required')

    now = datetime.utcnow()
    generated_password = _generate_premium_order_password()
    birth_year = base_document.get('birthYear')
    death_year = base_document.get('deathYear')
    life_range = ''
    if birth_year and death_year:
        life_range = f'{birth_year}-{death_year}'
    elif birth_year:
        life_range = f'{birth_year}-'
    elif death_year:
        life_range = f'-{death_year}'

    order_doc = {
        'personId': '',
        'personIds': [],
        'personName': _qr_str(base_document.get('name')),
        'personNames': [_qr_str(base_document.get('name'))] if _qr_str(base_document.get('name')) else [],
        'lifeRange': life_range,
        'cemetery': _qr_str(base_document.get('cemetery')),
        'cemeteryAddress': _qr_str(base_document.get('area')),
        'avatarUrl': '',
        'pathType': QR_PATH_LABELS.get('premium_qr_firma', 'Преміум QR | Фірма'),
        'status': 'on_moderation',
        'customerName': company_name,
        'customerPhone': phone,
        'customerEmail': '',
        'companyName': company_name,
        'websiteUrl': website_url,
        'paymentMethod': 'moderation',
        'paymentStatus': 'pending',
        'invoiceId': '',
        'certificates': [],
        'ttn': '',
        'generatedPassword': generated_password,
        'adminNotes': '',
        'sourceType': 'qr_activation_premium_qr_firma',
        'activationType': 'premium_qr_firma',
        'qrCode': qr_code,
        'qrPathKey': 'premium_qr_firma',
        'qrDocId': str(qr_doc.get('_id')) if qr_doc.get('_id') else '',
        'qrNumber': _qr_str(qr_doc.get('qrNumber')),
        'wiredQrCodeId': _qr_str(qr_doc.get('wiredQrCodeId')),
        'moderationPayload': base_document,
        'createdAt': now,
        'updatedAt': now,
    }

    premium_orders_collection.insert_one(order_doc)
    return jsonify({'success': True})


@application.route('/api/qr/activation/plaques', methods=['POST'])
def public_qr_activation_plaques():
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        abort(400, description='JSON object expected')

    qr_code = _qr_str(data.get('qrCode'))
    qr_doc = _qr_activation_find_doc_or_404(qr_code, 'plaques')

    base_document = _build_person_moderation_base_document(data)
    phone = loc_clean_str(data.get('phone'))
    if not phone:
        abort(400, description='`phone` is required')

    now = datetime.utcnow()
    document = {
        **base_document,
        'status': 'on_moderation',
        'sourceType': 'qr_activation_plaques',
        'activationType': 'plaques',
        'phone': phone,
        'qrCode': qr_code,
        'qrPathKey': 'plaques',
        'qrDocId': str(qr_doc.get('_id')) if qr_doc.get('_id') else '',
        'qrNumber': _qr_str(qr_doc.get('qrNumber')),
        'wiredQrCodeId': _qr_str(qr_doc.get('wiredQrCodeId')),
        'updatedAt': now,
        'createdAt': now,
    }

    insert_result = plaques_moderation_collection.insert_one(document)
    moderation_id = str(insert_result.inserted_id)

    payment_method_raw = _qr_str(data.get('paymentMethod')).lower()
    payment_method = payment_method_raw if payment_method_raw in {'online', 'cod', 'moderation'} else 'moderation'
    payment_status = _qr_str(data.get('paymentStatus'))
    if not payment_status:
        payment_status = 'pending' if payment_method == 'online' else ('cod' if payment_method == 'cod' else 'pending')

    plaques_order_doc = {
        'moderationId': moderation_id,
        'personId': '',
        'personIds': [],
        'personName': _qr_str(base_document.get('name')),
        'personNames': [_qr_str(base_document.get('name'))] if _qr_str(base_document.get('name')) else [],
        'lifeRange': _admin_plaques_life_range_text(base_document),
        'cemetery': _qr_str(base_document.get('cemetery')),
        'cemeteryAddress': _qr_str(base_document.get('area')),
        'customerPhone': phone,
        'paymentMethod': payment_method,
        'paymentStatus': payment_status,
        'invoiceId': _qr_str(data.get('invoiceId')),
        'webhookData': data.get('webhookData') if isinstance(data.get('webhookData'), dict) else {},
        'sourceType': 'qr_activation_plaques',
        'activationType': 'plaques',
        'status': 'on_moderation',
        'qrCode': qr_code,
        'qrPathKey': 'plaques',
        'qrDocId': str(qr_doc.get('_id')) if qr_doc.get('_id') else '',
        'qrNumber': _qr_str(qr_doc.get('qrNumber')),
        'wiredQrCodeId': _qr_str(qr_doc.get('wiredQrCodeId')),
        'createdAt': now,
        'updatedAt': now,
    }
    order_insert = plaques_orders_collection.insert_one(plaques_order_doc)
    return jsonify({'success': True, 'orderId': str(order_insert.inserted_id)})


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


PREMIUM_ORDER_ALLOWED_STATUSES = {
    'on_moderation',
    'sent',
    'received',
    'rejected',
    'refused',
}

QR_PATH_LABELS = {
    'premium_qr': 'Преміум QR',
    'premium_qr_firma': 'Преміум QR | Фірма',
    'plaques': 'Таблички',
}
QR_ALLOWED_PATH_KEYS = set(QR_PATH_LABELS.keys())
QR_EXPORT_TOKEN_TTL_SECONDS = 15 * 60
_qr_export_files = {}
_qr_export_files_lock = threading.Lock()


def _qr_str(value):
    if value is None:
        return ''
    return str(value).strip()


def _qr_parse_path_key(value, *, field_name='pathKey'):
    cleaned = _qr_str(value)
    if cleaned not in QR_ALLOWED_PATH_KEYS:
        abort(400, description=f'`{field_name}` must be one of: {", ".join(sorted(QR_ALLOWED_PATH_KEYS))}')
    return cleaned


def _qr_parse_positive_int(value, *, field_name='quantity', minimum=1, maximum=500):
    try:
        parsed = int(value)
    except Exception:
        abort(400, description=f'`{field_name}` must be a number')
    if parsed < minimum or parsed > maximum:
        abort(400, description=f'`{field_name}` must be between {minimum} and {maximum}')
    return parsed


def _qr_parse_bool(value):
    cleaned = _qr_str(value).lower()
    if cleaned in {'1', 'true', 'yes', 'y', 'on'}:
        return True
    if cleaned in {'0', 'false', 'no', 'n', 'off'}:
        return False
    return bool(value)


def _qr_ua_date_from_dt(value):
    if not isinstance(value, datetime):
        return ''
    return value.strftime('%d.%m.%Y')


def _qr_parse_ua_date(value):
    raw = _qr_str(value)
    if not raw:
        return None
    try:
        parsed = datetime.strptime(raw, '%d.%m.%Y')
    except Exception:
        abort(400, description='`date` must be in DD.MM.YYYY format')
    return parsed.date()


def _qr_seed_next():
    latest = admin_qr_codes_collection.find_one({}, {'qrNumber': 1}, sort=[('qrNumber', -1)])
    max_number = int(latest.get('qrNumber') or 0) if latest else 0
    return max_number + 1


def _qr_public_base_url():
    if PUBLIC_WEB_BASE_URL:
        return PUBLIC_WEB_BASE_URL.rstrip('/')
    try:
        host = _qr_str(request.host_url)
    except RuntimeError:
        host = ''
    return host.rstrip('/')


def _qr_build_scan_path(token):
    return f"/qr/{quote(_qr_str(token), safe='')}"


def _qr_build_scan_url(token):
    path = _qr_build_scan_path(token)
    base = _qr_public_base_url()
    return f'{base}{path}' if base else path


def _qr_build_image_url(scan_url):
    cleaned = _qr_str(scan_url)
    if not cleaned:
        return ''
    if segno is None:
        return ''

    out = io.BytesIO()
    qr = segno.make(cleaned, error='m')
    qr.save(out, kind='svg', scale=6, border=2, dark='#111111', light='#ffffff')
    svg_bytes = out.getvalue()
    encoded = base64.b64encode(svg_bytes).decode('ascii')
    return f'data:image/svg+xml;base64,{encoded}'


def _qr_build_svg_bytes(scan_url):
    cleaned = _qr_str(scan_url)
    if not cleaned or segno is None:
        return b''
    out = io.BytesIO()
    qr = segno.make(cleaned, error='m')
    qr.save(out, kind='svg', scale=6, border=2, dark='#111111', light='#ffffff')
    return out.getvalue()


def _qr_export_cleanup_expired_tokens(now_ts=None):
    ts = float(now_ts if now_ts is not None else time.time())
    with _qr_export_files_lock:
        stale = [token for token, payload in _qr_export_files.items() if float(payload.get('expiresAt') or 0) <= ts]
        for token in stale:
            _qr_export_files.pop(token, None)


def _qr_export_register_download(payload_bytes, file_name):
    _qr_export_cleanup_expired_tokens()
    token = secrets.token_urlsafe(24)
    expires_at = time.time() + QR_EXPORT_TOKEN_TTL_SECONDS
    with _qr_export_files_lock:
        _qr_export_files[token] = {
            'bytes': payload_bytes,
            'fileName': _qr_str(file_name) or 'qr-export.zip',
            'expiresAt': expires_at,
        }
    base = _qr_public_base_url()
    path = f"/api/admin/qr/export/download/{quote(token, safe='')}"
    return f'{base}{path}' if base else path


def _qr_generate_unique_token():
    for _ in range(12):
        token = secrets.token_urlsafe(9).rstrip('=')
        if not admin_qr_codes_collection.find_one({'qrToken': token}, {'_id': 1}):
            return token
    abort(500, description='Failed to allocate unique QR token')


def _qr_ensure_doc_runtime_fields(doc, *, persist=False):
    if not isinstance(doc, dict):
        return doc

    update_fields = {}
    token = _qr_str(doc.get('qrToken'))
    if not token:
        token = _qr_generate_unique_token()
        update_fields['qrToken'] = token

    scan_url = _qr_str(doc.get('scanUrl'))
    if not scan_url:
        scan_url = _qr_build_scan_url(token)
        update_fields['scanUrl'] = scan_url

    qr_image_url = _qr_str(doc.get('qrImageUrl'))
    if not qr_image_url:
        qr_image_url = _qr_build_image_url(scan_url)
        update_fields['qrImageUrl'] = qr_image_url

    wired_qr_code_id = _qr_str(doc.get('wiredQrCodeId'))
    if not wired_qr_code_id and doc.get('qrNumber') is not None:
        wired_qr_code_id = _qr_str(doc.get('qrNumber'))
        if wired_qr_code_id:
            update_fields['wiredQrCodeId'] = wired_qr_code_id

    if update_fields:
        update_fields['updatedAt'] = datetime.utcnow()
        doc.update(update_fields)
        if persist and doc.get('_id'):
            admin_qr_codes_collection.update_one({'_id': doc['_id']}, {'$set': update_fields})

    return doc


def _qr_summary_payload():
    summary_new = {
        key: {
            'pathKey': key,
            'pathLabel': QR_PATH_LABELS[key],
            'totalCount': 0,
            # TEMP_BACKEND_FALLBACK: UI shows green action by this field; mirror total for active rows.
            'printedCount': 0,
            'exported': False,
        }
        for key in QR_PATH_LABELS
    }
    summary_printed = {
        key: {
            'pathKey': key,
            'pathLabel': QR_PATH_LABELS[key],
            'totalCount': 0,
            'connectedCount': 0,
        }
        for key in QR_PATH_LABELS
    }

    for row in admin_qr_codes_collection.aggregate([
        {'$match': {'isPrinted': {'$ne': True}}},
        {'$group': {'_id': '$pathKey', 'total': {'$sum': 1}}},
    ]):
        path_key = _qr_str(row.get('_id'))
        if path_key not in summary_new:
            continue
        total = int(row.get('total') or 0)
        summary_new[path_key]['totalCount'] = total
        summary_new[path_key]['printedCount'] = total

    for row in admin_qr_codes_collection.aggregate([
        {'$match': {'isPrinted': True}},
        {
            '$group': {
                '_id': '$pathKey',
                'total': {'$sum': 1},
                'connected': {'$sum': {'$cond': [{'$eq': ['$isConnected', True]}, 1, 0]}},
            }
        },
    ]):
        path_key = _qr_str(row.get('_id'))
        if path_key not in summary_printed:
            continue
        summary_printed[path_key]['totalCount'] = int(row.get('total') or 0)
        summary_printed[path_key]['connectedCount'] = int(row.get('connected') or 0)

    exported_cursor = admin_qr_batches_collection.aggregate([
        {'$match': {'printedAt': None, 'exportedAt': {'$ne': None}}},
        {'$group': {'_id': '$pathKey'}},
    ])
    for row in exported_cursor:
        path_key = _qr_str(row.get('_id'))
        if path_key in summary_new:
            summary_new[path_key]['exported'] = True

    return {
        'new': [summary_new[key] for key in QR_PATH_LABELS],
        'printed': [summary_printed[key] for key in QR_PATH_LABELS],
        'seedNext': _qr_seed_next(),
    }


def _qr_detail_item_projection(doc, *, is_batch_main=False, batch_comment_text=''):
    doc = _qr_ensure_doc_runtime_fields(doc, persist=True)
    path_key = _qr_str(doc.get('pathKey'))
    is_connected = bool(doc.get('isConnected'))
    created_at = doc.get('createdAt')
    return {
        'id': str(doc.get('_id')),
        'batchId': str(doc.get('batchId')) if doc.get('batchId') else '',
        'pathKey': path_key,
        'pathLabel': QR_PATH_LABELS.get(path_key, ''),
        'qrNumber': int(doc.get('qrNumber') or 0),
        'status': 'Підключений' if is_connected else 'Не підключений',
        'isConnected': is_connected,
        'wiredPersonId': _qr_str(doc.get('wiredPersonId')),
        'wiredQrCodeId': _qr_str(doc.get('wiredQrCodeId')),
        'createdAt': _qr_ua_date_from_dt(created_at),
        'scanUrl': _qr_str(doc.get('scanUrl')),
        'qrImageUrl': _qr_str(doc.get('qrImageUrl')),
        'isPrinted': bool(doc.get('isPrinted')),
        'isBatchMain': bool(is_batch_main),
        'hasComment': bool(is_batch_main and _qr_str(batch_comment_text)),
        'commentText': _qr_str(batch_comment_text) if is_batch_main else '',
    }


@application.route('/api/admin/qr/summary', methods=['GET'])
def admin_qr_summary():
    return jsonify(_qr_summary_payload())


@application.route('/api/admin/qr/detail', methods=['GET'])
def admin_qr_detail():
    path_key = _qr_parse_path_key(request.args.get('pathKey'))
    subtab = _qr_str(request.args.get('subtab')).lower() or 'new'
    if subtab not in {'new', 'printed'}:
        abort(400, description='`subtab` must be `new` or `printed`')
    search = _qr_str(request.args.get('search')).lower()
    selected_date = _qr_parse_ua_date(request.args.get('date'))

    query = {'pathKey': path_key}
    if subtab == 'printed':
        query['isPrinted'] = True

    docs = list(admin_qr_codes_collection.find(query).sort([('createdAt', -1), ('qrNumber', -1)]))

    batch_max_qr = {}
    batch_ids = []
    for doc in docs:
        batch_id = doc.get('batchId')
        qr_number = int(doc.get('qrNumber') or 0)
        if not batch_id:
            continue
        if batch_id not in batch_max_qr or qr_number > batch_max_qr[batch_id]:
            batch_max_qr[batch_id] = qr_number
        batch_ids.append(batch_id)

    batch_comment_map = {}
    if batch_ids:
        unique_batch_ids = list({bid for bid in batch_ids if bid})
        batch_docs = admin_qr_batches_collection.find(
            {'_id': {'$in': unique_batch_ids}},
            {'commentText': 1},
        )
        for batch_doc in batch_docs:
            batch_comment_map[batch_doc.get('_id')] = _qr_str(batch_doc.get('commentText'))

    items = []
    for doc in docs:
        batch_id = doc.get('batchId')
        qr_number = int(doc.get('qrNumber') or 0)
        is_batch_main = True if not batch_id else qr_number == int(batch_max_qr.get(batch_id, qr_number))
        batch_comment_text = ''
        if batch_id:
            batch_comment_text = _qr_str(batch_comment_map.get(batch_id))
        items.append(
            _qr_detail_item_projection(
                doc,
                is_batch_main=is_batch_main,
                batch_comment_text=batch_comment_text,
            )
        )

    if selected_date is not None:
        selected_date_str = selected_date.strftime('%d.%m.%Y')
        items = [item for item in items if _qr_str(item.get('createdAt')) == selected_date_str]

    if search:
        def _match(item):
            return (
                search in _qr_str(item.get('qrNumber')).lower()
                or search in _qr_str(item.get('createdAt')).lower()
                or search in _qr_str(item.get('status')).lower()
                or search in _qr_str(item.get('pathLabel')).lower()
                or search in _qr_str(item.get('commentText')).lower()
            )
        items = [item for item in items if _match(item)]

    return jsonify({'total': len(items), 'items': items})


@application.route('/api/qr/resolve/<string:code>', methods=['GET'])
def public_qr_resolve(code):
    token = _qr_str(code)
    if not token:
        return jsonify({
            'found': False,
            'status': 'not_found',
            'pathKey': '',
            'isWired': False,
            'wiredPersonId': '',
            'wiredQrCodeId': '',
        })

    doc = admin_qr_codes_collection.find_one({'qrToken': token})
    if not doc:
        return jsonify({
            'found': False,
            'status': 'not_found',
            'pathKey': '',
            'isWired': False,
            'wiredPersonId': '',
            'wiredQrCodeId': '',
        })

    doc = _qr_ensure_doc_runtime_fields(doc, persist=True)
    wired_person_id = _qr_str(doc.get('wiredPersonId'))
    is_wired = bool(doc.get('isConnected') and wired_person_id)
    return jsonify({
        'found': True,
        'status': 'wired' if is_wired else 'not_wired',
        'pathKey': _qr_str(doc.get('pathKey')),
        'isWired': is_wired,
        'wiredPersonId': wired_person_id,
        'wiredQrCodeId': _qr_str(doc.get('wiredQrCodeId')),
        'scanUrl': _qr_str(doc.get('scanUrl')),
        'qrImageUrl': _qr_str(doc.get('qrImageUrl')),
    })


@application.route('/api/admin/qr/create', methods=['POST'])
def admin_qr_create():
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        abort(400, description='Request must be a JSON object')

    path_key = _qr_parse_path_key(data.get('pathKey'))
    quantity = _qr_parse_positive_int(data.get('quantity'), field_name='quantity')

    # Business rule: do not allow creating a new batch for the same path
    # while there are still "new" (not printed) QR codes for that path.
    has_unprinted_for_path = admin_qr_codes_collection.count_documents({
        'pathKey': path_key,
        'isPrinted': {'$ne': True},
    }, limit=1) > 0
    if has_unprinted_for_path:
        return jsonify({
            'error': 'path_has_unprinted_qr',
            'message': 'Для цього шляху вже є нові QR-коди. Спочатку позначте їх як "Надруковані".',
            'pathKey': path_key,
        }), 409

    now = datetime.utcnow()
    seed_start = _qr_seed_next()
    range_end = seed_start + quantity - 1

    batch_doc = {
        'pathKey': path_key,
        'pathLabel': QR_PATH_LABELS[path_key],
        'quantity': quantity,
        'rangeStart': seed_start,
        'rangeEnd': range_end,
        'commentText': '',
        'hasComment': False,
        'exportedAt': None,
        'printedAt': None,
        'createdAt': now,
        'updatedAt': now,
    }
    batch_result = admin_qr_batches_collection.insert_one(batch_doc)
    batch_id = batch_result.inserted_id

    codes = []
    for idx in range(quantity):
        qr_number = seed_start + idx
        qr_token = _qr_generate_unique_token()
        scan_url = _qr_build_scan_url(qr_token)
        codes.append({
            'batchId': batch_id,
            'pathKey': path_key,
            'pathLabel': QR_PATH_LABELS[path_key],
            'qrNumber': qr_number,
            'qrToken': qr_token,
            'scanUrl': scan_url,
            'qrImageUrl': _qr_build_image_url(scan_url),
            'status': 'disconnected',
            'isConnected': False,
            'wiredPersonId': '',
            'wiredQrCodeId': str(qr_number),
            'isPrinted': False,
            'printedAt': None,
            'exportedAt': None,
            'commentText': '',
            'hasComment': False,
            'createdAt': now,
            'updatedAt': now,
        })
    if codes:
        admin_qr_codes_collection.insert_many(codes)

    return jsonify({
        'batchId': str(batch_id),
        'createdCount': quantity,
        'rangeStart': seed_start,
        'rangeEnd': range_end,
        'seedNext': range_end + 1,
    }), 201


@application.route('/api/admin/qr/export', methods=['POST'])
def admin_qr_export():
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        abort(400, description='Request must be a JSON object')

    path_key = _qr_parse_path_key(data.get('pathKey'))
    scope = _qr_str(data.get('scope')).lower() or 'new'
    if scope not in {'new', 'detail'}:
        abort(400, description='`scope` must be `new` or `detail`')
    force = _qr_parse_bool(data.get('force'))

    active_batches = list(
        admin_qr_batches_collection.find({'pathKey': path_key, 'printedAt': None}).sort([('createdAt', -1), ('_id', -1)])
    )
    if not active_batches:
        return jsonify({'ok': True, 'alreadyExported': False, 'exportedAt': None, 'downloadUrl': '', 'fileName': ''})

    already_exported = any(isinstance(item.get('exportedAt'), datetime) for item in active_batches)
    if already_exported and not force:
        latest_export = max((item.get('exportedAt') for item in active_batches if isinstance(item.get('exportedAt'), datetime)), default=None)
        return jsonify({
            'ok': False,
            'alreadyExported': True,
            'exportedAt': latest_export.isoformat() if latest_export else None,
            'downloadUrl': '',
            'fileName': '',
        })

    codes_to_export = list(
        admin_qr_codes_collection.find(
            {'pathKey': path_key, 'isPrinted': {'$ne': True}},
            {'qrNumber': 1, 'scanUrl': 1, 'qrToken': 1},
        ).sort([('qrNumber', ASCENDING), ('_id', ASCENDING)])
    )
    zip_bytes = b''
    file_name = ''
    download_url = ''
    if codes_to_export:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as archive:
            for doc in codes_to_export:
                prepared = _qr_ensure_doc_runtime_fields(doc, persist=True)
                qr_number = int(prepared.get('qrNumber') or 0)
                scan_url = _qr_str(prepared.get('scanUrl'))
                if not scan_url:
                    scan_url = _qr_build_scan_url(prepared.get('qrToken'))
                svg_payload = _qr_build_svg_bytes(scan_url)
                if not svg_payload:
                    continue
                archive.writestr(f'qr-{qr_number}.svg', svg_payload)
        zip_bytes = zip_buffer.getvalue()
        file_name = f"qr-export-{path_key}-{datetime.utcnow().strftime('%d-%m-%Y')}.zip"
        if zip_bytes:
            download_url = _qr_export_register_download(zip_bytes, file_name)

    now = datetime.utcnow()
    batch_ids = [item.get('_id') for item in active_batches if item.get('_id')]
    admin_qr_batches_collection.update_many(
        {'_id': {'$in': batch_ids}},
        {'$set': {'exportedAt': now, 'updatedAt': now}},
    )
    admin_qr_codes_collection.update_many(
        {'batchId': {'$in': batch_ids}, 'isPrinted': {'$ne': True}},
        {'$set': {'exportedAt': now, 'updatedAt': now}},
    )
    return jsonify({
        'ok': True,
        'alreadyExported': already_exported,
        'exportedAt': now.isoformat(),
        'downloadUrl': download_url,
        'fileName': file_name,
    })


@application.route('/api/admin/qr/export/download/<string:token>', methods=['GET'])
def admin_qr_export_download(token):
    cleaned_token = _qr_str(token)
    if not cleaned_token:
        abort(404, description='Export file not found')

    _qr_export_cleanup_expired_tokens()
    with _qr_export_files_lock:
        payload = _qr_export_files.pop(cleaned_token, None)

    if not payload:
        abort(404, description='Export file expired or not found')

    content = payload.get('bytes') if isinstance(payload, dict) else b''
    if not isinstance(content, (bytes, bytearray)) or not content:
        abort(404, description='Export file is empty')

    filename = _qr_str(payload.get('fileName')) if isinstance(payload, dict) else ''
    if not filename:
        filename = 'qr-export.zip'

    return send_file(
        io.BytesIO(bytes(content)),
        mimetype='application/zip',
        as_attachment=True,
        download_name=filename,
        max_age=0,
    )


@application.route('/api/admin/qr/mark-printed', methods=['POST'])
def admin_qr_mark_printed():
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        abort(400, description='Request must be a JSON object')
    path_key = _qr_parse_path_key(data.get('pathKey'))
    now = datetime.utcnow()

    result = admin_qr_codes_collection.update_many(
        {'pathKey': path_key, 'isPrinted': {'$ne': True}},
        {'$set': {'isPrinted': True, 'printedAt': now, 'updatedAt': now}},
    )
    admin_qr_batches_collection.update_many(
        {'pathKey': path_key, 'printedAt': None},
        {'$set': {'printedAt': now, 'updatedAt': now}},
    )
    return jsonify({'updatedCount': int(result.modified_count or 0)})


@application.route('/api/admin/qr/batches/<string:batch_id>/comment', methods=['PATCH'])
def admin_qr_batch_comment(batch_id):
    try:
        oid = ObjectId(batch_id)
    except Exception:
        abort(400, description='Invalid batch id')

    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        abort(400, description='Request must be a JSON object')

    comment_text = _qr_str(data.get('commentText'))
    if not comment_text:
        abort(400, description='`commentText` is required')

    now = datetime.utcnow()
    batch_result = admin_qr_batches_collection.update_one(
        {'_id': oid},
        {'$set': {'commentText': comment_text, 'hasComment': True, 'updatedAt': now}},
    )
    if batch_result.matched_count == 0:
        abort(404, description='Batch not found')

    return jsonify({
        'batchId': str(oid),
        'commentText': comment_text,
        'updatedAt': now.isoformat(),
    })

PREMIUM_ORDER_TERMINAL_STATUSES = {'received', 'rejected', 'refused'}


def _parse_status_codes_env(raw, defaults):
    cleaned = '' if raw is None else str(raw).strip()
    if not cleaned:
        return set(defaults)
    return {part.strip() for part in cleaned.split(',') if part.strip()}


NP_TRACKING_DELIVERED_CODES = _parse_status_codes_env(os.getenv('NP_TRACKING_DELIVERED_CODES'), {'9'})
NP_TRACKING_REFUSED_CODES = _parse_status_codes_env(
    os.getenv('NP_TRACKING_REFUSED_CODES'),
    {'102', '103', '108'},
)


def _premium_order_str(value):
    if value is None:
        return ''
    return str(value).strip()


def _premium_split_comma_head_tail(value):
    cleaned = _premium_order_str(value)
    if not cleaned:
        return '', ''
    parts = [part.strip() for part in cleaned.split(',') if part and part.strip()]
    if not parts:
        return '', ''
    head = parts[0]
    tail = ', '.join(parts[1:]) if len(parts) > 1 else ''
    return head, tail


def _premium_has_street_hint(value):
    normalized = _premium_order_str(value).lower()
    if not normalized:
        return False
    markers = ('вул', 'вулиця', 'просп', 'пров', 'площа', 'буд', 'street', 'st.', 'ave', 'road')
    return any(marker in normalized for marker in markers)


def _premium_normalize_cemetery_and_address(cemetery_raw, address_raw):
    cemetery_name, cemetery_tail = _premium_split_comma_head_tail(cemetery_raw)
    explicit_address = _premium_order_str(address_raw)

    # Prefer address that includes street marker, fallback to any non-empty candidate.
    candidates = [explicit_address, cemetery_tail]
    address = ''
    for candidate in candidates:
        if _premium_has_street_hint(candidate):
            address = candidate
            break
    if not address:
        for candidate in candidates:
            cleaned = _premium_order_str(candidate)
            if cleaned:
                address = cleaned
                break

    return cemetery_name or _premium_order_str(cemetery_raw), address


def _premium_status_is_terminal(status):
    return _premium_order_str(status) in PREMIUM_ORDER_TERMINAL_STATUSES


def _premium_np_status_text(entry):
    if not isinstance(entry, dict):
        return ''
    for key in ('Status', 'status', 'StatusName', 'statusName'):
        value = _premium_order_str(entry.get(key))
        if value:
            return value
    return ''


def _premium_np_status_code(entry):
    if not isinstance(entry, dict):
        return ''
    for key in ('StatusCode', 'statusCode'):
        value = _premium_order_str(entry.get(key))
        if value:
            return value
    return ''


def _premium_np_is_delivered(status_text, status_code):
    if status_code and status_code in NP_TRACKING_DELIVERED_CODES:
        return True
    normalized = _premium_order_str(status_text).lower()
    return any(
        marker in normalized
        for marker in ('отримано', 'видано', 'доставлено', 'delivered')
    )


def _premium_np_is_refused(status_text, status_code):
    if status_code and status_code in NP_TRACKING_REFUSED_CODES:
        return True
    normalized = _premium_order_str(status_text).lower()
    return any(
        marker in normalized
        for marker in ('відмова', 'отказ', 'не забра', 'повернен', 'return to sender', 'refused')
    )


def _premium_np_check_ttn(ttn):
    cleaned_ttn = _premium_order_str(ttn)
    if not cleaned_ttn:
        return {
            'ok': False,
            'error': 'ТТН порожній',
            'statusText': '',
            'statusCode': '',
            'isDelivered': False,
            'isRefused': False,
        }

    payload = {
        'apiKey': NP_API_KEY,
        'modelName': 'TrackingDocumentGeneral',
        'calledMethod': 'getStatusDocuments',
        'methodProperties': {
            'Documents': [{'DocumentNumber': cleaned_ttn}],
        },
    }
    try:
        response = requests.post(NP_BASE_URL, json=payload, timeout=10)
        response.raise_for_status()
        body = response.json() or {}
    except Exception as exc:
        return {
            'ok': False,
            'error': str(exc),
            'statusText': '',
            'statusCode': '',
            'isDelivered': False,
            'isRefused': False,
        }

    data = body.get('data')
    if not isinstance(data, list) or not data:
        return {
            'ok': False,
            'error': 'Нова Пошта не повернула статус',
            'statusText': '',
            'statusCode': '',
            'isDelivered': False,
            'isRefused': False,
        }

    first = data[0] if isinstance(data[0], dict) else {}
    status_text = _premium_np_status_text(first)
    status_code = _premium_np_status_code(first)
    return {
        'ok': True,
        'error': '',
        'statusText': status_text,
        'statusCode': status_code,
        'isDelivered': _premium_np_is_delivered(status_text, status_code),
        'isRefused': _premium_np_is_refused(status_text, status_code),
    }


def _premium_parse_bool(value):
    normalized = _premium_order_str(value).lower()
    return normalized in {'1', 'true', 'yes', 'y', 'on'}


def _premium_tracking_cooldown_until(now):
    return now + timedelta(seconds=max(0, NP_TRACKING_COOLDOWN_SECONDS))


def _premium_tracking_transition_from_np(current_status, tracking_result):
    if _premium_status_is_terminal(current_status):
        return current_status

    if tracking_result.get('ok'):
        if tracking_result.get('isRefused'):
            return 'refused'
        if tracking_result.get('isDelivered'):
            return 'received'
    return current_status


def _premium_refresh_tracking_for_order(order_doc, *, force=False):
    now = datetime.utcnow()
    order_id = order_doc.get('_id')
    current_status = _premium_order_str(order_doc.get('status')) or 'on_moderation'
    ttn = _premium_order_str(order_doc.get('ttn'))

    if not ttn:
        return {
            'id': str(order_id),
            'checked': False,
            'updated': False,
            'status': current_status,
            'reason': 'missing_ttn',
        }

    if _premium_status_is_terminal(current_status):
        return {
            'id': str(order_id),
            'checked': False,
            'updated': False,
            'status': current_status,
            'reason': 'terminal',
        }

    if not force:
        next_check_at = order_doc.get('npNextCheckAt')
        if isinstance(next_check_at, datetime) and next_check_at > now:
            return {
                'id': str(order_id),
                'checked': False,
                'updated': False,
                'status': current_status,
                'reason': 'cooldown',
            }

    tracking_result = _premium_np_check_ttn(ttn)
    next_status = _premium_tracking_transition_from_np(current_status, tracking_result)

    set_fields = {
        'npStatusRaw': _premium_order_str(tracking_result.get('statusText')),
        'npStatusCode': _premium_order_str(tracking_result.get('statusCode')),
        'npLastCheckedAt': now,
        'npNextCheckAt': _premium_tracking_cooldown_until(now),
        'npCheckError': _premium_order_str(tracking_result.get('error')) if not tracking_result.get('ok') else '',
    }

    if next_status != current_status:
        set_fields['status'] = next_status
        set_fields['updatedAt'] = now
        if next_status == 'received':
            set_fields['npDeliveredAt'] = now
        if next_status == 'refused':
            set_fields['npRefusedAt'] = now

    premium_orders_collection.update_one({'_id': order_id}, {'$set': set_fields})
    return {
        'id': str(order_id),
        'checked': True,
        'updated': next_status != current_status,
        'status': next_status,
        'reason': '',
        'ok': bool(tracking_result.get('ok')),
        'error': _premium_order_str(tracking_result.get('error')),
    }


def _premium_order_status_label(status):
    mapping = {
        'on_moderation': 'На модерації',
        'sent': 'Надіслано',
        'received': 'Отримано',
        'rejected': 'Відхилено',
        'refused': 'Відмова',
    }
    cleaned = _premium_order_str(status)
    return mapping.get(cleaned, mapping['on_moderation'])


def _premium_normalize_match_text(value):
    cleaned = _premium_order_str(value).lower()
    if not cleaned:
        return ''
    return re.sub(r'\s+', ' ', cleaned).strip()


def _premium_extract_years_from_life_range(value):
    cleaned = _premium_order_str(value)
    if not cleaned:
        return None, None
    years = re.findall(r'(\d{4})', cleaned)
    if len(years) >= 2:
        try:
            return int(years[0]), int(years[1])
        except Exception:
            return None, None
    return None, None


def _premium_build_life_range_from_years(birth_year, death_year):
    if birth_year is not None and death_year is not None:
        return f'{birth_year}-{death_year}'
    if birth_year is not None:
        return f'{birth_year}-'
    if death_year is not None:
        return f'-{death_year}'
    return ''


def _premium_order_extract_match_target(doc):
    moderation_payload = doc.get('moderationPayload') if isinstance(doc.get('moderationPayload'), dict) else {}
    name = _premium_order_str(moderation_payload.get('name') or doc.get('personName'))
    cemetery = _premium_order_str(moderation_payload.get('cemetery') or doc.get('cemetery'))
    birth_year = _admin_moderation_parse_year(moderation_payload.get('birthYear'))
    death_year = _admin_moderation_parse_year(moderation_payload.get('deathYear'))
    if birth_year is None or death_year is None:
        parsed_birth_year, parsed_death_year = _premium_extract_years_from_life_range(
            moderation_payload.get('lifeRange') or doc.get('lifeRange')
        )
        if birth_year is None:
            birth_year = parsed_birth_year
        if death_year is None:
            death_year = parsed_death_year
    return {
        'name': name,
        'cemetery': cemetery,
        'birthYear': birth_year,
        'deathYear': death_year,
    }


def _premium_existing_person_candidate_projection(person_doc):
    item = _person_with_location_projection(person_doc)
    burial = normalize_person_burial(item)
    area = _premium_order_str(item.get('area') or (burial.get('location') or {}).get('display'))
    cemetery = _premium_order_str(item.get('cemetery') or (burial.get('cemeteryRef') or {}).get('name'))
    birth_year = _admin_moderation_parse_year(item.get('birthYear'))
    death_year = _admin_moderation_parse_year(item.get('deathYear'))
    life_range = ''
    if birth_year is not None and death_year is not None:
        life_range = f'{birth_year}-{death_year}'
    elif birth_year is not None:
        life_range = f'{birth_year}-'
    elif death_year is not None:
        life_range = f'-{death_year}'

    return {
        'id': str(item.get('_id')),
        'name': _premium_order_str(item.get('name')),
        'birthYear': birth_year,
        'deathYear': death_year,
        'lifeRange': life_range,
        'area': area,
        'cemetery': cemetery,
        'avatarUrl': _premium_order_str(item.get('avatarUrl') or item.get('portraitUrl')),
    }


def _premium_find_existing_person_candidate(order_doc):
    target = _premium_order_extract_match_target(order_doc)
    if (
        not target.get('name')
        or target.get('birthYear') is None
        or target.get('deathYear') is None
        or not target.get('cemetery')
    ):
        return None

    target_name = _premium_normalize_match_text(target.get('name'))
    target_cemetery = _premium_normalize_match_text(target.get('cemetery'))
    if not target_name or not target_cemetery:
        return None

    cursor = people_collection.find(
        {
            'birthYear': target.get('birthYear'),
            'deathYear': target.get('deathYear'),
        },
        {
            'name': 1,
            'birthYear': 1,
            'deathYear': 1,
            'area': 1,
            'cemetery': 1,
            'burial': 1,
            'avatarUrl': 1,
            'portraitUrl': 1,
            'createdAt': 1,
        },
    ).sort([('createdAt', -1), ('_id', -1)]).limit(60)

    for person_doc in cursor:
        item = _person_with_location_projection(person_doc)
        burial = normalize_person_burial(item)
        person_name = _premium_normalize_match_text(item.get('name'))
        person_cemetery = _premium_normalize_match_text(
            item.get('cemetery') or (burial.get('cemeteryRef') or {}).get('name')
        )
        if person_name == target_name and person_cemetery == target_cemetery:
            return _premium_existing_person_candidate_projection(person_doc)

    return None


def _premium_has_existing_person_match(order_doc):
    target = _premium_order_extract_match_target(order_doc)
    if (
        not target.get('name')
        or target.get('birthYear') is None
        or target.get('deathYear') is None
        or not target.get('cemetery')
    ):
        return False

    target_name = _premium_normalize_match_text(target.get('name'))
    target_cemetery = _premium_normalize_match_text(target.get('cemetery'))
    if not target_name or not target_cemetery:
        return False

    cursor = people_collection.find(
        {
            'birthYear': target.get('birthYear'),
            'deathYear': target.get('deathYear'),
        },
        {
            'name': 1,
            'cemetery': 1,
            'burial': 1,
        },
    ).sort([('createdAt', -1), ('_id', -1)]).limit(60)

    for person_doc in cursor:
        item = _person_with_location_projection(person_doc)
        burial = normalize_person_burial(item)
        person_name = _premium_normalize_match_text(item.get('name'))
        person_cemetery = _premium_normalize_match_text(
            item.get('cemetery') or (burial.get('cemeteryRef') or {}).get('name')
        )
        if person_name == target_name and person_cemetery == target_cemetery:
            return True

    return False


def _generate_premium_order_password(length=8):
    alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%'
    if length < 8:
        length = 8
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def _premium_order_projection(doc, include_existing_person_candidate=False):
    if not isinstance(doc, dict):
        return {}

    person_names = doc.get('personNames')
    if not isinstance(person_names, list):
        person_names = []
    person_names = [_premium_order_str(name) for name in person_names if _premium_order_str(name)]

    ttn = _premium_order_str(doc.get('ttn'))
    status = _premium_order_str(doc.get('status')) or 'on_moderation'
    if status not in PREMIUM_ORDER_ALLOWED_STATUSES:
        status = 'on_moderation'
    if status == 'sent' and not ttn:
        status = 'on_moderation'

    certificates = doc.get('certificates')
    if not isinstance(certificates, list):
        certificates = []
    normalized_certificates = []
    for certificate in certificates:
        cleaned_certificate = _premium_order_str(certificate)
        if cleaned_certificate:
            normalized_certificates.append(cleaned_certificate)

    cemetery_name, cemetery_address = _premium_normalize_cemetery_and_address(
        doc.get('cemetery'),
        doc.get('cemeteryAddress'),
    )
    activation_type = _premium_order_str(doc.get('activationType'))
    company_name = _premium_order_str(doc.get('companyName'))
    website_url = _premium_order_str(doc.get('websiteUrl'))
    premium_qr_firma_id = _premium_order_str(doc.get('premiumQrFirmaId'))
    merge_person_id = _premium_order_str(doc.get('mergePersonId'))
    moderation_payload = doc.get('moderationPayload') if isinstance(doc.get('moderationPayload'), dict) else None
    moderation_birth_date = _premium_order_str((moderation_payload or {}).get('birthDate'))
    moderation_death_date = _premium_order_str((moderation_payload or {}).get('deathDate'))
    moderation_birth_year = _admin_moderation_parse_year((moderation_payload or {}).get('birthYear'))
    moderation_death_year = _admin_moderation_parse_year((moderation_payload or {}).get('deathYear'))
    moderation_area = _premium_order_str((moderation_payload or {}).get('area')) or _premium_order_str(doc.get('cemeteryAddress'))
    moderation_area_id = _premium_order_str((moderation_payload or {}).get('areaId'))
    moderation_cemetery = _premium_order_str((moderation_payload or {}).get('cemetery')) or cemetery_name
    moderation_internet_links = _premium_order_str(
        (moderation_payload or {}).get('sourceLink')
        or (moderation_payload or {}).get('link')
        or (moderation_payload or {}).get('internetLinks')
    )
    moderation_achievements = _premium_order_str((moderation_payload or {}).get('bio') or (moderation_payload or {}).get('achievements'))
    moderation_notable = bool((moderation_payload or {}).get('notable')) or bool(moderation_internet_links) or bool(moderation_achievements)
    raw_burial_photo_urls = (moderation_payload or {}).get('burialSitePhotoUrls')
    burial_photo_urls = (
        [_premium_order_str(url) for url in raw_burial_photo_urls if _premium_order_str(url)]
        if isinstance(raw_burial_photo_urls, list)
        else []
    )
    burial_site_photo_url = _premium_order_str((moderation_payload or {}).get('burialSitePhotoUrl'))
    if burial_site_photo_url and burial_site_photo_url not in burial_photo_urls:
        burial_photo_urls.insert(0, burial_site_photo_url)
    elif not burial_site_photo_url and burial_photo_urls:
        burial_site_photo_url = burial_photo_urls[0]
    is_firma_activation = (
        activation_type == 'premium_qr_firma'
        or _premium_order_str(doc.get('qrPathKey')) == 'premium_qr_firma'
        or _premium_order_str(doc.get('pathType')).lower() == 'преміум qr | фірма'
    )
    existing_person_candidate = (
        _premium_find_existing_person_candidate(doc)
        if is_firma_activation and include_existing_person_candidate
        else None
    )
    has_existing_person_match = (
        bool(existing_person_candidate)
        if include_existing_person_candidate
        else (_premium_has_existing_person_match(doc) if is_firma_activation else False)
    )

    return {
        'id': str(doc.get('_id')),
        'personId': _premium_order_str(doc.get('personId')),
        'personIds': [_premium_order_str(item) for item in (doc.get('personIds') or []) if _premium_order_str(item)],
        'personName': _premium_order_str(doc.get('personName')),
        'personNames': person_names,
        'lifeRange': _premium_order_str(doc.get('lifeRange')),
        'birthDate': moderation_birth_date,
        'deathDate': moderation_death_date,
        'birthYear': moderation_birth_year,
        'deathYear': moderation_death_year,
        'area': moderation_area,
        'areaId': moderation_area_id,
        'cemetery': cemetery_name,
        'cemeteryModeration': moderation_cemetery,
        'notable': moderation_notable,
        'internetLinks': moderation_internet_links,
        'sourceLink': moderation_internet_links,
        'achievements': moderation_achievements,
        'link': moderation_internet_links,
        'bio': moderation_achievements,
        'burialSiteCoords': _premium_order_str((moderation_payload or {}).get('burialSiteCoords')),
        'burialSitePhotoUrl': burial_site_photo_url,
        'burialSitePhotoUrls': burial_photo_urls,
        'address': cemetery_address,
        'avatarUrl': _premium_order_str(doc.get('avatarUrl') or doc.get('portraitUrl')),
        'pathType': _premium_order_str(doc.get('pathType')) or 'Сайт',
        'activationType': activation_type,
        'status': status,
        'statusLabel': _premium_order_status_label(status),
        'paymentMethod': _premium_order_str(doc.get('paymentMethod')),
        'paymentStatus': _premium_order_str(doc.get('paymentStatus')) or 'pending',
        'invoiceId': _premium_order_str(doc.get('invoiceId')),
        'customerName': _premium_order_str(doc.get('customerName')),
        'customerPhone': _premium_order_str(doc.get('customerPhone')),
        'customerEmail': _premium_order_str(doc.get('customerEmail')),
        'companyName': company_name,
        'websiteUrl': website_url,
        'premiumQrFirmaId': premium_qr_firma_id,
        'mergePersonId': merge_person_id,
        'mergeState': 'selected' if merge_person_id else 'none',
        'hasExistingPersonMatch': has_existing_person_match,
        'existingPersonCandidate': existing_person_candidate,
        'deliveryCityRef': _premium_order_str(doc.get('deliveryCityRef')),
        'deliveryCityName': _premium_order_str(doc.get('deliveryCityName')),
        'deliveryBranchRef': _premium_order_str(doc.get('deliveryBranchRef')),
        'deliveryBranchDesc': _premium_order_str(doc.get('deliveryBranchDesc')),
        'ttn': ttn,
        'adminNotes': _premium_order_str(doc.get('adminNotes')),
        'orderNumber': _premium_order_str(doc.get('orderNumber')),
        'wiredQrCodeId': _premium_order_str(doc.get('wiredQrCodeId')),
        'wiredQrCodeDocId': _premium_order_str(doc.get('wiredQrCodeDocId')),
        'wiredPersonId': _premium_order_str(doc.get('wiredPersonId')),
        'npStatusRaw': _premium_order_str(doc.get('npStatusRaw')),
        'npStatusCode': _premium_order_str(doc.get('npStatusCode')),
        'npCheckError': _premium_order_str(doc.get('npCheckError')),
        'npLastCheckedAt': doc.get('npLastCheckedAt'),
        'npNextCheckAt': doc.get('npNextCheckAt'),
        'npDeliveredAt': doc.get('npDeliveredAt'),
        'npRefusedAt': doc.get('npRefusedAt'),
        'certificates': normalized_certificates,
        'documentImageUrl': normalized_certificates[0] if normalized_certificates else '',
        'moderationPayload': moderation_payload,
        'createdAt': doc.get('createdAt'),
        'updatedAt': doc.get('updatedAt'),
        'activatedAt': doc.get('activatedAt'),
    }


@application.route('/api/premium-orders', methods=['POST'])
def create_premium_order():
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        abort(400, description='Request must be a JSON object')

    raw_person_ids = data.get('personIds')
    if raw_person_ids is None:
        single_person_id = _premium_order_str(data.get('personId'))
        raw_person_ids = [single_person_id] if single_person_id else []
    if not isinstance(raw_person_ids, list):
        abort(400, description='`personIds` must be an array')

    person_ids = [_premium_order_str(pid) for pid in raw_person_ids if _premium_order_str(pid)]
    if not person_ids:
        abort(400, description='`personIds` is required')

    customer_name = _premium_order_str(data.get('customerName') or data.get('name'))
    customer_phone = _premium_order_str(data.get('customerPhone') or data.get('phone'))
    customer_email = _premium_order_str(data.get('customerEmail') or data.get('email')).lower()
    city_ref = _premium_order_str(data.get('deliveryCityRef') or data.get('cityRef'))
    city_name = _premium_order_str(data.get('deliveryCityName') or data.get('cityName'))
    branch_ref = _premium_order_str(data.get('deliveryBranchRef') or data.get('branchRef'))
    branch_desc = _premium_order_str(data.get('deliveryBranchDesc') or data.get('branchDesc'))
    payment_method = _premium_order_str(data.get('paymentMethod')).lower()
    invoice_id = _premium_order_str(data.get('invoiceId'))
    path_type = _premium_order_str(data.get('pathType') or 'Сайт')

    if not customer_name:
        abort(400, description='`customerName` is required')
    if not customer_phone:
        abort(400, description='`customerPhone` is required')
    if not city_ref or not city_name:
        abort(400, description='Delivery city is required')
    if not branch_ref or not branch_desc:
        abort(400, description='Delivery branch is required')
    if payment_method not in {'online', 'cod'}:
        abort(400, description='`paymentMethod` must be `online` or `cod`')
    if payment_method == 'online' and not invoice_id:
        abort(400, description='`invoiceId` is required for online payments')

    person_docs = []
    for person_id in person_ids:
        try:
            oid = ObjectId(person_id)
        except Exception:
            abort(400, description='Invalid person id')
        person_doc = people_collection.find_one({'_id': oid})
        if not person_doc:
            abort(404, description='Person not found')
        person_docs.append(_person_with_location_projection(person_doc))

    raw_person_names = data.get('personNames')
    person_names = []
    if isinstance(raw_person_names, list):
        person_names = [_premium_order_str(name) for name in raw_person_names if _premium_order_str(name)]
    if not person_names:
        person_names = [_premium_order_str(person.get('name')) for person in person_docs if _premium_order_str(person.get('name'))]

    first_person = person_docs[0]
    normalized_cemetery, normalized_address = _premium_normalize_cemetery_and_address(
        first_person.get('cemetery'),
        first_person.get('area'),
    )
    life_range = ''
    birth_year = first_person.get('birthYear')
    death_year = first_person.get('deathYear')
    if birth_year and death_year:
        life_range = f'{birth_year}-{death_year}'
    elif birth_year:
        life_range = f'{birth_year}-'
    elif death_year:
        life_range = f'-{death_year}'

    certificates = data.get('certificates')
    if certificates is None:
        certificates = []
    if not isinstance(certificates, list):
        abort(400, description='`certificates` must be an array')
    normalized_certificates = []
    for certificate in certificates:
        cleaned_certificate = _premium_order_str(certificate)
        if not cleaned_certificate:
            abort(400, description='`certificates` contains empty value')
        normalized_certificates.append(cleaned_certificate)

    now = datetime.utcnow()
    generated_password = _generate_premium_order_password()
    order_doc = {
        'personId': person_ids[0],
        'personIds': person_ids,
        'personName': person_names[0] if person_names else _premium_order_str(first_person.get('name')),
        'personNames': person_names,
        'lifeRange': life_range,
        'cemetery': normalized_cemetery,
        'cemeteryAddress': normalized_address,
        'avatarUrl': _premium_order_str(first_person.get('avatarUrl') or first_person.get('portraitUrl')),
        'pathType': path_type or 'Сайт',
        'status': 'on_moderation',
        'customerName': customer_name,
        'customerPhone': customer_phone,
        'customerEmail': customer_email or '',
        'deliveryCityRef': city_ref,
        'deliveryCityName': city_name,
        'deliveryBranchRef': branch_ref,
        'deliveryBranchDesc': branch_desc,
        'paymentMethod': payment_method,
        'paymentStatus': 'pending' if payment_method == 'online' else 'cod',
        'invoiceId': invoice_id or '',
        'certificates': normalized_certificates,
        'ttn': '',
        'generatedPassword': generated_password,
        'adminNotes': '',
        'createdAt': now,
        'updatedAt': now,
    }

    try:
        result = premium_orders_collection.insert_one(order_doc)
    except Exception as exc:
        return jsonify({'error': 'DB insert failed', 'details': str(exc)}), 500

    return jsonify({
        'orderId': str(result.inserted_id),
        'status': 'on_moderation',
        'generatedPassword': generated_password,
        'paymentStatus': order_doc['paymentStatus'],
        'createdAt': now.isoformat(),
    }), 201


@application.route('/api/admin/premium-orders', methods=['GET'])
def admin_list_premium_orders():
    search = _premium_order_str(request.args.get('search'))
    status = _premium_order_str(request.args.get('status'))

    query = {}
    if status:
        query['status'] = status
    if search:
        pattern = {'$regex': re.escape(search), '$options': 'i'}
        query['$or'] = [
            {'personName': pattern},
            {'personNames': pattern},
            {'cemetery': pattern},
            {'cemeteryAddress': pattern},
            {'customerName': pattern},
            {'deliveryCityName': pattern},
            {'deliveryBranchDesc': pattern},
            {'ttn': pattern},
        ]

    docs = list(premium_orders_collection.find(query).sort([('createdAt', -1), ('_id', -1)]))
    items = [_premium_order_projection(doc) for doc in docs]
    return jsonify({'total': len(items), 'items': items})


@application.route('/api/admin/premium-orders/<string:order_id>', methods=['GET'])
def admin_get_premium_order(order_id):
    try:
        oid = ObjectId(order_id)
    except Exception:
        abort(400, description='Invalid order id')

    doc = premium_orders_collection.find_one({'_id': oid})
    if not doc:
        abort(404, description='Order not found')

    payload = _premium_order_projection(doc, include_existing_person_candidate=True)
    if not _premium_order_str(payload.get('avatarUrl')):
        person_id = _premium_order_str(doc.get('personId'))
        if not person_id:
            person_ids = doc.get('personIds')
            if isinstance(person_ids, list) and person_ids:
                person_id = _premium_order_str(person_ids[0])
        if person_id:
            try:
                person_doc = people_collection.find_one(
                    {'_id': ObjectId(person_id)},
                    {'avatarUrl': 1, 'portraitUrl': 1},
                )
            except Exception:
                person_doc = None
            if person_doc:
                payload['avatarUrl'] = _premium_order_str(person_doc.get('avatarUrl') or person_doc.get('portraitUrl'))

    return jsonify(payload)


@application.route('/api/admin/premium-orders/<string:order_id>/password', methods=['GET'])
def admin_get_premium_order_password(order_id):
    try:
        oid = ObjectId(order_id)
    except Exception:
        abort(400, description='Invalid order id')

    doc = premium_orders_collection.find_one({'_id': oid}, {'generatedPassword': 1})
    if not doc:
        abort(404, description='Order not found')

    generated_password = _premium_order_str(doc.get('generatedPassword'))
    if not generated_password:
        abort(404, description='Password not found')

    return jsonify({
        'generatedPassword': generated_password,
    })


@application.route('/api/admin/premium-orders/<string:order_id>', methods=['PATCH'])
def admin_update_premium_order(order_id):
    try:
        oid = ObjectId(order_id)
    except Exception:
        abort(400, description='Invalid order id')

    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        abort(400, description='Request must be a JSON object')

    current_doc = premium_orders_collection.find_one({'_id': oid})
    if not current_doc:
        abort(404, description='Order not found')
    current_activation_type = _premium_order_str(current_doc.get('activationType'))
    current_qr_path_key = _premium_order_str(current_doc.get('qrPathKey'))
    current_path_type = _premium_order_str(current_doc.get('pathType')).lower()
    is_premium_qr_firma_order = (
        current_activation_type == 'premium_qr_firma'
        or current_qr_path_key == 'premium_qr_firma'
        or current_path_type == 'преміум qr | фірма'
    )
    is_premium_qr_order = (
        is_premium_qr_firma_order
        or current_activation_type == 'premium_qr'
        or current_qr_path_key == 'premium_qr'
        or current_path_type == 'преміум qr'
    )

    current_status = _premium_order_str(current_doc.get('status')) or 'on_moderation'
    current_activated_at = current_doc.get('activatedAt')
    old_ttn = _premium_order_str(current_doc.get('ttn'))
    order_id_str = str(oid)
    update_fields = {}
    qr_reserve_target_doc_id = None
    qr_release_previous_doc_id = None
    has_explicit_status = 'status' in data
    current_moderation_payload = (
        dict(current_doc.get('moderationPayload'))
        if isinstance(current_doc.get('moderationPayload'), dict)
        else {}
    )
    next_moderation_payload = dict(current_moderation_payload)
    moderation_payload_touched = False

    if 'ttn' in data:
        next_ttn = _premium_order_str(data.get('ttn'))
        update_fields['ttn'] = next_ttn
        if next_ttn != old_ttn:
            update_fields['npStatusRaw'] = ''
            update_fields['npStatusCode'] = ''
            update_fields['npCheckError'] = ''
            update_fields['npLastCheckedAt'] = None
            update_fields['npNextCheckAt'] = None
            if not next_ttn:
                update_fields['npDeliveredAt'] = None
                update_fields['npRefusedAt'] = None

        if next_ttn and not has_explicit_status and not _premium_status_is_terminal(current_status):
            update_fields['status'] = 'sent'
        if not next_ttn and not has_explicit_status and not _premium_status_is_terminal(current_status):
            update_fields['status'] = 'on_moderation'

    if 'generatedPassword' in data:
        next_password = _premium_order_str(data.get('generatedPassword'))
        if not next_password:
            abort(400, description='`generatedPassword` cannot be empty')
        update_fields['generatedPassword'] = next_password

    if 'wiredQrCodeId' in data or 'qrNumber' in data:
        if current_activated_at:
            abort(409, description='Cannot rewire QR after activation')

        target_qr_number_raw = _premium_order_str(data.get('wiredQrCodeId') or data.get('qrNumber'))
        current_qr_doc_id = _premium_order_str(current_doc.get('wiredQrCodeDocId'))
        if current_qr_doc_id:
            try:
                qr_release_previous_doc_id = ObjectId(current_qr_doc_id)
            except Exception:
                qr_release_previous_doc_id = None

        if not target_qr_number_raw:
            update_fields['wiredQrCodeId'] = ''
            update_fields['wiredQrCodeDocId'] = ''
        else:
            try:
                target_qr_number = int(target_qr_number_raw)
            except Exception:
                abort(400, description='Invalid QR number')
            if target_qr_number <= 0:
                abort(400, description='Invalid QR number')

            target_qr_path_key = 'premium_qr_firma' if is_premium_qr_firma_order else 'premium_qr'
            qr_doc = admin_qr_codes_collection.find_one({'pathKey': target_qr_path_key, 'qrNumber': target_qr_number})
            if not qr_doc:
                abort(404, description='QR code not found')
            if bool(qr_doc.get('isConnected')):
                abort(409, description='QR code is already connected')

            reserved_order_id = _qr_str(qr_doc.get('reservedOrderId'))
            if reserved_order_id and reserved_order_id != order_id_str:
                abort(409, description='QR code is already reserved')

            qr_doc_id = qr_doc.get('_id')
            if not qr_doc_id:
                abort(409, description='QR code cannot be reserved')

            qr_reserve_target_doc_id = qr_doc_id
            effective_qr_code_id = _qr_str(qr_doc.get('wiredQrCodeId')) or _qr_str(qr_doc.get('qrNumber'))
            update_fields['wiredQrCodeId'] = effective_qr_code_id
            update_fields['wiredQrCodeDocId'] = str(qr_doc_id)

    if 'personName' in data:
        next_person_name = _premium_order_str(data.get('personName'))
        update_fields['personName'] = next_person_name
        update_fields['personNames'] = [next_person_name] if next_person_name else []
        next_moderation_payload['name'] = next_person_name
        moderation_payload_touched = True

    if 'birthDate' in data:
        raw_birth_date = _premium_order_str(data.get('birthDate'))
        next_birth_date = _admin_moderation_parse_iso_date(raw_birth_date)
        if raw_birth_date and not next_birth_date:
            abort(400, description='Invalid birthDate, expected YYYY-MM-DD')
        next_moderation_payload['birthDate'] = next_birth_date
        if 'birthYear' not in data:
            next_moderation_payload['birthYear'] = int(next_birth_date[:4]) if next_birth_date else None
        moderation_payload_touched = True

    if 'deathDate' in data:
        raw_death_date = _premium_order_str(data.get('deathDate'))
        next_death_date = _admin_moderation_parse_iso_date(raw_death_date)
        if raw_death_date and not next_death_date:
            abort(400, description='Invalid deathDate, expected YYYY-MM-DD')
        next_moderation_payload['deathDate'] = next_death_date
        if 'deathYear' not in data:
            next_moderation_payload['deathYear'] = int(next_death_date[:4]) if next_death_date else None
        moderation_payload_touched = True

    if 'birthYear' in data:
        parsed_birth_year = _admin_moderation_parse_year(data.get('birthYear'))
        raw_birth_year = _premium_order_str(data.get('birthYear'))
        if raw_birth_year and parsed_birth_year is None:
            abort(400, description='Invalid birthYear')
        next_moderation_payload['birthYear'] = parsed_birth_year
        moderation_payload_touched = True

    if 'deathYear' in data:
        parsed_death_year = _admin_moderation_parse_year(data.get('deathYear'))
        raw_death_year = _premium_order_str(data.get('deathYear'))
        if raw_death_year and parsed_death_year is None:
            abort(400, description='Invalid deathYear')
        next_moderation_payload['deathYear'] = parsed_death_year
        moderation_payload_touched = True

    if 'area' in data:
        next_area = _premium_order_str(data.get('area'))
        next_moderation_payload['area'] = next_area
        update_fields['cemeteryAddress'] = next_area
        moderation_payload_touched = True

    if 'areaId' in data:
        next_area_id = _premium_order_str(data.get('areaId'))
        next_moderation_payload['areaId'] = next_area_id
        moderation_payload_touched = True

    if 'cemetery' in data:
        next_cemetery = _premium_order_str(data.get('cemetery'))
        next_moderation_payload['cemetery'] = next_cemetery
        update_fields['cemetery'] = next_cemetery
        moderation_payload_touched = True

    if 'customerPhone' in data:
        next_customer_phone = _premium_order_str(data.get('customerPhone'))
        update_fields['customerPhone'] = next_customer_phone
        next_moderation_payload['phone'] = next_customer_phone
        moderation_payload_touched = True

    if is_premium_qr_order and any(key in data for key in ('internetLinks', 'sourceLink', 'link')):
        next_moderation_payload['sourceLink'] = _premium_order_str(
            data.get('internetLinks') or data.get('sourceLink') or data.get('link')
        )
        next_moderation_payload['link'] = _premium_order_str(
            data.get('internetLinks') or data.get('sourceLink') or data.get('link')
        )
        moderation_payload_touched = True

    if is_premium_qr_order and any(key in data for key in ('achievements', 'bio')):
        next_moderation_payload['bio'] = _premium_order_str(data.get('achievements') or data.get('bio'))
        moderation_payload_touched = True

    if is_premium_qr_order and any(key in data for key in ('notable', 'internetLinks', 'sourceLink', 'link', 'achievements', 'bio')):
        payload_link = _premium_order_str(
            next_moderation_payload.get('sourceLink')
            or next_moderation_payload.get('link')
            or next_moderation_payload.get('internetLinks')
        )
        payload_bio = _premium_order_str(next_moderation_payload.get('bio') or next_moderation_payload.get('achievements'))
        notable_flag = data.get('notable') if 'notable' in data else next_moderation_payload.get('notable')
        next_moderation_payload['notable'] = bool(notable_flag) or bool(payload_link) or bool(payload_bio)
        moderation_payload_touched = True

    if 'companyName' in data:
        update_fields['companyName'] = _premium_order_str(data.get('companyName'))

    if 'websiteUrl' in data:
        update_fields['websiteUrl'] = _premium_order_str(data.get('websiteUrl'))

    if 'premiumQrFirmaId' in data:
        premium_qr_firma_id = _premium_order_str(data.get('premiumQrFirmaId'))
        if premium_qr_firma_id:
            try:
                premium_qr_firma_oid = ObjectId(premium_qr_firma_id)
            except Exception:
                abort(400, description='Invalid premiumQrFirmaId')
            premium_qr_firma = premium_qr_firmas_collection.find_one({'_id': premium_qr_firma_oid})
            if not premium_qr_firma:
                abort(404, description='Premium QR firma not found')
            update_fields['premiumQrFirmaId'] = premium_qr_firma_id
            update_fields['companyName'] = _clean_str(premium_qr_firma.get('name'))
            update_fields['websiteUrl'] = _clean_str(premium_qr_firma.get('website'))
        else:
            update_fields['premiumQrFirmaId'] = ''
            update_fields['companyName'] = ''
            update_fields['websiteUrl'] = ''
    if 'mergePersonId' in data:
        merge_person_id = _premium_order_str(data.get('mergePersonId'))
        if merge_person_id:
            try:
                merge_person_oid = ObjectId(merge_person_id)
            except Exception:
                abort(400, description='Invalid mergePersonId')
            merge_person_doc = people_collection.find_one({'_id': merge_person_oid}, {'_id': 1})
            if not merge_person_doc:
                abort(404, description='Merge person not found')
            update_fields['mergePersonId'] = merge_person_id
            update_fields['personId'] = merge_person_id
            update_fields['personIds'] = [merge_person_id]
        else:
            update_fields['mergePersonId'] = ''

    if moderation_payload_touched:
        merged_birth_date = _admin_moderation_parse_iso_date(next_moderation_payload.get('birthDate'))
        merged_death_date = _admin_moderation_parse_iso_date(next_moderation_payload.get('deathDate'))
        merged_birth_year = _admin_moderation_parse_year(next_moderation_payload.get('birthYear'))
        merged_death_year = _admin_moderation_parse_year(next_moderation_payload.get('deathYear'))

        if merged_birth_year is None and merged_birth_date:
            merged_birth_year = int(merged_birth_date[:4])
        if merged_death_year is None and merged_death_date:
            merged_death_year = int(merged_death_date[:4])

        next_moderation_payload['birthDate'] = merged_birth_date
        next_moderation_payload['deathDate'] = merged_death_date
        next_moderation_payload['birthYear'] = merged_birth_year
        next_moderation_payload['deathYear'] = merged_death_year
        next_moderation_payload['lifeRange'] = _premium_build_life_range_from_years(
            merged_birth_year,
            merged_death_year,
        )
        update_fields['lifeRange'] = next_moderation_payload.get('lifeRange') or ''
        if 'cemeteryAddress' not in update_fields:
            update_fields['cemeteryAddress'] = _premium_order_str(next_moderation_payload.get('area'))
        if 'cemetery' not in update_fields:
            update_fields['cemetery'] = _premium_order_str(next_moderation_payload.get('cemetery'))
        update_fields['moderationPayload'] = next_moderation_payload
    if 'adminNotes' in data:
        update_fields['adminNotes'] = _premium_order_str(data.get('adminNotes'))
    if 'status' in data:
        next_status = _premium_order_str(data.get('status'))
        if next_status not in PREMIUM_ORDER_ALLOWED_STATUSES:
            abort(400, description='Invalid status')
        update_fields['status'] = next_status

    resulting_ttn = _premium_order_str(update_fields.get('ttn')) if 'ttn' in update_fields else old_ttn
    resulting_status = _premium_order_str(update_fields.get('status')) if 'status' in update_fields else current_status
    if resulting_status == 'sent' and not resulting_ttn:
        update_fields['status'] = 'on_moderation'

    if not update_fields:
        abort(400, description='No fields to update')

    now = datetime.utcnow()
    update_fields['updatedAt'] = now

    if qr_reserve_target_doc_id:
        reserve_result = admin_qr_codes_collection.update_one(
            {
                '_id': qr_reserve_target_doc_id,
                'isConnected': {'$ne': True},
                '$or': [
                    {'reservedOrderId': {'$exists': False}},
                    {'reservedOrderId': ''},
                    {'reservedOrderId': order_id_str},
                ],
            },
            {
                '$set': {
                    'reservedOrderId': order_id_str,
                    'reservedAt': now,
                    'updatedAt': now,
                }
            }
        )
        if reserve_result.matched_count == 0:
            abort(409, description='QR code is already reserved')

    premium_orders_collection.update_one({'_id': oid}, {'$set': update_fields})

    if qr_release_previous_doc_id and (not qr_reserve_target_doc_id or qr_release_previous_doc_id != qr_reserve_target_doc_id):
        admin_qr_codes_collection.update_one(
            {
                '_id': qr_release_previous_doc_id,
                'isConnected': {'$ne': True},
                'reservedOrderId': order_id_str,
            },
            {
                '$unset': {
                    'reservedOrderId': '',
                    'reservedAt': '',
                },
                '$set': {
                    'updatedAt': now,
                },
            }
        )

    doc = premium_orders_collection.find_one({'_id': oid})
    if isinstance(doc, dict):
        activation_type = _premium_order_str(doc.get('activationType'))
        qr_path_key = _premium_order_str(doc.get('qrPathKey'))
        path_type = _premium_order_str(doc.get('pathType')).lower()
        is_premium_qr_firma = activation_type == 'premium_qr_firma' or qr_path_key == 'premium_qr_firma'
        is_premium_qr = (
            is_premium_qr_firma
            or activation_type == 'premium_qr'
            or qr_path_key == 'premium_qr'
            or path_type == 'преміум qr'
        )
        should_sync_company = is_premium_qr_firma and any(
            key in data for key in ('companyName', 'premiumQrFirmaId', 'mergePersonId')
        )
        should_sync_notable = is_premium_qr and any(
            key in data for key in ('internetLinks', 'sourceLink', 'link', 'achievements', 'bio', 'notable')
        )
        if should_sync_company or should_sync_notable:
            company_name_for_people = _premium_order_str(doc.get('companyName'))
            moderation_payload = doc.get('moderationPayload') if isinstance(doc.get('moderationPayload'), dict) else {}
            notable_link_for_people = _premium_order_str(
                moderation_payload.get('sourceLink')
                or moderation_payload.get('link')
                or moderation_payload.get('internetLinks')
            )
            notable_bio_for_people = _premium_order_str(moderation_payload.get('bio') or moderation_payload.get('achievements'))
            notable_for_people = bool(moderation_payload.get('notable')) or bool(notable_link_for_people) or bool(notable_bio_for_people)
            person_ids_for_sync = doc.get('personIds') if isinstance(doc.get('personIds'), list) else []
            if not person_ids_for_sync:
                fallback_person_id = _premium_order_str(doc.get('personId'))
                if fallback_person_id:
                    person_ids_for_sync = [fallback_person_id]

            for raw_person_id in person_ids_for_sync:
                cleaned_person_id = _premium_order_str(raw_person_id)
                if not cleaned_person_id:
                    continue
                try:
                    person_oid = ObjectId(cleaned_person_id)
                except Exception:
                    continue

                set_payload = {'adminPage.updatedAt': datetime.utcnow()}
                unset_payload = {}
                if should_sync_company:
                    if company_name_for_people:
                        set_payload['companyName'] = company_name_for_people
                        set_payload['adminPage.companyName'] = company_name_for_people
                    else:
                        unset_payload['companyName'] = ''
                        unset_payload['adminPage.companyName'] = ''
                if should_sync_notable:
                    set_payload['notable'] = notable_for_people
                    if notable_link_for_people:
                        set_payload['sourceLink'] = notable_link_for_people
                        set_payload['link'] = notable_link_for_people
                    else:
                        unset_payload['sourceLink'] = ''
                        unset_payload['link'] = ''
                    if notable_bio_for_people:
                        set_payload['bio'] = notable_bio_for_people
                    else:
                        unset_payload['bio'] = ''

                update_doc = {'$set': set_payload}
                if unset_payload:
                    update_doc['$unset'] = unset_payload
                people_collection.update_one({'_id': person_oid}, update_doc)
    return jsonify(_premium_order_projection(doc, include_existing_person_candidate=True))


@application.route('/api/admin/premium-orders/<string:order_id>/refresh-tracking', methods=['POST'])
def admin_refresh_single_premium_order_tracking(order_id):
    try:
        oid = ObjectId(order_id)
    except Exception:
        abort(400, description='Invalid order id')

    body = request.get_json(silent=True) or {}
    if body is None:
        body = {}
    if not isinstance(body, dict):
        abort(400, description='Request must be a JSON object')

    force = _premium_parse_bool(body.get('force'))
    order_doc = premium_orders_collection.find_one({'_id': oid})
    if not order_doc:
        abort(404, description='Order not found')

    outcome = _premium_refresh_tracking_for_order(order_doc, force=force)
    latest = premium_orders_collection.find_one({'_id': oid}) or order_doc
    return jsonify({
        'item': _premium_order_projection(latest),
        'tracking': outcome,
    })


@application.route('/api/admin/premium-orders/refresh-tracking', methods=['POST'])
def admin_refresh_premium_orders_tracking():
    body = request.get_json(silent=True) or {}
    if body is None:
        body = {}
    if not isinstance(body, dict):
        abort(400, description='Request must be a JSON object')

    force = _premium_parse_bool(body.get('force'))
    requested_limit = body.get('limit')
    try:
        limit = int(requested_limit) if requested_limit is not None else NP_TRACKING_BATCH_LIMIT_DEFAULT
    except (TypeError, ValueError):
        abort(400, description='`limit` must be a number')

    limit = max(1, min(limit, NP_TRACKING_BATCH_LIMIT_MAX))
    now = datetime.utcnow()

    query = {
        'ttn': {'$nin': ['', None]},
        'status': {'$nin': list(PREMIUM_ORDER_TERMINAL_STATUSES)},
    }
    if not force:
        query['$or'] = [
            {'npNextCheckAt': {'$exists': False}},
            {'npNextCheckAt': None},
            {'npNextCheckAt': {'$lte': now}},
        ]

    docs = list(
        premium_orders_collection
        .find(query)
        .sort([('npLastCheckedAt', 1), ('createdAt', 1), ('_id', 1)])
        .limit(limit)
    )

    if not docs:
        return jsonify({
            'processed': 0,
            'checked': 0,
            'updated': 0,
            'received': 0,
            'refused': 0,
            'errors': 0,
            'cooldown': 0,
            'terminal': 0,
            'missingTtn': 0,
            'items': [],
        })

    max_workers = max(1, min(NP_TRACKING_BATCH_CONCURRENCY, len(docs)))
    outcomes = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_premium_refresh_tracking_for_order, doc, force=force) for doc in docs]
        for future in as_completed(futures):
            try:
                outcomes.append(future.result())
            except Exception as exc:
                outcomes.append({
                    'id': '',
                    'checked': False,
                    'updated': False,
                    'status': '',
                    'reason': 'error',
                    'ok': False,
                    'error': str(exc),
                })

    checked_count = sum(1 for item in outcomes if item.get('checked'))
    updated_count = sum(1 for item in outcomes if item.get('updated'))
    received_count = sum(1 for item in outcomes if item.get('status') == 'received')
    refused_count = sum(1 for item in outcomes if item.get('status') == 'refused')
    errors_count = sum(1 for item in outcomes if item.get('checked') and not item.get('ok'))
    cooldown_count = sum(1 for item in outcomes if item.get('reason') == 'cooldown')
    terminal_count = sum(1 for item in outcomes if item.get('reason') == 'terminal')
    missing_ttn_count = sum(1 for item in outcomes if item.get('reason') == 'missing_ttn')

    return jsonify({
        'processed': len(outcomes),
        'checked': checked_count,
        'updated': updated_count,
        'received': received_count,
        'refused': refused_count,
        'errors': errors_count,
        'cooldown': cooldown_count,
        'terminal': terminal_count,
        'missingTtn': missing_ttn_count,
        'items': outcomes,
    })


FINANCE_TABS = [
    {'id': 'main', 'label': 'Головна'},
    {'id': 'plaques', 'label': 'Таблички'},
    {'id': 'ritual_services', 'label': 'Ритуальні послуги'},
    {'id': 'notes', 'label': 'Електронні записки'},
    {'id': 'ads', 'label': 'Реклама'},
    {'id': 'premium_qr', 'label': 'Преміум QR'},
]
FINANCE_TAB_IDS = {item['id'] for item in FINANCE_TABS}
FINANCE_ADS_SURFACE_LABELS = {
    'columns': 'Стовпчики',
    'plaques': 'Таблички',
    'pages': 'Сторінка',
}


def _finance_tab_label(tab_id):
    cleaned = _clean_str(tab_id)
    for item in FINANCE_TABS:
        if item['id'] == cleaned:
            return item['label']
    return ''


def _finance_resolve_timezone(preferred_timezone=''):
    timezone_name = _clean_str(preferred_timezone) or _clean_str(DEFAULT_FINANCE_TIMEZONE) or 'Europe/Kyiv'
    alias_map = {'Europe/Kiev': 'Europe/Kyiv'}
    timezone_name = alias_map.get(timezone_name, timezone_name)
    if ZoneInfo is None:
        return timezone.utc, 'UTC'
    try:
        return ZoneInfo(timezone_name), timezone_name
    except Exception:
        return ZoneInfo('Europe/Kyiv'), 'Europe/Kyiv'


def _finance_parse_bool(value):
    normalized = _clean_str(value).lower()
    return normalized in {'1', 'true', 'yes', 'y', 'on'}


def _finance_parse_date_param(value, field_name):
    cleaned = _clean_str(value)
    if not cleaned:
        return None
    try:
        return datetime.strptime(cleaned, '%Y-%m-%d').date()
    except Exception:
        abort(400, description=f'`{field_name}` must be YYYY-MM-DD')


def _finance_parse_any_datetime(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            if cleaned.endswith('Z'):
                return datetime.fromisoformat(cleaned.replace('Z', '+00:00'))
            return datetime.fromisoformat(cleaned)
        except Exception:
            return None
    return None


def _finance_to_local_date(value, tzinfo):
    dt = _finance_parse_any_datetime(value)
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        return dt.astimezone(tzinfo).date()
    except Exception:
        return dt.date()


def _finance_is_in_period(local_date, date_from, date_to):
    if local_date is None:
        return False
    if date_from and local_date < date_from:
        return False
    if date_to and local_date > date_to:
        return False
    return True


def _finance_parse_webhook_amount_minor(raw_amount):
    if raw_amount is None:
        return None
    if isinstance(raw_amount, bool):
        return None
    if isinstance(raw_amount, (int, float)):
        try:
            return int(round(float(raw_amount)))
        except Exception:
            return None
    if isinstance(raw_amount, str):
        cleaned = raw_amount.strip().replace(' ', '')
        if not cleaned:
            return None
        cleaned = cleaned.replace(',', '.')
        try:
            return int(round(float(cleaned)))
        except Exception:
            return None
    return None


def _finance_extract_webhook_amount_uah(webhook_data):
    if not isinstance(webhook_data, dict):
        return None
    amount_minor = _finance_parse_webhook_amount_minor(webhook_data.get('amount'))
    if amount_minor is None:
        return None
    return int(round(amount_minor / 100.0))


def _finance_extract_webhook_datetime(webhook_data):
    if not isinstance(webhook_data, dict):
        return None
    for key in ('modifiedDate', 'createdDate', 'date'):
        parsed = _finance_parse_any_datetime(webhook_data.get(key))
        if parsed:
            return parsed
    return None


def _finance_datetime_sort_key(local_date, event_dt):
    if local_date:
        if event_dt and isinstance(event_dt, datetime):
            dt = event_dt
            if dt.tzinfo is not None:
                try:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                except Exception:
                    dt = dt.replace(tzinfo=None)
            return datetime.combine(local_date, dt.time())
        return datetime.combine(local_date, datetime.min.time())
    return datetime.min


def _finance_build_row(
    *,
    row_id='',
    tab_id='',
    category_label='',
    amount_uah=0,
    local_date=None,
    event_dt=None,
    source_name='',
    source_details='',
    source_avatar_url='',
    contact='',
    status='',
    path_label='',
    period_label='',
    receipt_available=False,
):
    safe_amount = max(int(amount_uah or 0), 0)
    date_label = local_date.strftime('%d.%m.%Y') if local_date else ''
    safe_row_id = _clean_str(row_id)
    can_open_receipt = bool(receipt_available and safe_row_id)
    return {
        'id': safe_row_id,
        'tab': _clean_str(tab_id),
        'tabLabel': _finance_tab_label(tab_id),
        'categoryLabel': _clean_str(category_label),
        'amountUah': safe_amount,
        'amountLabel': f'{safe_amount} грн',
        'eventDate': date_label,
        'sourceName': _clean_str(source_name),
        'sourceDetails': _clean_str(source_details),
        'sourceAvatarUrl': _clean_str(source_avatar_url),
        'contact': _clean_str(contact),
        'status': _clean_str(status),
        'pathLabel': _clean_str(path_label),
        'periodLabel': _clean_str(period_label),
        'receiptAvailable': can_open_receipt,
        'receiptOpenUrl': f"/api/admin/finance/receipt?rowId={quote(safe_row_id, safe='')}" if can_open_receipt else '',
        'sortKey': _finance_datetime_sort_key(local_date, event_dt).isoformat(),
    }


def _finance_extract_invoice_id(payload):
    if not isinstance(payload, dict):
        return ''
    webhook_data = payload.get('webhookData') if isinstance(payload.get('webhookData'), dict) else {}
    return _clean_str(webhook_data.get('invoiceId')) or _clean_str(payload.get('invoiceId'))


def _finance_parse_ads_period_end(item):
    if not isinstance(item, dict):
        return datetime.min
    end_iso = _clean_str(item.get('periodEnd'))
    if end_iso:
        try:
            return datetime.strptime(end_iso, '%Y-%m-%d')
        except Exception:
            pass
    start_iso = _clean_str(item.get('periodStart'))
    if start_iso:
        try:
            return datetime.strptime(start_iso, '%Y-%m-%d')
        except Exception:
            pass
    return datetime.min


def _finance_parse_ritual_period_end(item):
    if not isinstance(item, dict):
        return datetime.min
    parsed = _parse_ritual_period_str(item.get('period'))
    if parsed and isinstance(parsed.get('endDateObj'), datetime):
        return parsed.get('endDateObj')
    end_text = _clean_str(item.get('endDate')) or _clean_str(item.get('periodEnd'))
    if end_text:
        try:
            return datetime.strptime(end_text, '%d.%m.%Y')
        except Exception:
            pass
    start_text = _clean_str(item.get('startDate')) or _clean_str(item.get('periodStart'))
    if start_text:
        try:
            return datetime.strptime(start_text, '%d.%m.%Y')
        except Exception:
            pass
    return datetime.min


def _finance_pick_latest_ads_receipt_url(doc):
    if not isinstance(doc, dict):
        return ''
    payments = _ads_normalize_payments(doc.get('payments'))
    sorted_payments = sorted(
        payments,
        key=lambda item: (_finance_parse_ads_period_end(item), _clean_str(item.get('periodEnd')), _clean_str(item.get('periodStart'))),
        reverse=True,
    )
    for item in sorted_payments:
        pdf_url = _clean_str(item.get('pdfUrl'))
        if pdf_url:
            return pdf_url
    return _clean_str(doc.get('pdfUrl'))


def _finance_pick_latest_ritual_receipt_url(doc):
    if not isinstance(doc, dict):
        return ''
    payments = _clean_ritual_payments(doc.get('payments'))
    sorted_payments = sorted(
        payments,
        key=lambda item: (_finance_parse_ritual_period_end(item), _clean_str(item.get('endDate')), _clean_str(item.get('period'))),
        reverse=True,
    )
    for item in sorted_payments:
        pdf_url = _clean_str(item.get('pdfUrl'))
        if pdf_url:
            return pdf_url
    return ''


def _finance_fetch_monobank_receipt_pdf(invoice_id):
    cleaned_invoice_id = _clean_str(invoice_id)
    if not cleaned_invoice_id:
        return None
    token = _clean_str(os.getenv('MONOPAY_TOKEN'))
    if not token:
        return None

    response = None
    try:
        response = requests.get(
            'https://api.monobank.ua/api/merchant/invoice/receipt',
            headers={'X-Token': token},
            params={'invoiceId': cleaned_invoice_id},
            timeout=25,
        )
        response.raise_for_status()
    except Exception:
        return None

    content_type = _clean_str(response.headers.get('Content-Type')).lower()
    if 'application/pdf' in content_type and response.content:
        return response.content

    encoded = ''
    try:
        payload = response.json()
    except Exception:
        payload = None

    if isinstance(payload, dict):
        for key in ('receipt', 'content', 'file', 'pdf', 'data'):
            candidate = payload.get(key)
            if isinstance(candidate, str) and candidate.strip():
                encoded = candidate.strip()
                break
    elif isinstance(payload, str) and payload.strip():
        encoded = payload.strip()

    if not encoded:
        text = (response.text or '').strip()
        if text:
            encoded = text

    if not encoded:
        return None

    if encoded.startswith('data:'):
        comma_idx = encoded.find(',')
        if comma_idx >= 0:
            encoded = encoded[comma_idx + 1:]
    encoded = ''.join(encoded.split())
    if not encoded:
        return None
    try:
        decoded = base64.b64decode(encoded + '=' * ((4 - len(encoded) % 4) % 4))
    except Exception:
        return None
    return decoded or None


def _finance_matches_search(row, search_query):
    query = _clean_str(search_query).lower()
    if not query:
        return True
    hay = ' '.join([
        _clean_str(row.get('categoryLabel')).lower(),
        _clean_str(row.get('sourceName')).lower(),
        _clean_str(row.get('sourceDetails')).lower(),
        _clean_str(row.get('contact')).lower(),
        _clean_str(row.get('status')).lower(),
        _clean_str(row.get('pathLabel')).lower(),
        _clean_str(row.get('periodLabel')).lower(),
        _clean_str(row.get('eventDate')).lower(),
        _clean_str(row.get('amountLabel')).lower(),
    ])
    return query in hay


def _finance_best_effort_refresh_premium_tracking():
    now = datetime.utcnow()
    query = {
        'ttn': {'$nin': ['', None]},
        'status': {'$nin': list(PREMIUM_ORDER_TERMINAL_STATUSES)},
        '$or': [
            {'npNextCheckAt': {'$exists': False}},
            {'npNextCheckAt': None},
            {'npNextCheckAt': {'$lte': now}},
        ],
    }
    docs = list(
        premium_orders_collection
        .find(query)
        .sort([('npLastCheckedAt', 1), ('createdAt', 1), ('_id', 1)])
        .limit(NP_TRACKING_BATCH_LIMIT_DEFAULT)
    )
    if not docs:
        return
    max_workers = max(1, min(NP_TRACKING_BATCH_CONCURRENCY, len(docs)))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_premium_refresh_tracking_for_order, doc, force=False) for doc in docs]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                # Tracking refresh is best-effort only.
                pass


def _finance_is_success_payment(value):
    return _clean_str(value).lower() in ADMIN_NOTES_SUCCESS_PAYMENT_STATUSES


def _finance_collect_premium_rows(tzinfo, date_from, date_to):
    rows = []
    success_statuses = []
    for status in ADMIN_NOTES_SUCCESS_PAYMENT_STATUSES:
        success_statuses.extend([status, status.upper(), status.capitalize()])
    docs = premium_orders_collection.find({
        '$or': [
            {'paymentStatus': {'$in': sorted(set(success_statuses))}},
            {'status': 'received'},
        ]
    }).sort([('updatedAt', -1), ('createdAt', -1), ('_id', -1)])
    for doc in docs:
        if not (_finance_is_success_payment(doc.get('paymentStatus')) or _clean_str(doc.get('status')).lower() == 'received'):
            continue
        webhook_data = doc.get('webhookData') if isinstance(doc.get('webhookData'), dict) else {}
        amount_uah = _finance_extract_webhook_amount_uah(webhook_data)
        if amount_uah is None:
            amount_uah = 0
        event_dt = (
            _finance_extract_webhook_datetime(webhook_data)
            or _finance_parse_any_datetime(doc.get('npDeliveredAt'))
            or _finance_parse_any_datetime(doc.get('updatedAt'))
            or _finance_parse_any_datetime(doc.get('createdAt'))
        )
        local_date = _finance_to_local_date(event_dt, tzinfo)
        if not _finance_is_in_period(local_date, date_from, date_to):
            continue
        path_label = _clean_str(doc.get('pathType')) or 'Сайт'
        rows.append(
            _finance_build_row(
                row_id=f"premium:{doc.get('_id')}",
                tab_id='premium_qr',
                category_label=f'Преміум QR | {path_label}',
                amount_uah=amount_uah,
                local_date=local_date,
                event_dt=event_dt,
                source_name=_clean_str(doc.get('personName')) or _clean_str(doc.get('customerName')),
                source_details=_clean_str(doc.get('cemetery')) or _clean_str(doc.get('cemeteryAddress')),
                contact=_clean_str(doc.get('customerPhone')),
                status='Отримано',
                path_label=path_label,
                receipt_available=bool(_finance_extract_invoice_id(doc)),
            )
        )
    return rows


def _finance_collect_plaques_rows(tzinfo, date_from, date_to):
    rows = []
    docs = plaques_orders_collection.find({}).sort([('updatedAt', -1), ('createdAt', -1), ('_id', -1)])
    for doc in docs:
        webhook_data = doc.get('webhookData') if isinstance(doc.get('webhookData'), dict) else {}
        amount_uah = _finance_extract_webhook_amount_uah(webhook_data)
        if amount_uah is None:
            amount_uah = FINANCE_FALLBACK_PLAQUE_UAH
        event_dt = (
            _finance_extract_webhook_datetime(webhook_data)
            or _finance_parse_any_datetime(doc.get('updatedAt'))
            or _finance_parse_any_datetime(doc.get('createdAt'))
        )
        local_date = _finance_to_local_date(event_dt, tzinfo)
        if not _finance_is_in_period(local_date, date_from, date_to):
            continue
        payment_status = _clean_str(doc.get('paymentStatus')).lower()
        status_label = 'Оплачено' if payment_status in {'success', 'paid', 'completed'} else 'На модерації'
        rows.append(
            _finance_build_row(
                row_id=f"plaques_order:{doc.get('_id')}",
                tab_id='plaques',
                category_label='Таблички',
                amount_uah=amount_uah,
                local_date=local_date,
                event_dt=event_dt,
                source_name=_clean_str(doc.get('personName')) or _clean_str(doc.get('name')),
                source_details=_clean_str(doc.get('cemetery')) or _clean_str(doc.get('cemeteryAddress')),
                contact=_clean_str(doc.get('customerPhone')) or _clean_str(doc.get('phone')),
                status=status_label,
                path_label='Таблички',
                receipt_available=bool(_finance_extract_invoice_id(doc)),
            )
        )
    return rows


def _finance_collect_ritual_rows(tzinfo, date_from, date_to):
    rows = []
    services = ritual_services_collection.find({}).sort([('updatedAt', -1), ('createdAt', -1), ('_id', -1)])
    for service_doc in services:
        service_name = _clean_str(service_doc.get('name'))
        service_logo = _clean_str(service_doc.get('logo'))
        status = _normalize_ritual_service_status(service_doc.get('status'))
        receipt_available = bool(_finance_pick_latest_ritual_receipt_url(service_doc))
        contacts = _clean_ritual_contacts_loose(service_doc.get('contacts'))
        contact_phone = ''
        if contacts:
            contact_phone = _clean_str(contacts[0].get('phone'))
        payments = _clean_ritual_payments(service_doc.get('payments'))
        for index, payment in enumerate(payments):
            webhook_data = payment.get('webhookData') if isinstance(payment.get('webhookData'), dict) else {}
            amount_uah = _finance_extract_webhook_amount_uah(webhook_data)
            if amount_uah is None:
                amount_uah = FINANCE_FALLBACK_RITUAL_PAYMENT_UAH
            event_dt = (
                _finance_extract_webhook_datetime(webhook_data)
                or _finance_parse_any_datetime(payment.get('createdAt'))
                or _finance_parse_any_datetime(service_doc.get('updatedAt'))
                or _finance_parse_any_datetime(service_doc.get('createdAt'))
            )
            local_date = _finance_to_local_date(event_dt, tzinfo)
            if not _finance_is_in_period(local_date, date_from, date_to):
                continue
            period_label = _clean_str(payment.get('period'))
            rows.append(
                _finance_build_row(
                    row_id=f"ritual:{service_doc.get('_id')}:{index}",
                    tab_id='ritual_services',
                    category_label='Ритуальні послуги | Проплата',
                    amount_uah=amount_uah,
                    local_date=local_date,
                    event_dt=event_dt,
                    source_name=service_name,
                    source_details=_clean_str(service_doc.get('address')),
                    source_avatar_url=service_logo,
                    contact=contact_phone,
                    status=status,
                    period_label=period_label,
                    receipt_available=receipt_available,
                )
            )
    return rows


def _finance_collect_notes_rows(tzinfo, date_from, date_to):
    rows = []
    projection = {'churchName': 1, 'personName': 1, 'phone': 1, 'price': 1, 'paymentStatus': 1, 'createdAt': 1, 'serviceDate': 1, 'webhookData': 1, 'invoiceId': 1}
    docs = liturgies_collection.find({'paymentStatus': {'$in': ADMIN_NOTES_SUCCESS_QUERY_VALUES}}, projection).sort([('createdAt', -1), ('_id', -1)])
    
    # Build church mapping for address lookup
    church_projection = {'name': 1, 'address': 1, 'locality': 1, 'location': 1, 'telegramChatId': 1, 'status': 1}
    church_docs = list(churches_collection.find({}, church_projection).sort([('name', 1), ('_id', 1)]))
    church_by_id = {str(doc.get('_id')): doc for doc in church_docs}
    church_by_name = {}
    for doc in church_docs:
        normalized_name = _clean_str(doc.get('name')).casefold()
        if normalized_name and normalized_name not in church_by_name:
            church_by_name[normalized_name] = doc
    
    for doc in docs:
        if not _admin_notes_is_success_payment(doc.get('paymentStatus')):
            continue
            
        webhook_data = doc.get('webhookData') if isinstance(doc.get('webhookData'), dict) else {}
        amount_uah = _finance_extract_webhook_amount_uah(webhook_data)
        if amount_uah is None:
            amount_uah = int(round(_admin_note_payments_parse_amount(doc.get('price'))))
        event_dt = (
            _finance_extract_webhook_datetime(webhook_data)
            or _finance_parse_any_datetime(doc.get('createdAt'))
            or _finance_parse_any_datetime(doc.get('serviceDate'))
        )
        local_date = _finance_to_local_date(event_dt, tzinfo)
        if not _finance_is_in_period(local_date, date_from, date_to):
            continue
        
        # Get church address instead of person name
        church_address = ''
        raw_church_id = _clean_str(doc.get('churchId'))
        raw_church_name = _clean_str(doc.get('churchName'))
        
        if raw_church_id and raw_church_id in church_by_id:
            church_address = church_by_id[raw_church_id].get('address', '')
        elif raw_church_name:
            church_doc = church_by_name.get(raw_church_name.casefold())
            if church_doc:
                church_address = church_doc.get('address', '')
        
        rows.append(
            _finance_build_row(
                row_id=f"notes:{doc.get('_id')}",
                tab_id='notes',
                category_label='Електронні записки',
                amount_uah=amount_uah,
                local_date=local_date,
                event_dt=event_dt,
                source_name=_clean_str(doc.get('churchName')) or 'Церква',
                source_details=church_address,
                contact=_clean_str(doc.get('phone')),
                status='Оплачено',
                receipt_available=bool(_finance_extract_invoice_id(doc)),
            )
        )
    return rows


def _finance_collect_ads_rows(tzinfo, date_from, date_to):
    rows = []
    docs = ads_campaigns_collection.find({'status': 'active'}).sort([('updatedAt', -1), ('createdAt', -1), ('_id', -1)])
    for doc in docs:
        webhook_data = doc.get('webhookData') if isinstance(doc.get('webhookData'), dict) else {}
        amount_uah = _finance_extract_webhook_amount_uah(webhook_data)
        if amount_uah is None:
            amount_uah = _ads_clean_int(doc.get('priceUah'), 'priceUah', default=0)
        event_dt = (
            _finance_extract_webhook_datetime(webhook_data)
            or _finance_parse_any_datetime(doc.get('verifiedAt'))
            or _finance_parse_any_datetime(doc.get('updatedAt'))
            or _finance_parse_any_datetime(doc.get('createdAt'))
        )
        local_date = _finance_to_local_date(event_dt, tzinfo)
        if not _finance_is_in_period(local_date, date_from, date_to):
            continue
        surface_type = _ads_normalize_surface_type(doc.get('surfaceType'), default='columns')
        surface_label = FINANCE_ADS_SURFACE_LABELS.get(surface_type, 'Стовпчики')
        period_start = _clean_str(doc.get('periodStart'))
        period_end = _clean_str(doc.get('periodEnd'))
        period_label = ''
        if period_start and period_end:
            period_label = f'{period_start} - {period_end}'
        rows.append(
            _finance_build_row(
                row_id=f"ads:{doc.get('_id')}",
                tab_id='ads',
                category_label=f'Реклама {surface_label} | Проплата',
                amount_uah=amount_uah,
                local_date=local_date,
                event_dt=event_dt,
                source_name=_clean_str(doc.get('companyName')),
                source_details=_clean_str(doc.get('address')),
                contact=_clean_str(doc.get('phone')),
                status=_ads_campaign_status_label(_ads_normalize_campaign_status(doc.get('status'))),
                path_label=surface_label,
                period_label=period_label,
                receipt_available=bool(_finance_pick_latest_ads_receipt_url(doc)),
            )
        )
    return rows


def _finance_collect_rows(date_from=None, date_to=None):
    tzinfo, timezone_name = _finance_resolve_timezone()
    by_tab = {
        'plaques': _finance_collect_plaques_rows(tzinfo, date_from, date_to),
        'ritual_services': _finance_collect_ritual_rows(tzinfo, date_from, date_to),
        'notes': _finance_collect_notes_rows(tzinfo, date_from, date_to),
        'ads': _finance_collect_ads_rows(tzinfo, date_from, date_to),
        'premium_qr': _finance_collect_premium_rows(tzinfo, date_from, date_to),
    }
    by_tab['main'] = []
    for tab_id in ('plaques', 'ritual_services', 'notes', 'ads', 'premium_qr'):
        by_tab['main'].extend(by_tab.get(tab_id, []))
    for tab_id in by_tab:
        by_tab[tab_id].sort(key=lambda row: row.get('sortKey') or '', reverse=True)
    return by_tab, timezone_name


def _finance_build_graph_points(period, rows, tzinfo, *, date_from=None, date_to=None):
    cleaned_period = _clean_str(period).lower() or 'month'
    if cleaned_period not in {'day', 'week', 'month', 'custom'}:
        cleaned_period = 'month'

    source_rows = rows if isinstance(rows, list) else []
    now_local = datetime.now(tzinfo)

    if cleaned_period == 'day':
        labels = [f'{hour:02d}:00' for hour in range(0, 24, 3)]
        sums = [0 for _ in labels]
        today = now_local.date()

        for row in source_rows:
            row_dt = _finance_parse_any_datetime(row.get('sortKey'))
            if not row_dt:
                continue
            if row_dt.tzinfo is not None:
                try:
                    row_dt = row_dt.astimezone(tzinfo).replace(tzinfo=None)
                except Exception:
                    row_dt = row_dt.replace(tzinfo=None)
            if row_dt.date() != today:
                continue
            bucket = min(max(row_dt.hour // 3, 0), len(sums) - 1)
            sums[bucket] += max(int(row.get('amountUah') or 0), 0)

        return cleaned_period, [{'label': labels[idx], 'value': sums[idx]} for idx in range(len(labels))]

    if cleaned_period == 'week':
        day_list = [now_local.date() - timedelta(days=delta) for delta in range(6, -1, -1)]
        index_by_day = {day: idx for idx, day in enumerate(day_list)}
        labels = [ADMIN_NOTES_WEEKDAY_LABELS[(day.weekday() + 1) % 7] for day in day_list]
        sums = [0 for _ in day_list]

        for row in source_rows:
            row_dt = _finance_parse_any_datetime(row.get('sortKey'))
            if not row_dt:
                continue
            if row_dt.tzinfo is not None:
                try:
                    row_dt = row_dt.astimezone(tzinfo).replace(tzinfo=None)
                except Exception:
                    row_dt = row_dt.replace(tzinfo=None)
            row_day = row_dt.date()
            if row_day not in index_by_day:
                continue
            sums[index_by_day[row_day]] += max(int(row.get('amountUah') or 0), 0)

        return cleaned_period, [{'label': labels[idx], 'value': sums[idx]} for idx in range(len(labels))]

    if cleaned_period == 'custom':
        if not date_from or not date_to or date_from > date_to:
            return cleaned_period, []
        day_list = [date_from + timedelta(days=delta) for delta in range((date_to - date_from).days + 1)]
        index_by_day = {day: idx for idx, day in enumerate(day_list)}
        labels = [day.strftime('%d.%m') for day in day_list]
        sums = [0 for _ in day_list]

        for row in source_rows:
            row_dt = _finance_parse_any_datetime(row.get('sortKey'))
            if not row_dt:
                continue
            if row_dt.tzinfo is not None:
                try:
                    row_dt = row_dt.astimezone(tzinfo).replace(tzinfo=None)
                except Exception:
                    row_dt = row_dt.replace(tzinfo=None)
            row_day = row_dt.date()
            if row_day not in index_by_day:
                continue
            sums[index_by_day[row_day]] += max(int(row.get('amountUah') or 0), 0)

        return cleaned_period, [{'label': labels[idx], 'value': sums[idx]} for idx in range(len(labels))]

    day_list = [now_local.date() - timedelta(days=delta) for delta in range(29, -1, -1)]
    index_by_day = {day: idx for idx, day in enumerate(day_list)}
    labels = [day.strftime('%d.%m') for day in day_list]
    sums = [0 for _ in day_list]

    for row in source_rows:
        row_dt = _finance_parse_any_datetime(row.get('sortKey'))
        if not row_dt:
            continue
        if row_dt.tzinfo is not None:
            try:
                row_dt = row_dt.astimezone(tzinfo).replace(tzinfo=None)
            except Exception:
                row_dt = row_dt.replace(tzinfo=None)
        row_day = row_dt.date()
        if row_day not in index_by_day:
            continue
        sums[index_by_day[row_day]] += max(int(row.get('amountUah') or 0), 0)

    return cleaned_period, [{'label': labels[idx], 'value': sums[idx]} for idx in range(len(labels))]


def _finance_build_summary_payload(rows_by_tab):
    tabs = []
    grand_total = 0
    grand_count = 0
    for item in FINANCE_TABS:
        tab_id = item['id']
        rows = rows_by_tab.get(tab_id, [])
        amount = int(sum(max(int(row.get('amountUah') or 0), 0) for row in rows))
        count = len(rows)
        if tab_id != 'main':
            grand_total += amount
            grand_count += count
        tabs.append({
            'id': tab_id,
            'label': item['label'],
            'amountUah': amount,
            'amountLabel': f'{amount} грн',
            'count': count,
        })
    return {
        'totalAmountUah': grand_total,
        'totalAmountLabel': f'{grand_total} грн',
        'totalCount': grand_count,
        'tabs': tabs,
    }


@application.route('/api/admin/finance/summary', methods=['GET'])
def admin_finance_summary():
    date_from = _finance_parse_date_param(request.args.get('from'), 'from')
    date_to = _finance_parse_date_param(request.args.get('to'), 'to')
    if date_from and date_to and date_from > date_to:
        abort(400, description='`from` must be less than or equal to `to`')

    run_refresh = _finance_parse_bool(request.args.get('refreshPremium', '1'))
    if run_refresh:
        _finance_best_effort_refresh_premium_tracking()

    rows_by_tab, timezone_name = _finance_collect_rows(date_from=date_from, date_to=date_to)
    summary = _finance_build_summary_payload(rows_by_tab)
    return jsonify({
        'period': {
            'from': date_from.isoformat() if date_from else '',
            'to': date_to.isoformat() if date_to else '',
            'inclusive': True,
        },
        'timezone': timezone_name,
        **summary,
        'generatedAt': datetime.utcnow().isoformat(),
    })


@application.route('/api/admin/finance/report', methods=['GET'])
def admin_finance_report():
    tab = _clean_str(request.args.get('tab')) or 'main'
    if tab not in FINANCE_TAB_IDS:
        abort(400, description='`tab` must be one of: main, plaques, ritual_services, notes, ads, premium_qr')

    date_from = _finance_parse_date_param(request.args.get('from'), 'from')
    date_to = _finance_parse_date_param(request.args.get('to'), 'to')
    if date_from and date_to and date_from > date_to:
        abort(400, description='`from` must be less than or equal to `to`')

    search_query = _clean_str(request.args.get('search'))
    run_refresh = _finance_parse_bool(request.args.get('refreshPremium', '1'))
    if run_refresh and tab in {'main', 'premium_qr'}:
        _finance_best_effort_refresh_premium_tracking()

    rows_by_tab, timezone_name = _finance_collect_rows(date_from=date_from, date_to=date_to)
    summary = _finance_build_summary_payload(rows_by_tab)
    source_rows = rows_by_tab.get(tab, [])
    rows = [row for row in source_rows if _finance_matches_search(row, search_query)]
    for row in rows:
        row.pop('sortKey', None)

    tab_amount = int(sum(max(int(row.get('amountUah') or 0), 0) for row in rows))
    return jsonify({
        'tab': tab,
        'tabLabel': _finance_tab_label(tab),
        'period': {
            'from': date_from.isoformat() if date_from else '',
            'to': date_to.isoformat() if date_to else '',
            'inclusive': True,
        },
        'timezone': timezone_name,
        'search': search_query,
        'totalCount': len(rows),
        'totalAmountUah': tab_amount,
        'totalAmountLabel': f'{tab_amount} грн',
        'rows': rows,
        'summary': summary,
        'generatedAt': datetime.utcnow().isoformat(),
    })


@application.route('/api/admin/finance/receipt', methods=['GET'])
def admin_finance_receipt():
    raw_row_id = _clean_str(request.args.get('rowId'))
    if not raw_row_id:
        abort(400, description='`rowId` is required')

    parts = raw_row_id.split(':')
    if not parts:
        abort(404, description='Receipt not found')

    kind = parts[0]
    target_url = ''
    invoice_id = ''

    if kind == 'premium' and len(parts) >= 2:
        try:
            doc = premium_orders_collection.find_one({'_id': ObjectId(parts[1])}, {'invoiceId': 1, 'webhookData': 1})
        except Exception:
            doc = None
        invoice_id = _finance_extract_invoice_id(doc or {})
    elif kind in {'plaques', 'plaques_order'} and len(parts) >= 2:
        try:
            if kind == 'plaques_order':
                doc = plaques_orders_collection.find_one({'_id': ObjectId(parts[1])}, {'invoiceId': 1, 'webhookData': 1})
            else:
                doc = plaques_moderation_collection.find_one({'_id': ObjectId(parts[1])}, {'invoiceId': 1, 'webhookData': 1})
        except Exception:
            doc = None
        invoice_id = _finance_extract_invoice_id(doc or {})
    elif kind == 'notes' and len(parts) >= 2:
        try:
            doc = liturgies_collection.find_one({'_id': ObjectId(parts[1])}, {'invoiceId': 1, 'webhookData': 1})
        except Exception:
            doc = None
        invoice_id = _finance_extract_invoice_id(doc or {})
    elif kind == 'ads' and len(parts) >= 2:
        try:
            doc = ads_campaigns_collection.find_one({'_id': ObjectId(parts[1])}, {'payments': 1, 'pdfUrl': 1})
        except Exception:
            doc = None
        target_url = _finance_pick_latest_ads_receipt_url(doc or {})
    elif kind == 'ritual' and len(parts) >= 2:
        try:
            doc = ritual_services_collection.find_one({'_id': ObjectId(parts[1])}, {'payments': 1})
        except Exception:
            doc = None
        target_url = _finance_pick_latest_ritual_receipt_url(doc or {})

    if target_url:
        return redirect(target_url, code=302)

    if invoice_id:
        pdf_bytes = _finance_fetch_monobank_receipt_pdf(invoice_id)
        if pdf_bytes:
            return send_file(
                io.BytesIO(pdf_bytes),
                mimetype='application/pdf',
                as_attachment=False,
                download_name=f'{invoice_id}-receipt.pdf',
            )

    abort(404, description='Receipt not found')


@application.route('/api/admin/finance/graph', methods=['GET'])
def admin_finance_graph():
    period = _clean_str(request.args.get('period')).lower() or 'month'
    if period not in {'day', 'week', 'month', 'custom'}:
        abort(400, description='`period` must be one of: day, week, month, custom')

    date_from_raw = _clean_str(request.args.get('from') or request.args.get('dateFrom'))
    date_to_raw = _clean_str(request.args.get('to') or request.args.get('dateTo'))
    date_from = _finance_parse_date_param(date_from_raw, 'from') if date_from_raw else None
    date_to = _finance_parse_date_param(date_to_raw, 'to') if date_to_raw else None
    if period == 'custom':
        if not date_from_raw or not date_to_raw:
            abort(400, description='`from` and `to` are required for custom period')
        if date_from is None or date_to is None:
            abort(400, description='`from` and `to` must be in format YYYY-MM-DD')
        if date_from > date_to:
            abort(400, description='`from` must be less or equal to `to`')
        if (date_to - date_from).days > 366:
            abort(400, description='Custom period cannot exceed 367 days')

    timezone_value = _clean_str(request.args.get('timezone'))
    tzinfo, timezone_name = _finance_resolve_timezone(timezone_value)
    rows_by_tab, _ = _finance_collect_rows(date_from=None, date_to=None)
    active_period, points = _finance_build_graph_points(
        period,
        rows_by_tab.get('main', []),
        tzinfo,
        date_from=date_from,
        date_to=date_to,
    )

    return jsonify({
        'period': active_period,
        'points': points,
        'meta': {
            'timezone': timezone_name,
            'isFallback': False,
            'fallbackReason': [],
        },
    })


def _premium_pick_qr_code_for_activation(path_key='premium_qr'):
    target_path_key = _premium_order_str(path_key) or 'premium_qr'
    preferred_doc = admin_qr_codes_collection.find_one(
        {'pathKey': target_path_key, 'isConnected': {'$ne': True}, 'isPrinted': True},
        sort=[('qrNumber', ASCENDING), ('_id', ASCENDING)],
    )
    if preferred_doc:
        return preferred_doc
    return admin_qr_codes_collection.find_one(
        {'pathKey': target_path_key, 'isConnected': {'$ne': True}},
        sort=[('qrNumber', ASCENDING), ('_id', ASCENDING)],
    )


@application.route('/api/admin/premium-orders/<string:order_id>/activate', methods=['POST'])
def admin_activate_premium_order(order_id):
    try:
        oid = ObjectId(order_id)
    except Exception:
        abort(400, description='Invalid order id')

    order_doc = premium_orders_collection.find_one({'_id': oid})
    if not order_doc:
        abort(404, description='Order not found')

    moderation_payload = order_doc.get('moderationPayload') if isinstance(order_doc.get('moderationPayload'), dict) else {}
    cemetery_name = loc_clean_str(moderation_payload.get('cemetery') or order_doc.get('cemetery'))
    area = loc_clean_str(
        moderation_payload.get('area')
        or moderation_payload.get('cemeteryAddress')
        or order_doc.get('cemeteryAddress')
        or order_doc.get('address')
        or order_doc.get('area')
    )
    if not _admin_moderation_cemetery_exists(cemetery_name, area):
        abort(400, description='Активація недоступна: кладовище має статус "Не додано".')

    raw_password = _premium_order_str(order_doc.get('generatedPassword'))
    if not raw_password:
        abort(400, description='Missing generatedPassword for activation')

    now = datetime.utcnow()
    activation_type = _premium_order_str(order_doc.get('activationType'))
    qr_path_key = _premium_order_str(order_doc.get('qrPathKey')) or 'premium_qr'
    is_premium_qr_firma = activation_type == 'premium_qr_firma' or qr_path_key == 'premium_qr_firma'
    company_name = _premium_order_str(order_doc.get('companyName'))
    customer_name = _premium_order_str(order_doc.get('customerName'))
    customer_phone = _premium_order_str(order_doc.get('customerPhone'))
    notable_link = _premium_order_str(
        moderation_payload.get('sourceLink')
        or moderation_payload.get('link')
        or moderation_payload.get('internetLinks')
    )
    notable_bio = _premium_order_str(moderation_payload.get('bio') or moderation_payload.get('achievements'))
    notable_flag = bool(moderation_payload.get('notable')) or bool(notable_link) or bool(notable_bio)

    person_ids = order_doc.get('personIds') if isinstance(order_doc.get('personIds'), list) else []
    primary_person_id = _premium_order_str(order_doc.get('personId')) or _premium_order_str(person_ids[0] if person_ids else '')

    premium_partner_ref = ''
    premium_qr_firma_id = _premium_order_str(order_doc.get('premiumQrFirmaId'))
    if is_premium_qr_firma and premium_qr_firma_id:
        try:
            premium_qr_firma_oid = ObjectId(premium_qr_firma_id)
        except Exception:
            abort(400, description='Invalid premiumQrFirmaId')
        premium_qr_firma = premium_qr_firmas_collection.find_one({'_id': premium_qr_firma_oid}, {'_id': 1})
        if not premium_qr_firma:
            abort(404, description='Premium QR firma not found')
        premium_partner_ref = f'firma:{premium_qr_firma_id}'

    if is_premium_qr_firma:
        merge_person_id = _premium_order_str(order_doc.get('mergePersonId'))
        if merge_person_id:
            try:
                merge_person_oid = ObjectId(merge_person_id)
            except Exception:
                abort(400, description='Invalid mergePersonId')
            merge_person = people_collection.find_one({'_id': merge_person_oid}, {'_id': 1})
            if not merge_person:
                abort(404, description='Merge person not found')
            primary_person_id = merge_person_id
            person_ids = [merge_person_id]
        else:
            if not moderation_payload:
                abort(400, description='Missing moderationPayload for premium_qr_firma activation')
            person_doc = _admin_build_person_from_moderation(moderation_payload, {})
            person_doc['notable'] = notable_flag
            person_doc['sourceLink'] = notable_link
            person_doc['link'] = notable_link
            person_doc['bio'] = notable_bio
            if company_name:
                person_doc['companyName'] = company_name
                admin_page_payload = person_doc.get('adminPage') if isinstance(person_doc.get('adminPage'), dict) else {}
                admin_page_payload['companyName'] = company_name
                admin_page_payload['updatedAt'] = datetime.utcnow()
                person_doc['adminPage'] = admin_page_payload
            if premium_partner_ref:
                person_doc['premiumPartnerRitualServiceId'] = premium_partner_ref
            created_person = people_collection.insert_one(person_doc)
            primary_person_id = str(created_person.inserted_id)
            person_ids = [primary_person_id]
    else:
        if not person_ids:
            abort(400, description='Order has no personIds')
        if not primary_person_id:
            abort(400, description='Order has no primary personId')

    order_id_str = str(oid)
    qr_doc = None
    wired_qr_doc_id = _premium_order_str(order_doc.get('qrDocId') or order_doc.get('wiredQrCodeDocId'))
    wired_qr_code_from_order = _premium_order_str(order_doc.get('wiredQrCodeId'))
    if not wired_qr_doc_id and not wired_qr_code_from_order:
        abort(400, description="Спочатку прив'яжіть QR-код перед активацією")
    if wired_qr_doc_id:
        try:
            qr_doc = admin_qr_codes_collection.find_one({'_id': ObjectId(wired_qr_doc_id)})
        except Exception:
            abort(400, description='Invalid wired QR code id')
        if not qr_doc:
            abort(409, description='Selected QR code is not found')
    elif wired_qr_code_from_order:
        qr_query = {
            'pathKey': qr_path_key,
            '$or': [
                {'wiredQrCodeId': wired_qr_code_from_order},
            ],
        }
        try:
            qr_number = int(wired_qr_code_from_order)
            qr_query['$or'].append({'qrNumber': qr_number})
        except Exception:
            pass
        qr_doc = admin_qr_codes_collection.find_one(qr_query)
        if not qr_doc:
            abort(409, description='Selected QR code is not found')

    qr_doc = _qr_ensure_doc_runtime_fields(qr_doc, persist=True)
    qr_doc_id = qr_doc.get('_id')
    if bool(qr_doc.get('isConnected')):
        abort(409, description='QR code is already connected')
    reserved_order_id = _qr_str(qr_doc.get('reservedOrderId'))
    if reserved_order_id and reserved_order_id != order_id_str:
        abort(409, description='QR code is already reserved')
    wired_qr_code_id = _qr_str(qr_doc.get('wiredQrCodeId')) or _qr_str(qr_doc.get('qrNumber'))

    for person_id in person_ids:
        cleaned = _premium_order_str(person_id)
        if not cleaned:
            continue
        try:
            pid = ObjectId(cleaned)
        except Exception:
            continue
        people_collection.update_one(
            {'_id': pid},
            {
                '$set': (
                    {
                        **_premium_password_set_payload(raw_password, include_updated_at=False),
                        'premium.updatedAt': now,
                        'customerName': customer_name,
                        'customerPhone': customer_phone,
                        'notable': notable_flag,
                        'sourceLink': notable_link,
                        'link': notable_link,
                        'bio': notable_bio,
                        **({'premiumPartnerRitualServiceId': premium_partner_ref} if premium_partner_ref else {}),
                    }
                    if not is_premium_qr_firma
                    else {
                        **_premium_password_set_payload(raw_password, include_updated_at=False),
                        'premium.updatedAt': now,
                        'customerName': customer_name,
                        'customerPhone': customer_phone,
                        'notable': notable_flag,
                        'sourceLink': notable_link,
                        'link': notable_link,
                        'bio': notable_bio,
                        **({'companyName': company_name} if company_name else {}),
                        **({'adminPage.companyName': company_name} if company_name else {}),
                        'adminPage.pathKey': 'premium_qr_firma',
                        'adminPage.pathLabel': 'Преміум QR | Фірма',
                        'adminPage.updatedAt': now,
                        **({'premiumPartnerRitualServiceId': premium_partner_ref} if premium_partner_ref else {}),
                    }
                )
            }
        )

    premium_orders_collection.update_one(
        {'_id': oid},
        {
            '$set': {
                'personId': primary_person_id,
                'personIds': person_ids,
                'activatedAt': now,
                'wiredQrCodeId': wired_qr_code_id,
                'wiredQrCodeDocId': str(qr_doc_id) if qr_doc_id else '',
                'wiredPersonId': primary_person_id,
                'updatedAt': now,
            }
        }
    )

    if qr_doc_id:
        admin_qr_codes_collection.update_one(
            {'_id': qr_doc_id},
            {
                '$set': {
                    'status': 'connected',
                    'isConnected': True,
                    'wiredPersonId': primary_person_id,
                    'wiredQrCodeId': wired_qr_code_id,
                    'wiredOrderId': str(oid),
                    'wiredAt': now,
                    'updatedAt': now,
                },
                '$unset': {
                    'reservedOrderId': '',
                    'reservedAt': '',
                },
            }
        )

    updated = premium_orders_collection.find_one({'_id': oid})
    return jsonify(_premium_order_projection(updated, include_existing_person_candidate=True))


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

    premium_orders_collection.update_one(
        {'invoiceId': invoice_id},
        {
            '$set': {
                'paymentStatus': status,
                'webhookData': data,
                'updatedAt': datetime.utcnow(),
            }
        }
    )

    plaques_orders_collection.update_one(
        {'invoiceId': invoice_id},
        {
            '$set': {
                'paymentStatus': status,
                'webhookData': data,
                'updatedAt': datetime.utcnow(),
            }
        }
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
                **_premium_password_set_payload(new_pw),
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
            **_premium_password_set_payload(new_pw),
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
