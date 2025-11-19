import time

import eventlet

eventlet.monkey_patch()

import os
import re
import secrets
import bcrypt
import boto3
import requests
import base64
from datetime import datetime, timedelta, timezone
import jwt

from flask import Flask, request, jsonify, abort, make_response
from flask_cors import CORS
from pymongo import MongoClient, ASCENDING
from bson.objectid import ObjectId
from dotenv import load_dotenv
from flask_socketio import SocketIO, join_room

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

load_dotenv()

JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-key")
JWT_ALGORITHM = "HS256"
JWT_EXP_DELTA_SECONDS = 43200

NP_API_KEY = os.getenv('NP_API_KEY')
NP_BASE_URL = 'https://api.novaposhta.ua/v2.0/json/'

application = Flask(__name__)
CORS(application)

socketio = SocketIO(application, cors_allowed_origins="*")

KYIVSTAR_OTP_SEND_URL = "https://api-gateway.kyivstar.ua/mock/rest/v1/verification/sms"
KYIVSTAR_OTP_VERIFY_URL = "https://api-gateway.kyivstar.ua/mock/rest/v1/verification/sms/check"
KYIVSTAR_ACCESS_TOKEN = os.environ.get("KYIVSTAR_ACCESS_TOKEN")

SPACES_KEY = os.environ.get("SPACES_KEY")
SPACES_SECRET = os.environ.get("SPACES_SECRET")
SPACES_REGION = os.environ.get("SPACES_REGION")
SPACES_BUCKET = os.environ.get("SPACES_BUCKET")

# Create S3-compatible client for DigitalOcean Spaces
s3 = boto3.client(
    "s3",
    region_name=SPACES_REGION,
    endpoint_url=f"https://{SPACES_REGION}.digitaloceanspaces.com",
    aws_access_key_id=SPACES_KEY,
    aws_secret_access_key=SPACES_SECRET,
)

client = MongoClient('mongodb+srv://tsbgalcontract:mymongodb26@cluster0.kppkt.mongodb.net/test')
db = client['memoria_test']
people_collection = db['people']
areas_collection = db['areas']
people_moderation_collection = db['people_moderation']
orders_collection = db['orders']
chat_collection = db['chats']
message_collection = db['messages']
cemeteries_collection = db['cemeteries']
ritual_services_collection = db['ritual_services']
location_moderation_collection = db['location_moderation']
liturgies_collection = db['liturgies']

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
}

GEONAMES_USER = os.environ.get("GEONAMES_USER", "memoria")
GEONAMES_LANG = "uk"

try:
    liturgies_collection.create_index([("personId", ASCENDING), ("serviceDate", ASCENDING)])
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


@application.route('/api/people', methods=['GET'])
def people():
    search_query = request.args.get('search', '').strip()
    birth_year = request.args.get('birthYear')
    death_year = request.args.get('deathYear')
    area_id = request.args.get('areaId', '').strip()
    area = request.args.get('area', '').strip()
    cemetery = request.args.get('cemetery', '').strip()

    # Будуємо список умов і комбінуємо їх через $and,
    # при цьому areaId та area об'єднуємо через $or (як дублюючі ключі місця).
    conditions = []

    if search_query:
        conditions.append({'name': {'$regex': re.escape(search_query), '$options': 'i'}})

    if birth_year and birth_year.isdigit():
        conditions.append({'birthYear': int(birth_year)})

    if death_year and death_year.isdigit():
        conditions.append({'deathYear': int(death_year)})

    area_or = []
    if area_id:
        area_or.append({'areaId': area_id})
    if area:
        # area з фронта може бути у форматі "Місто, Область, ...".
        # Для фільтрації по документах осіб використовуємо лише перше слово/частину (місто),
        # щоб збігатися з рядками виду "м. Самбір, Львівська обл.".
        city_part = area.split(',')[0].strip()
        if city_part:
            pattern = r'\b' + re.escape(city_part) + r'\b'
            area_or.append({'area': {'$regex': pattern, '$options': 'i'}})
    if area_or:
        conditions.append({'$or': area_or})

    if cemetery:
        conditions.append({'cemetery': {'$regex': re.escape(cemetery), '$options': 'i'}})

    if not conditions:
        mongo_filter = {}
    elif len(conditions) == 1:
        mongo_filter = conditions[0]
    else:
        mongo_filter = {'$and': conditions}

    people_cursor = people_collection.find(mongo_filter)

    people_list = []
    for person in people_cursor:
        people_list.append({
            "id": str(person.get('_id')),
            "name": person.get('name'),
            "birthYear": person.get('birthYear'),
            "deathYear": person.get('deathYear'),
            "notable": person.get('notable'),
            "avatarUrl": person.get('avatarUrl'),
            "portraitUrl": person.get('portraitUrl'),
            "areaId": person.get('areaId'),
            "area": person.get('area'),
            "cemetery": person.get('cemetery')
        })

    return jsonify({
        "total": len(people_list),
        "people": people_list
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
        "bio": person.get('bio'),
        "photos": photos_norm,
        "sharedPending": person.get('sharedPending', []),
        "sharedPhotos": person.get('sharedPhotos', []),
        "comments": person.get('comments', [])
    }
    if 'relatives' in person:
        response['relatives'] = [
            {"personId": str(r['personId']), "role": r.get('role')}
            for r in person.get('relatives', [])
        ]
    if 'premium' in person:
        premium_payload = sanitize_premium_payload(person.get('premium'))
        if premium_payload:
            response['premium'] = premium_payload
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


ALLOWED_ROLES = {'Батько', 'Мати', 'Брат', 'Сестра', ' '}


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
        else:
            # accepts avatarUrl=None and portraitUrl=None to clear either/both
            update_doc[field] = value

    if not update_doc:
        abort(400, description=f"No valid fields to update. Allowed: {', '.join(sorted(ALLOWED_UPDATE_FIELDS))}")

    result = people_collection.update_one({'_id': oid}, {'$set': update_doc})
    if result.matched_count == 0:
        abort(404, description="Person not found")

    person = people_collection.find_one({'_id': oid})
    return jsonify({
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
        "bio": person.get('bio'),
        "photos": person.get('photos', []),
        "sharedPending": person.get('sharedPending', []),
        "sharedPhotos": person.get('sharedPhotos', []),
        "comments": person.get('comments', []),
        "relatives": [
            {"personId": str(r['personId']), "role": r['role']}
            for r in person.get('relatives', [])
        ]
    }), 200


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
    data = request.get_json()
    personId = data.get('personId')
    location = data.get('location')

    document = {
        'personId': personId,
        'location': location
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
        cemeteries_list.append({
            "id": str(cemetery.get('_id')),
            "name": cemetery.get('name'),
            "image": cemetery.get('image'),
            "address": cemetery.get('address'),
            "phone": cemetery.get('phone'),
            "description": cemetery.get('description')
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
    return jsonify({
        "id": str(cemetery.get('_id')),
        "name": cemetery.get('name'),
        "image": cemetery.get('image'),
        "address": cemetery.get('address'),
        "phone": cemetery.get('phone'),
        "description": cemetery.get('description')
    })


@application.route('/api/locations', methods=['GET'])
def locations():
    """
    Повертає список назв населених пунктів (area),
    використовуючи той самий GeoNames API, що й сторінка ритуальних послуг.

    Параметри:
      - search — початок назви міста (name_startsWith).

    Відповідь: масив об'єктів:
      - id      — GeoNames geonameId (рядок)
      - display — "Місто, Область, Країна"
      - lat/lng — координати (float), опційно
    """
    search = request.args.get('search', '').strip()
    if not search:
        return jsonify([])

    try:
        url = (
            "https://secure.geonames.org/searchJSON?"
            f"name_startsWith={requests.utils.quote(search)}"
            "&country=UA"
            "&featureClass=P"
            "&maxRows=10"
            f"&lang={GEONAMES_LANG}"
            f"&username={GEONAMES_USER}"
        )
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        geonames = data.get("geonames") or []
    except Exception as exc:
        print(f"[GeoNames] locations error for search='{search}': {exc}")
        return jsonify([])

    seen = set()
    areas = []
    for place in geonames:
        name = (place.get("name") or "").strip()
        admin = (place.get("adminName1") or "").strip()
        country = (place.get("countryName") or "").strip()
        geoname_id = str(place.get("geonameId") or "").strip()
        lat_raw = place.get("lat")
        lng_raw = place.get("lng")
        try:
            lat = float(lat_raw) if lat_raw is not None else None
        except (TypeError, ValueError):
            lat = None
        try:
            lng = float(lng_raw) if lng_raw is not None else None
        except (TypeError, ValueError):
            lng = None
        parts = [p for p in (name, admin, country) if p]
        display = ", ".join(parts)
        if geoname_id and display and geoname_id not in seen:
            seen.add(geoname_id)
            areas.append({
                "id": geoname_id,
                "display": display,
                "lat": lat,
                "lng": lng,
            })

    return jsonify(areas)


@application.route('/api/cemeteries', methods=['GET'])
def cemeteries():
    """
    Повертає список об'єктів { name, area, areaId }:
      - name: назва кладовища
      - area: населений пункт / область, з документа areas_collection.area
      - areaId: GeoNames ID населеного пункту (areas_collection.areaId)

    Фільтри (будь-який з них опційний):
      - areaId — унікальний GeoNames ID населеного пункту
      - area   — місто (перша частина рядка "Місто, Область, ..."),
                 шукається як окреме слово в полі area (case-insensitive),
                 використовується як фолбек, якщо немає areaId
      - search — початок рядка, case-insensitive по елементам масиву cemetries
    """
    area_id = request.args.get('areaId', '').strip()
    area = request.args.get('area', '').strip()
    search = request.args.get('search', '').strip()

    # Будуємо aggregation pipeline
    pipeline = []

    # 1) Фільтр по населеному пункту:
    #    якщо передано areaId — використовуємо його;
    #    інакше фолбек на текстовий area (місто як окреме слово).
    if area_id:
        pipeline.append({
            '$match': {
                'areaId': area_id
            }
        })
    elif area:
        city_part = area.split(',')[0].strip()
        if city_part:
            pattern = r'\b' + re.escape(city_part) + r'\b'
            pipeline.append({
                '$match': {
                    'area': {'$regex': pattern, '$options': 'i'}
                }
            })

    # 2) Беремо лише потрібні поля
    pipeline.extend([
        {'$project': {'area': 1, 'areaId': 1, 'cemetries': 1}},
        {'$unwind': '$cemetries'}
    ])

    # 3) Фільтр по назві кладовища (тільки з початку рядка)
    if search:
        pipeline.append({
            '$match': {
                'cemetries': {'$regex': f'^{re.escape(search)}', '$options': 'i'}
            }
        })

    # 4) Прибрати порожні значення
    pipeline.append({
        '$match': {
            'cemetries': {'$nin': [None, '']}
        }
    })

    # 5) Унікальні пари (кладовище, area, areaId)
    pipeline.extend([
        {'$group': {
            '_id': {'name': '$cemetries', 'area': '$area', 'areaId': '$areaId'}
        }},
        {'$project': {
            '_id': 0,
            'name': '$_id.name',
            'area': '$_id.area',
            'areaId': '$_id.areaId'
        }},
        {'$sort': {'name': 1}},
        {'$limit': 10}
    ])

    items = list(areas_collection.aggregate(pipeline))
    return jsonify(items)


@application.route('/api/ritual_services', methods=['GET'])
def ritual_services():
    search_query = request.args.get('search', '').strip()
    address = request.args.get('address', '').strip()

    query_filter = {}

    if search_query:
        query_filter['name'] = {'$regex': re.escape(search_query), '$options': 'i'}

    if address:
        query_filter['address'] = {'$regex': re.escape(address),
                                   '$options': 'i'}  # частковий, нечутливий до регістру пошук

    ritual_services_cursor = ritual_services_collection.find(query_filter)

    ritual_services_list = []
    for ritual_service in ritual_services_cursor:
        ritual_services_list.append({
            "id": str(ritual_service.get('_id')),
            "name": ritual_service.get('name'),
            "address": ritual_service.get('address'),
            "category": ritual_service.get('category'),
            "logo": ritual_service.get('logo'),
            "latitude": ritual_service.get("latitude"),
            "longitude": ritual_service.get("longitude")
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

        return jsonify({
            "id": str(ritual_service['_id']),
            "name": ritual_service.get('name'),
            "address": ritual_service.get('address'),
            "category": ritual_service.get('category'),
            "logo": ritual_service.get('logo'),
            "latitude": ritual_service.get('latitude'),
            "longitude": ritual_service.get('longitude'),
            "banner": ritual_service.get('banner'),
            "description": ritual_service.get('description'),
            "link": ritual_service.get('link'),
            "phone": ritual_service.get('phone'),
            "items": norm_items  # normalized for the new UI (photos or video)
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
    data = request.get_json()
    name = data.get('name')
    birthYear = data.get('birthYear')
    deathYear = data.get('deathYear')
    area = data.get('area')
    cemetery = data.get('cemetery')
    occupation = data.get('occupation', '')
    link = data.get('link', '')
    bio = data.get('bio', '')

    document = {
        'name': name,
        'birthYear': birthYear,
        'deathYear': deathYear,
        'area': area,
        'cemetery': cemetery,
        'occupation': occupation,
        'link': link,
        'bio': bio
    }
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
    required = ['personId', 'name', 'cityRef', 'branchRef', 'phone', 'paymentMethod', 'email']
    missing = [k for k in required if k not in data or (isinstance(data[k], str) and not data[k].strip())]
    if missing:
        return jsonify({'error': 'Missing required fields', 'fields': missing}), 400

    # Validate email
    email = (data.get('email') or '').strip().lower()

    # Validate personId & fetch person
    try:
        person_oid = ObjectId(data['personId'])
    except Exception:
        return jsonify({'error': 'Invalid personId'}), 400

    person = people_collection.find_one({'_id': person_oid})
    if not person:
        return jsonify({'error': 'Person not found'}), 404

    # Upsert/Update person's email (do not create a new person here)
    try:
        people_collection.update_one(
            {'_id': person_oid},
            {'$set': {
                'email': email,
                'updatedAt': datetime.utcnow()
            }}
        )
    except Exception as e:
        return jsonify({'error': 'Failed to update person email', 'details': str(e)}), 500

    # Build order document (store email as well for audit)
    order_doc = {
        'personId': data['personId'],
        'personName': data.get('personName'),  # may be None; keep as-is
        'name': data['name'],
        'cityRef': data['cityRef'],
        'cityName': data.get('cityName'),  # optional
        'branchRef': data['branchRef'],
        'branchDesc': data.get('branchDesc'),  # optional
        'phone': data['phone'],
        'email': email,  # <- saved on order too
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

    # Monopay очікує 200 OK
    return jsonify({'result': 'ok'}), 200


@application.route('/api/chats', methods=['POST'])
def create_chat():
    """Create a new chat session and send initial admin welcome message."""
    result = chat_collection.insert_one({'createdAt': datetime.utcnow()})
    chat_id = str(result.inserted_id)

    admin_msg = {
        'chatId': result.inserted_id,
        'sender': 'admin',
        'text': "Вітаємо! Чим можемо вам допомогти?",
        'createdAt': datetime.utcnow(),
        'imageData': None
    }
    message_collection.insert_one(admin_msg)
    socketio.emit('newMessage', {
        'sender': admin_msg['sender'],
        'text': admin_msg['text'],
        'imageData': admin_msg['imageData'],
        'createdAt': admin_msg['createdAt'].isoformat()
    }, room=chat_id)

    return jsonify({'chatId': chat_id}), 201


@application.route('/api/chats', methods=['GET'])
def list_chats():
    """List all chat sessions for admin."""
    chats = chat_collection.find().sort('createdAt', -1)
    out = [{'chatId': str(c['_id']), 'createdAt': c['createdAt'].isoformat()} for c in chats]
    return jsonify(out)


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
            'imageData': m['imageData']
        })
    return jsonify(out)


@application.route('/api/chats/<chat_id>/messages', methods=['POST'])
def post_message(chat_id):
    """Post a message (user or admin) with optional image as Base64 and broadcast."""
    sender = None
    text = ''
    image = None

    # Розбір multipart/form-data або JSON
    if request.content_type and 'multipart/form-data' in request.content_type:
        sender = request.form.get('sender')
        text = (request.form.get('text', '') or '').strip()
        image = request.files.get('image')
    else:
        data = request.get_json() or {}
        sender = data.get('sender')
        text = (data.get('text') or '').strip()

    if sender not in ('user', 'admin') or (not text and not image):
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
        {'$setOnInsert': {'createdAt': datetime.utcnow()}},
        upsert=True
    )

    # Кодуємо зображення, якщо воно є
    image_data = None
    if image:
        raw = image.read()
        b64 = base64.b64encode(raw).decode('utf-8')
        image_data = f"data:{image.mimetype};base64,{b64}"

    # Зберігаємо повідомлення
    msg = {
        'chatId': cid,
        'sender': sender,
        'text': text,
        'createdAt': datetime.utcnow(),
        'imageData': image_data
    }
    message_collection.insert_one(msg)

    # Розсилаємо повідомлення через Socket.IO
    payload = {
        'sender': sender,
        'text': text,
        'imageData': image_data,
        'createdAt': msg['createdAt'].isoformat()
    }
    socketio.emit('newMessage', payload, room=chat_id)

    # Автовідповідь на перше повідомлення користувача
    if sender == 'user' and existing_user_msgs == 0:
        followup = {
            'chatId': cid,
            'sender': 'admin',
            'text': "Дякуємо за Ваше повідомлення! Наш спеціаліст відповість Вам протягом 5 хвилин",
            'createdAt': datetime.utcnow(),
            'imageData': None
        }
        time.sleep(2)
        message_collection.insert_one(followup)
        socketio.emit('newMessage', {
            'sender': followup['sender'],
            'text': followup['text'],
            'imageData': followup['imageData'],
            'createdAt': followup['createdAt'].isoformat()
        }, room=chat_id)

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
      "price": 500                       # optional (number)
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

    now_utc = datetime.now(timezone.utc)

    # Only the required minimal fields are persisted
    doc = {
        "person": person_oid,
        "serviceDate": service_dt,
        "churchName": church_name,
        "price": price,
        "createdAt": now_utc,
    }

    ins = liturgies_collection.insert_one(doc)

    # Minimal response with only the requested fields
    out = {
        "_id": str(ins.inserted_id),
        "person": str(doc["person"]),
        "serviceDate": doc["serviceDate"].isoformat(),
        "churchName": doc["churchName"],
        "price": doc["price"],
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
            "serviceDate": doc["serviceDate"].isoformat(),
            "churchName": doc.get("churchName"),
            "price": doc.get("price"),
            "createdAt": doc["createdAt"].isoformat(),
        })
    return jsonify(results)


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


if __name__ == '__main__':
    socketio.run(application, host='0.0.0.0', port=5000)
