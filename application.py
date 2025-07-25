import eventlet
eventlet.monkey_patch()

import os
import re
import requests
import base64
from datetime import datetime, timedelta
import jwt

from flask import Flask, request, jsonify, abort
from flask_cors import CORS
from pymongo import MongoClient
from bson.objectid import ObjectId
from dotenv import load_dotenv
from twilio.rest import Client
from flask_socketio import SocketIO, join_room, emit

load_dotenv()

JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-key")
JWT_ALGORITHM = "HS256"
JWT_EXP_DELTA_SECONDS = 3600  # 1 година

NP_API_KEY = os.getenv('NP_API_KEY')
NP_BASE_URL = 'https://api.novaposhta.ua/v2.0/json/'

application = Flask(__name__)
CORS(application)

socketio = SocketIO(application, cors_allowed_origins="*")

# init Twilio client
twilio_client = Client(
    os.getenv('TWILIO_ACCOUNT_SID'),
    os.getenv('TWILIO_AUTH_TOKEN')
)
verify_service = os.getenv('TWILIO_VERIFY_SERVICE_SID')

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
    "area",
    "cemetery",
    "location",
    "bio",
    "photos",
}


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
    area = request.args.get('area', '').strip()
    cemetery = request.args.get('cemetery', '').strip()

    query_filter = {}

    if search_query:
        query_filter['name'] = {'$regex': re.escape(search_query), '$options': 'i'}

    if birth_year and birth_year.isdigit():
        query_filter['birthYear'] = int(birth_year)

    if death_year and death_year.isdigit():
        query_filter['deathYear'] = int(death_year)

    if area:
        query_filter['area'] = {'$regex': re.escape(area), '$options': 'i'}  # частковий, нечутливий до регістру пошук

    if cemetery:
        query_filter['cemetery'] = {'$regex': re.escape(cemetery), '$options': 'i'}

    people_cursor = people_collection.find(query_filter)

    people_list = []
    for person in people_cursor:
        people_list.append({
            "id": str(person.get('_id')),
            "name": person.get('name'),
            "birthYear": person.get('birthYear'),
            "deathYear": person.get('deathYear'),
            "notable": person.get('notable'),
            "avatarUrl": person.get('avatarUrl'),
            "area": person.get('area'),
            "cemetery": person.get('cemetery')
        })

    return jsonify({
        "total": len(people_list),
        "people": people_list
    })


@application.route('/api/people/<string:person_id>', methods=['GET'])
def get_person(person_id):
    # 1) Validate & convert the id
    try:
        oid = ObjectId(person_id)
    except Exception:
        abort(400, description="Invalid person id")

    # 2) Fetch from Mongo
    person = people_collection.find_one({'_id': oid})
    if not person:
        abort(404, description="Person not found")

    # 3) Build your response payload
    return jsonify({
        "id": str(person['_id']),
        "name": person.get('name'),
        "birthYear": person.get('birthYear'),
        "birthDate": person.get('birthDate'),
        "deathYear": person.get('deathYear'),
        "deathDate": person.get('deathDate'),
        "notable": person.get('notable', False),
        "avatarUrl": person.get('avatarUrl'),
        "area": person.get('area'),
        "cemetery": person.get('cemetery'),
        # if you store a location sub-doc it will be passed along here:
        "location": person.get('location'),
        "bio": person.get('bio'),
        "photos": person.get('photos')
    })


@application.route('/api/people/<string:person_id>', methods=['PUT'])
def update_person(person_id):
    # 1) Validate & convert the id
    try:
        oid = ObjectId(person_id)
    except Exception:
        abort(400, description="Invalid person id")

    # 2) Parse JSON body
    data = request.get_json(silent=True)
    if not data or not isinstance(data, dict):
        abort(400, description="Request must be a JSON object")

    # 3) Build the update document, only for allowed fields
    update_doc = {}
    for field, value in data.items():
        if field in ALLOWED_UPDATE_FIELDS:
            update_doc[field] = value

    if not update_doc:
        abort(400, description=f"No valid fields to update. Allowed: {', '.join(ALLOWED_UPDATE_FIELDS)}")

    # 4) Perform the update
    result = people_collection.update_one(
        {'_id': oid},
        {'$set': update_doc}
    )

    if result.matched_count == 0:
        abort(404, description="Person not found")

    # 5) Fetch and return the updated document
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
        "area": person.get('area'),
        "cemetery": person.get('cemetery'),
        "location": person.get('location'),
        "bio": person.get('bio'),
        "photos": person.get('photos')
    }), 200


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
    Повертає список унікальних назв «area»,
    що містять рядок search, нечутливих до регістру.
    """
    search = request.args.get('search', '').strip()
    query = {}

    if search:
        # частковий, нечутливий до регістру пошук по полі area
        query['area'] = {'$regex': re.escape(search), '$options': 'i'}

    # отримуємо усі унікальні значення area з нової колекції
    areas = areas_collection.distinct('area', query)

    # відфільтруємо порожні, відсортуємо та обмежимо 10 результатами
    areas = sorted([a for a in areas if a])[:10]

    return jsonify(areas)


@application.route('/api/cemeteries', methods=['GET'])
def cemeteries():
    """
    Повертає список унікальних назв кладовищ ('cemetries')
    для заданої області, з опційним пошуком по імені кладовища.
    """
    area = request.args.get('area', '').strip()
    search = request.args.get('search', '').strip()
    query = {}

    # Фільтр по області (partial, case-insensitive)
    if area:
        query['area'] = {'$regex': re.escape(area), '$options': 'i'}

    # Фільтр по назві кладовища (partial, case-insensitive)
    if search:
        # шукаємо документи, в яких хоч один елемент масива cemetries містить пошуковий рядок
        query['cemetries'] = {'$regex': re.escape(search), '$options': 'i'}

    # Повертаємо унікальний перелік назв кладовищ із відповідних масивів
    cemeteries = areas_collection.distinct('cemetries', query)

    # Приберемо пусті, відсортуємо та обмежимо 10 варіантами
    cemeteries = sorted([c for c in cemeteries if c])[:10]

    return jsonify(cemeteries)


@application.route('/api/ritual_services', methods=['GET'])
def ritual_services():
    search_query = request.args.get('search', '').strip()
    address = request.args.get('address', '').strip()

    query_filter = {}

    if search_query:
        query_filter['name'] = {'$regex': re.escape(search_query), '$options': 'i'}

    if address:
        query_filter['address'] = {'$regex': re.escape(address), '$options': 'i'}  # частковий, нечутливий до регістру пошук

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
    if request.method == 'GET':
        try:
            oid = ObjectId(ritual_service_id)
        except Exception:
            abort(400, description="Invalid ritual service id")

        ritual_service = ritual_services_collection.find_one({'_id': oid})
        if not ritual_service:
            abort(404, description="Ritual service not found")

        return jsonify({
            "id": str(ritual_service['_id']),
            "name": ritual_service.get('name'),
            "address": ritual_service.get('address'),
            "category": ritual_service.get('category'),
            'logo': ritual_service.get('logo'),
            "latitude": ritual_service.get('latitude'),
            "longitude": ritual_service.get('longitude'),
            "banner": ritual_service.get('banner'),
            "description": ritual_service.get('description'),
            "link": ritual_service.get('link'),
            "phone": ritual_service.get('phone'),
            "items": ritual_service.get('items')
        })
    elif request.method == 'PUT':
        data = request.get_json() or {}
        try:
            oid = ObjectId(ritual_service_id)
        except Exception:
            abort(400, description="Invalid ritual service id")

        ritual_service = ritual_services_collection.find_one({'_id': oid})
        if not ritual_service:
            abort(404, description="Ritual service not found")

        update_fields = {}

        # Оновлення опису
        if 'description' in data:
            update_fields['description'] = data['description']

        # Оновлення списку категорій з фото
        if 'items' in data and isinstance(data['items'], list):
            # Перевірка, що кожен елемент має правильну структуру
            valid_items = []
            for item in data['items']:
                if (
                        isinstance(item, list) and
                        len(item) == 2 and
                        isinstance(item[0], str) and
                        isinstance(item[1], list)
                ):
                    valid_items.append(item)
                else:
                    abort(400, description="Invalid format for items")
            update_fields['items'] = valid_items

        if not update_fields:
            abort(400, description="Nothing to update")

        ritual_services_collection.update_one(
            {'_id': oid},
            {'$set': update_fields}
        )

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


@application.route('/api/send-code', methods=['POST'])
def send_code():
    data = request.get_json() or {}
    phone = data.get('phone')
    if not phone:
        return jsonify({'error': 'phone is required'}), 400

    verification = twilio_client.verify \
        .services(verify_service) \
        .verifications \
        .create(to=phone, channel='sms')

    return jsonify({'status': verification.status})  # e.g. "pending"


@application.route('/api/verify-code', methods=['POST'])
def verify_code():
    data = request.get_json() or {}
    phone = data.get('phone')
    code = data.get('code')
    if not phone or not code:
        return jsonify({'error': 'phone and code are required'}), 400

    check = twilio_client.verify \
        .services(verify_service) \
        .verification_checks \
        .create(to=phone, code=code)

    if check.status == 'approved':
        return jsonify({'success': True})
    else:
        return jsonify({'success': False}), 401


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
    # Обов’язкові поля
    required = ['personId', 'name', 'cityRef', 'branchRef', 'phone', 'paymentMethod']
    missing = [k for k in required if k not in data]
    if missing:
        return jsonify({
            'error': 'Missing required fields',
            'fields': missing
        }), 400

    # Формуємо документ замовлення
    order_doc = {
        'personId': data['personId'],
        'personName': data['personName'],
        'name': data['name'],
        'cityRef': data['cityRef'],
        'cityName': data['cityName'],
        'branchRef': data['branchRef'],
        'branchDesc': data['branchDesc'],
        'phone': data['phone'],
        'paymentMethod': data['paymentMethod'],  # 'online' або 'cod'
        'paymentStatus': 'pending' if data['paymentMethod'] == 'online' else 'cod',
        'createdAt': datetime.utcnow(),
        'invoiceId': data['invoiceId']
        # сюди пізніше Monopay вебхук може додати поля status, webhookData тощо
    }

    try:
        result = orders_collection.insert_one(order_doc)
    except Exception as e:
        return jsonify({
            'error': 'DB insert failed',
            'details': str(e)
        }), 500

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
        'text': "Вітаємо! Чим можемо допомогти?",
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
        message_collection.insert_one(followup)
        socketio.emit('newMessage', {
            'sender': followup['sender'],
            'text': followup['text'],
            'imageData': followup['imageData'],
            'createdAt': followup['createdAt'].isoformat()
        }, room=chat_id)

    return jsonify({'success': True}), 201


@socketio.on('joinRoom')
def handle_join(room):
    """Called by client/admin: socket.emit('joinRoom', chatId)"""
    join_room(room)


if __name__ == '__main__':
    socketio.run(application, host='0.0.0.0', port=5000)
