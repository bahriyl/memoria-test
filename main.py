from flask import Flask, jsonify, request, abort
from flask_cors import CORS
from pymongo import MongoClient
import re
from bson.objectid import ObjectId
import os
from flask import Flask, request, jsonify
from twilio.rest import Client
from dotenv import load_dotenv
import requests

load_dotenv()

application = Flask(__name__)
CORS(application)

# init Twilio client
twilio_client = Client(
    os.getenv('TWILIO_ACCOUNT_SID'),
    os.getenv('TWILIO_AUTH_TOKEN')
)
verify_service = os.getenv('TWILIO_VERIFY_SERVICE_SID')

print(os.getenv('TWILIO_ACCOUNT_SID'))

client = MongoClient('mongodb+srv://tsbgalcontract:mymongodb26@cluster0.kppkt.mongodb.net/test')
db = client['memoria_test']
people_collection = db['people']
areas_collection = db['areas']

BINANCE_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"

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
    area   = request.args.get('area', '').strip()
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


@application.route('/api/send-code', methods=['POST'])
def send_code():
    data = request.get_json() or {}
    phone = data.get('phone')
    if not phone:
        return jsonify({ 'error': 'phone is required' }), 400

    verification = twilio_client.verify \
        .services(verify_service) \
        .verifications \
        .create(to=phone, channel='sms')

    return jsonify({ 'status': verification.status })  # e.g. "pending"


@application.route('/api/verify-code', methods=['POST'])
def verify_code():
    data = request.get_json() or {}
    phone = data.get('phone')
    code  = data.get('code')
    if not phone or not code:
        return jsonify({ 'error': 'phone and code are required' }), 400

    check = twilio_client.verify \
        .services(verify_service) \
        .verification_checks \
        .create(to=phone, code=code)

    if check.status == 'approved':
        return jsonify({ 'success': True })
    else:
        return jsonify({ 'success': False }), 401


if __name__ == '__main__':
    application.run(host='0.0.0.0')
