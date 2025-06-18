from flask import Flask, jsonify, request, abort
from flask_cors import CORS
from pymongo import MongoClient
import re
from bson.objectid import ObjectId

application = Flask(__name__)
CORS(application)

client = MongoClient('mongodb+srv://tsbgalcontract:mymongodb26@cluster0.kppkt.mongodb.net/test')
db = client['memoria_test']
people_collection = db['people']


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
    query_filter = {}

    if search:
        # частковий, нечутливий до регістру пошук по полі area
        query_filter['area'] = {'$regex': re.escape(search), '$options': 'i'}

    # отримаємо усі унікальні значення area, що задовольняють фільтр
    areas = people_collection.distinct('area', query_filter)

    # відсортуємо та віддаємо максимум 10 варіантів
    areas = sorted([a for a in areas if a])[:10]

    return jsonify(areas)


@application.route('/api/cemeteries', methods=['GET'])
def cemeteries():
    """
    Повертає список унікальних назв «cemetery» для заданої області,
    з опційним пошуком по імені кладовища.
    """
    area   = request.args.get('area',   '').strip()
    search = request.args.get('search', '').strip()
    query_filter = {}

    # Фільтр по області (partial, case-insensitive)
    if area:
        query_filter['area'] = {'$regex': re.escape(area), '$options': 'i'}

    # Фільтр по назві кладовища (partial, case-insensitive)
    if search:
        query_filter['cemetery'] = {'$regex': re.escape(search), '$options': 'i'}

    # Повернемо унікальний перелік назв кладовищ, що задовольняють фільтр
    cemeteries = people_collection.distinct('cemetery', query_filter)

    # Приберемо пусті значення, відсортуємо і обмежимо 10 варіантами
    cemeteries = sorted([c for c in cemeteries if c])[:10]

    return jsonify(cemeteries)


if __name__ == '__main__':
    application.run(host='0.0.0.0')
