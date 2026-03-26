#!/usr/bin/env python3
from datetime import datetime
from pymongo import MongoClient

from location_mapper import (
    normalize_person_burial,
    person_burial_to_legacy_fields,
    normalize_cemetery_location,
    cemetery_location_to_legacy,
    normalize_church_location,
    church_location_to_legacy,
    normalize_ritual_hq_location,
    ritual_location_to_legacy,
)


MONGO_URI = 'mongodb+srv://tsbgalcontract:mymongodb26@cluster0.kppkt.mongodb.net/test'
DB_NAME = 'memoria_test'


def run():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    people = db['people']
    cemeteries = db['cemeteries']
    churches = db['churches']
    ritual_services = db['ritual_services']

    stats = {
        'people': 0,
        'cemeteries': 0,
        'churches': 0,
        'ritual_services': 0,
    }

    for doc in people.find({}, {'_id': 1, 'burial': 1, 'areaId': 1, 'area': 1, 'cemetery': 1, 'location': 1}):
        burial = normalize_person_burial(doc)
        legacy = person_burial_to_legacy_fields(burial)
        people.update_one(
            {'_id': doc['_id']},
            {'$set': {
                'burial': burial,
                'areaId': legacy['areaId'],
                'area': legacy['area'],
                'cemetery': legacy['cemetery'],
                'location': legacy['location'],
                'locationBackfillAt': datetime.utcnow(),
            }}
        )
        stats['people'] += 1

    for doc in cemeteries.find({}, {'_id': 1, 'location': 1, 'locality': 1, 'addressLine': 1, 'address': 1}):
        location = normalize_cemetery_location(doc)
        legacy = cemetery_location_to_legacy(location)
        cemeteries.update_one(
            {'_id': doc['_id']},
            {'$set': {
                'location': location,
                'locality': legacy['locality'],
                'addressLine': legacy['addressLine'],
                'address': legacy['address'],
                'locationBackfillAt': datetime.utcnow(),
            }}
        )
        stats['cemeteries'] += 1

    for doc in churches.find({}, {'_id': 1, 'location': 1, 'locality': 1, 'address': 1}):
        location = normalize_church_location(doc)
        legacy = church_location_to_legacy(location)
        churches.update_one(
            {'_id': doc['_id']},
            {'$set': {
                'location': location,
                'locality': legacy['locality'],
                'address': legacy['address'],
                'locationBackfillAt': datetime.utcnow(),
            }}
        )
        stats['churches'] += 1

    for doc in ritual_services.find({}, {'_id': 1, 'hqLocation': 1, 'address': 1, 'latitude': 1, 'longitude': 1}):
        hq_location = normalize_ritual_hq_location(doc)
        legacy = ritual_location_to_legacy(hq_location, current_doc=doc)
        ritual_services.update_one(
            {'_id': doc['_id']},
            {'$set': {
                'hqLocation': hq_location,
                'address': legacy['address'],
                'latitude': legacy['latitude'],
                'longitude': legacy['longitude'],
                'locationBackfillAt': datetime.utcnow(),
            }}
        )
        stats['ritual_services'] += 1

    print('Backfill done:', stats)


if __name__ == '__main__':
    run()
