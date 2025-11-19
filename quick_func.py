from pymongo import MongoClient

# adjust if you want to reuse the existing client from app.py instead
client = MongoClient('mongodb+srv://tsbgalcontract:mymongodb26@cluster0.kppkt.mongodb.net/test')
db = client['memoria_test']
people_collection = db['people']

SAMBYR_GEONAME_ID = "694864"  # e.g. "695344"
LVIV_GEONAME_ID = "702550"  # e.g. "702550"


def backfill_area_ids():
    # Самбір
    res_sambir = people_collection.update_many(
        {"area": {"$regex": "Самбір"}},
        {"$set": {"areaId": SAMBYR_GEONAME_ID}}
    )
    print(f"Updated Самбір docs: {res_sambir.modified_count}")

    # Львів
    res_lviv = people_collection.update_many(
        {"area": {"$regex": "Львів"}},
        {"$set": {"areaId": LVIV_GEONAME_ID}}
    )
    print(f"Updated Львів docs: {res_lviv.modified_count}")


backfill_area_ids()