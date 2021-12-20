import random
import time
from typing import Mapping
from typing import Any

import pymongo
from faker import Faker
from pymongo.database import Database

mongo_uri = "mongodb://localhost:27017/?retryWrites=true&w=majority"
collection_names = ['big_collection']
db_name = "lab2-extra"
fake = Faker()


def remove_collections(db, db_collection_names):
    db_list_collection_names = db.list_collection_names()
    for collection_name in set(db_collection_names):
        if collection_name in db_list_collection_names:
            collection = db[collection_name]
            collection.drop()


def create_collections(db: Database, db_collection_names):
    db_list_collection_names = db.list_collection_names()
    for collection_name in set(db_collection_names):
        if collection_name not in db_list_collection_names:
            db.create_collection(collection_name)


def init_lab(db: Database, customers_count=1, items_count=1, orders_count=1):
    remove_collections(db, collection_names)
    create_collections(db, collection_names)


def do_lab2_extra(kwargs: Mapping[str, Any]):
    db: Database = kwargs['db']
    for i in range(100000):
        db['big_collection'].insert_one(document={'id': i, 'text': fake.sentence(nb_words=random.randint(5, 50))})


def functionTimer(function, kwargs: Mapping[str, Any]):
    startTime = time.perf_counter()
    function(kwargs)
    stopTime = time.perf_counter()

    return stopTime - startTime


def main():
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]

    init_lab(db, customers_count=5, items_count=20, orders_count=200)
    timeSpent = functionTimer(
        do_lab2_extra,
        {
            'db': db
        }
    )

    print(f"Time spent: {timeSpent} sec")





if __name__ == "__main__":
    main()