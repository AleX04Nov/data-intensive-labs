from datetime import datetime

import pymongo as pymongo
from bson import DBRef, ObjectId
from pymongo.database import Database

from lab2.src.comment import Comment
from lab2.src.customer import Customer
from lab2.src.items import *
from lab2.src.orders import OrderMongo

mongo_uri = "mongodb+srv://mongolab1:mongolab1@cluster0.h37b1.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
collection_names = ['orders', 'items', 'customers']
db_name = "lab2-mongo"


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


def create_customers(customers_count=1):
    customer_list = list()
    for customer_num in range(customers_count):
        customer_list.append(Customer())
    return customer_list


def create_items(items_count=1):
    init_fake_items()
    classes_list = [Phone, TV, Microphone, Display]
    items_list = list()
    for i in range(items_count):
        items_list.append(random.choice(classes_list)())
    return items_list


def create_orders(db: Database, orders_count=1):
    orders_list = list()
    for order_num in range(orders_count):
        customer = db['customers'].aggregate(
            [
                {
                    "$sample": {
                        "size": 1
                    }
                }
            ]
        )
        for customer in customer:
            customer = Customer(customer)
        items = db['items'].aggregate(
            [
                {
                    "$sample": {
                        "size": random.randint(1, 10)
                    }
                }
            ]
        )
        orders_list.append(OrderMongo(customer=customer, items_info=items))
    return orders_list


def db_add_customer(db: Database, customer):
    db['customers'].insert_one(document=customer.__dict__)


def db_add_customers(db: Database, customers):
    customers_dicts = list()
    for customer in customers:
        customers_dicts.append(customer.__dict__)
    db['customers'].insert_many(
        documents=customers_dicts,
        ordered=False
    )


def db_add_item(db: Database, item):
    db['items'].insert_one(document=item.__dict__)


def db_add_items(db: Database, items):
    items_dicts = list()
    for item in items:
        items_dicts.append(item.__dict__)
    db['items'].insert_many(
        documents=items_dicts,
        ordered=False
    )


def db_add_order(db: Database, order):
    db['orders'].insert_one(document=order.__dict__)


def db_add_orders(db: Database, orders):
    orders_dicts = list()
    for order in orders:
        orders_dicts.append(order.__dict__)
    db['orders'].insert_many(
        documents=orders_dicts,
        ordered=False
    )


def init_lab(db: Database, customers_count=1, items_count=1, orders_count=1):
    remove_collections(db, collection_names)
    create_collections(db, collection_names)

    temp_init_local = create_customers(customers_count)
    db_add_customers(db, temp_init_local)

    temp_init_local = create_items(items_count)
    db_add_items(db, temp_init_local)

    temp_init_local = create_orders(db, orders_count)
    db_add_orders(db, temp_init_local)


def get_all_items(db: Database):
    result = db['items'].find({})
    for item in result:
        print(item)


def get_items_in_category_count(db: Database, category):
    result = db['items'].count_documents(
        {
            "category": category
        }
    )
    print(result)


# db.items.distinct('category').length
def get_items_category_count(db: Database):
    result = len(db['items'].distinct("category"))
    print(result)


# db.items.distinct('producer')
def get_items_producers(db: Database):
    result = db['items'].distinct("producer")
    print(result)


# { "category" : "Microphone", "price" : { $gt : NumberInt(100), $lt : NumberInt(300) } }
# { $or : [ { "model" : "Reed" }, { "model" : "Johnson" } ] }
# { producer: { $in: [ 'Castro PLC', 'Webb and Sons' ] } }
def get_items_criteria(db: Database):
    category = "Microphone"
    price_limits = [100, 300]
    model_list = ["Reed", "Johnson"]
    producer_list = [ 'Castro PLC', 'Webb and Sons' ]
    queries_list = [
        {
            "category": category,
            "price":
                {
                    "$gt": price_limits[0],
                    "$lt": price_limits[1]
                }
        },
        {
            "$or":
                [
                    {
                        "model": model_list[0]
                    },
                    {
                        "model": model_list[1]
                    }
                ]
        },
        {
            "producer":
                {
                    "$in":
                        [
                            producer_list[0],
                            producer_list[1]
                        ]
                }
        }
    ]
    for query in queries_list:
        result = db['items'].find(query)
        for item in result:
            print(item)
        print("="*20)


# db.items.updateMany({ "category" : "TV", $or : [ { "model" : "Yu" }, { "model" : "Brown" } ] }, {"$inc": { "price": NumberInt(1) }, "$set": { "bluetooth": true, "wifi": false} } )
def update_items_by_criteria(db: Database):
    model = "Wallace"
    queries_list = [
        {
            "model": model
        },
        {
            "category": "TV",
            "$or":
                [
                    {"model": "Yu"},
                    {"model": "Brown"}
                ]
        }
    ]
    print("Before Update Query")
    result = db['items'].find(queries_list[1])
    for item in result:
        print(item)

    result = db['items'].update_many(
        queries_list[1],
        {
            "$inc":
                {
                    "price": 1
                },
            "$set":
                {
                    "bluetooth": True,
                    "wifi": False
                }
        }
    )

    print("=" * 40)
    print("After Update Query")
    result = db['items'].find(queries_list[1])
    for item in result:
        print(item)


# { "has4g" : { $exists : true } }
def get_item_has_property(db: Database):
    property = "has4g"
    query = {
        "has4g":
            {
                "$exists": True
            }
    }
    result = db['items'].find(query)
    for item in result:
        print(item)

# db.items.updateMany({ "model" : "Anderson" }, {"$inc": { "price": NumberInt(1) } } )
def update_price_by_one(db: Database):
    model = "Anderson"
    query = {
        "model": model
    }
    print("Before Update Query")
    result = db['items'].find(query)
    for item in result:
        print(item)
    result = db['items'].update_many(
        query,
        {
            "$inc":
                {
                    "price": 1
                }
        }
    )
    print("="*40)
    print("After Update Query")
    result = db['items'].find(query)
    for item in result:
        print(item)


def get_all_orders(db: Database):
    result = db['orders'].find({})
    for order in result:
        print(order)


def get_all_orders_price_higher(db: Database):
    query = {
        "total_sum":
            {
                "$gt": 50000
            }
    }
    result = db['orders'].find(query)
    for order in result:
        print(order)


def get_orders_by_customer(db: Database):
    customer_name = "Kimberly"
    customer_surname = "Ross"
    query = {
        "customer.name": customer_name,
        "customer.surname": customer_surname
    }
    result = db['orders'].find(query)
    for order in result:
        print(order)


def get_orders_by_item_id(db: Database):
    ref = "items"
    item_id = "616a2ac70d5b60eaf10d5787"

    query = {
        "order_items_id": {
            "$ref": ref,
            "$id": ObjectId(item_id)
        }
    }
    # {"order_items_id": DBRef("items", ObjectId("616a2ac70d5b60eaf10d5787"))}
    result = db['orders'].find(query)
    for order in result:
        print(order)


def update_orders_add_item(db: Database):
    ref = "items"
    item_id = "616a2ac70d5b60eaf10d5787"

    query = {
        "order_items_id": {
            "$ref": ref,
            "$id": ObjectId(item_id)
        }
    }
    print("Before Updating:")
    result = db['orders'].find(query)
    for order in result:
        print(order)

    new_item = db['items'].aggregate(
        [
            {
                "$sample": {
                    "size": 1
                }
            }
        ]
    )
    for new_item in new_item:
        new_item = new_item

    modifier = {
        "$inc":
            {
                "total_sum": new_item.get("price")
            },
        "$addToSet":
            {
                "order_items_id": {
                    "$ref": ref,
                    "$id": new_item.get('_id')
                }
            }
    }

    result = db['orders'].update_many(query, modifier)

    print("="*40, "\nAfter Updating:")
    result = db['orders'].find(query)
    for order in result:
        print(order)


def get_items_count_in_order(db: Database):
    ref = "items"
    item_id = "616a2ac70d5b60eaf10d5787"

    query = {
        "order_items_id": {
            "$ref": ref,
            "$id": ObjectId(item_id)
        }
    }
    result = db['orders'].aggregate(
        [
            {
                "$match": query
            },
           {
                "$project": {
                    "count": {"$size": "$order_items_id"}
                }
           }
        ]
    )
    for order in result:
        print(order)


def get_order_customer_by_price(db: Database):
    query = {
        "total_sum":
            {
                "$gt": 50000
            }
    }
    specified_fields = {
        "customer": 1,
        "payment.card_id": 1,
        "_id": 0
    }
    result = db['orders'].find(query, specified_fields)
    for order in result:
        print(order)


def remove_item_from_order_by_date(db: Database):
    query = {
        "date":
            {
                "$gte": datetime(2020, 10, 25),
                "$lte": datetime(2020, 12, 25)
            }
    }
    print("Before Update")
    result = db['orders'].find(query)
    for order in result:
        print(order)

    specified_fields = {
        "order_items_id": {
            '$arrayElemAt': ['$order_items_id', 0]
        },
        "_id": 0
    }
    result = db['orders'].find(query, specified_fields)
    item_dbref: DBRef
    for order in result:
        print(order)
        item_dbref = order.get('order_items_id')
        query_item = {
            "_id": item_dbref.id
        }
        item = db[item_dbref.collection].find_one(query_item, {"price": 1, "_id": 0})
        print(item)

    modifier = {
        "$inc":
            {
                "total_sum": -item.get("price")
            },
        "$pull":
            {
                "order_items_id": {
                    "$ref": item_dbref.collection,
                    "$id": item_dbref.id
                }
            }
    }

    result = db['orders'].update_many(query, modifier)
    print(result)

    print("="*40, "\nAfter Update")
    result = db['orders'].find(query)
    for order in result:
        print(order)


def rename_in_all_orders(db: Database):
    from faker import Faker
    fake = Faker()

    print("\nBefore Update")
    result = db['orders'].find({})
    for order in result:
        print("order")
        print(order)

    modifier = {
        "$set": {
            "customer.name": fake.first_name()
        }
    }
    result = db['orders'].update_many({}, modifier)

    print("=" * 40, "\nAfter Update")
    result = db['orders'].find({})
    for order in result:
        print(order)


def plus_two_balls(db: Database):
    customer_name = "Neil"
    customer_surname = "Kim"
    query = {
        "customer.name": customer_name,
        "customer.surname": customer_surname
    }
    result = db['orders'].aggregate(
        [
            {
                "$match": query
            },
            {
                "$unwind": {"path": "$order_items_id"}
            },
            {
                "$lookup": {
                    "from": "items",
                    "localField": "order_items_id.$id",
                    "foreignField": "_id",
                    "as": "items"
                }
            },
            {"$unwind": "$items"},
            {
                "$group": {
                    "_id": "$_id",
                    "customer": {
                        "$first": "$customer"
                    },
                    "items": {"$push": "$items"}
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "customer": 1,
                    "items": {
                        "model": 1,
                        "category": 1,
                        "producer": 1,
                        "price": 1
                    }
                }
            }
        ]
    )
    for order in result:
        print(order)


def init_capped_collection(db: Database):
    db_list_collection_names = db.list_collection_names()
    if 'comments' in db_list_collection_names:
        collection = db['comments']
        collection.drop()

    db_list_collection_names = db.list_collection_names()
    if 'comments' not in db_list_collection_names:
        db.create_collection(
            'comments',
            capped=True,
            size=10000,
            max=5
        )

    comments_list = list()
    for i in range(5):
        comments_list.append(Comment().__dict__)
    db['comments'].insert_many(
        documents=comments_list,
        ordered=False
    )


def add_to_capped_collection(db: Database):
    db['comments'].insert_one(document=Comment().__dict__)




def main():
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]

    #init_lab(db, customers_count=5, items_count=20, orders_count=13)
    #get_all_items(db)
    #get_items_in_category_count(db, "Microphone")
    #get_items_category_count(db)
    #get_items_producers(db)
    #get_items_criteria(db)
    #get_item_has_property(db)
    #update_price_by_one(db)
    #update_items_by_criteria(db)
    #get_all_orders(db)
    #get_all_orders_price_higher(db)
    #get_orders_by_customer(db)
    #get_orders_by_item_id(db)
    #update_orders_add_item(db)
    #get_items_count_in_order(db)
    #get_order_customer_by_price(db)
    #remove_item_from_order_by_date(db)
    #rename_in_all_orders(db)
    #plus_two_balls(db)
    #init_capped_collection(db)
    add_to_capped_collection(db)



if __name__ == "__main__":
    main()