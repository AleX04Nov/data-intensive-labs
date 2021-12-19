from datetime import datetime

import pymongo as pymongo
from bson import DBRef, ObjectId
from pymongo.database import Database

import pprint

from lab2.src.comment import Comment
from lab2.src.customer import Customer
from lab2.src.items import *
from lab2.src.orders import OrderMongo

# mongo_uri = "mongodb+srv://mongolab1:mongolab1@cluster0.h37b1.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
mongo_uri = "mongodb://localhost:27017/?retryWrites=true&w=majority"
collection_names = ['orders', 'items', 'customers']
db_name = "lab7-mongo"


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

# TASK 1
def prod_count(db: Database):
    map = """
function () {
    emit(this.producer, 1);
}
    """
    reduce = """
function (key, values) {
    return Array.sum(values);
}
    """
    # print(db['items'].map_reduce(map=map, reduce=reduce, out="prod_count"))
    print(db['items'].inline_map_reduce(map=map, reduce=reduce))

# Task 2
def prod_count_price(db: Database):
    map = """
function () {
    emit(this.producer, this.price);
}
    """
    reduce = """
function (key, values) {
    return Array.sum(values);
}
    """
    # print(db['items'].map_reduce(map=map, reduce=reduce, out="prod_count"))
    print(db['items'].inline_map_reduce(map=map, reduce=reduce))

# TASK 3
## Because i dont have customer id in orders collection
## but i have enough unique fileds: thus i can create
## hash from all unique fields so customers with same name
## but different addresses and phone numbers (other unique fields)
## would be treated as different persons
def customer_count_orders_price(db: Database):
    map = """
    function () {
        var customer_key = this.customer.name + " " + this.customer.surname
        var hash_str = customer_key + this.customer.address
        for (let phone_id = 0; phone_id < this.customer.phones.length; phone_id++){
            hash_str += this.customer.phones[phone_id].toString();
        }
        var hash_md5 = hex_md5(hash_str);
        var result = {
            name: customer_key,
            total_sum: this.total_sum
        }
        emit(hash_md5, result);
    }
        """
    reduce = """
    function (key, values) {
        var result = {
            name: values[0].name,
            total_sum: 0
        }
        for (let value_id = 0; value_id < values.length; value_id++){
            result.total_sum += values[value_id].total_sum
        }
        return result;
    }
        """

    print(db['orders'].inline_map_reduce(map=map, reduce=reduce))


# TASK 4
## Because i dont have customer id in orders collection
## but i have enough unique fileds: thus i can create
## hash from all unique fields so customers with same name
## but different addresses and phone numbers (other unique fields)
## would be treated as different persons
def customer_count_orders_price_timePeriod(db: Database):
    map = """
function () {
    var customer_key = this.customer.name + " " + this.customer.surname
    var hash_str = customer_key + this.customer.address
    for (let phone_id = 0; phone_id < this.customer.phones.length; phone_id++){
        hash_str += this.customer.phones[phone_id].toString();
    }
    var hash_md5 = hex_md5(hash_str);
    var result = {
        name: customer_key,
        total_sum: this.total_sum
    }
    emit(hash_md5, result);
}
    """
    reduce = """
function (key, values) {
    var result = {
        name: values[0].name,
        total_sum: 0
    }
    for (let value_id = 0; value_id < values.length; value_id++){
        result.total_sum += values[value_id].total_sum
    }
    return result;
}
    """
    date_limits = [
        datetime(2019, 1, 1),
        datetime(2022, 1, 1)
    ]
    query = {
        "date": {
            "$gt": date_limits[0],
            "$lt": date_limits[1]
        }
    }
    pprint.pprint(db['orders'].inline_map_reduce(map=map, reduce=reduce, query=query))

# TASK 5
def orders_avg_price(db: Database):
    map = """
function () {
    var result = {
        items_count: this.order_items_id.length,
        avg_price:  this.total_sum / this.order_items_id.length
    }
    emit(this._id, result);
}
    """
    reduce = """
function (key, values) {
    var result = {
        items_count: 0,
        avg_price: 0
    }
    for (let value_id = 0; value_id < values.length; value_id++){
        result.avg_price = (result.avg_price * result.items_count + values[value_id].avg_price * values[value_id].items_count) / (result.items_count + values[value_id].items_count)
        result.items_count += values[value_id].items_count;
    }
    return result;
}
    """
    finalize = """
function (key, reducedValue) {
    return reducedValue.avg_price;
}
    """

    print(db['orders'].inline_map_reduce(map=map, reduce=reduce, finalize=finalize))


# TASK 6
def orders_customer_avg(db: Database):
    map = """
function () {
    var customer_key = this.customer.name + " " + this.customer.surname
    var hash_str = customer_key + this.customer.address
    for (let phone_id = 0; phone_id < this.customer.phones.length; phone_id++){
        hash_str += this.customer.phones[phone_id].toString();
    }
    var hash_md5 = hex_md5(hash_str);
    
    var result = {
        name: customer_key,
        items_count: this.order_items_id.length,
        avg_price:  this.total_sum / this.order_items_id.length
    }
    emit(hash_md5, result);
    }
        """
    reduce = """
    function (key, values) {
        var result = {
            name: values[0].name,
            items_count: 0,
            avg_price: 0
        }
        for (let value_id = 0; value_id < values.length; value_id++){
            result.avg_price = (result.avg_price * result.items_count + values[value_id].avg_price * values[value_id].items_count) / (result.items_count + values[value_id].items_count)
            result.items_count += values[value_id].items_count;
        }
        return result;
    }
        """
    finalize = """
    function (key, reducedValue) {
        var result = {
            name: reducedValue.name,
            avg_price: reducedValue.avg_price
        }
        return result;
    }
        """

    print(db['orders'].inline_map_reduce(map=map, reduce=reduce, finalize=finalize))

# TASK 7
def orders_count_items(db: Database):
    map = """
function () {
    for (let item_index = 0; item_index < this.order_items_id.length; item_index++)
        emit(this.order_items_id[item_index], 1);
}
    """
    reduce = """
function (key, values) {
    return Array.sum(values);
}
    """
    finalize = """
function (key, reducedValue) {
    return reducedValue;
}
    """

    print(db['orders'].inline_map_reduce(map=map, reduce=reduce, finalize=finalize))


## TASK 8-9 Identical
## Just change number in finalize
def orders_get_items_customer(db: Database, n):
    map = """
function () {
    var customer_key = this.customer.name + " " + this.customer.surname
    var hash_str = customer_key + this.customer.address
    for (let phone_id = 0; phone_id < this.customer.phones.length; phone_id++){
        hash_str += this.customer.phones[phone_id].toString();
    }
    var customer_hash_md5 = hex_md5(hash_str);
    
    var result = {
        hashes_arr: [customer_hash_md5],
    }
    result[customer_hash_md5] = {
        name: customer_key,
        count: 1
    }
    for (let item_index = 0; item_index < this.order_items_id.length; item_index++)
        emit(this.order_items_id[item_index], result);
}
    """
    reduce = """
function (key, values) { 
    var result = {
        hashes_arr: []
        // 'somehash': {
            // name: '',
            // count: 0
        //}
    }
    for(let value_index = 0; value_index < values.length; value_index++){
        var curr_hash = values[value_index].hashes_arr[0]
        if (!result.hashes_arr.includes(curr_hash)){
            result.hashes_arr.push(curr_hash)
            result[curr_hash] = {
                name: values[value_index][curr_hash].name,
                count: values[value_index][curr_hash].count,
            }
            continue;
        }
        result[curr_hash].count += values[value_index][curr_hash].count
    }
    
    return result;
}
    """
    finalize = """
function (key, reducedValue) {
    var result = []
    for (let hash_id = 0; hash_id < reducedValue.hashes_arr.length; hash_id++){
        var curr_hash = reducedValue.hashes_arr[hash_id]
        if (reducedValue[curr_hash].count >= bigger_then){
            result.push(
                {
                    name: reducedValue[curr_hash].name,
                    count: reducedValue[curr_hash].count
                }
            );
        }
    }

    return result;
}
    """
    scope = {
        'bigger_then': n
    }

    print(db['orders'].inline_map_reduce(map=map, reduce=reduce, finalize=finalize, scope=scope))


# TASK 10
def orders_top_items(db: Database, n):
    map = """
function () {
    for (let item_index = 0; item_index < this.order_items_id.length; item_index++)
        emit(this.order_items_id[item_index], 1);
}
    """

    reduce = """
    function (key, values) {
        return Array.sum(values);
    }
        """
    finalize = """
    function (key, reducedValue) {
        return reducedValue;
    }
        """

    db['orders'].map_reduce(map=map, reduce=reduce, finalize=finalize, out="temp_collection")

    map2 = """
function () {
    if (top_orders_array.length < top_number){
        top_orders_array.push(this.value)
        top_orders_array.sort()
    }
    else if (top_orders_array[0] < this.value){
        top_orders_array.shift()
        top_orders_array.push(this.value)
        top_orders_array.sort()
    }
    emit(this._id, this.value);
}
    """
    reduce2 = """
    function (key, values) {
        if (values < top_orders_array[0])
            return null
        return values;
    }
        """

    finalize2 = """
    function (key, reducedValue) {
        if (reducedValue < top_orders_array[0])
            return null
        return reducedValue;
    }
        """

    scope2 = {
        'top_orders_array': [],
        'top_number': n
    }
    pprint.pprint(db['temp_collection'].inline_map_reduce(map=map2, reduce=reduce2, finalize=finalize2, scope=scope2))
    db['temp_collection'].drop()


# TASK 11
def map_reduce_incremental(db: Database):
    db["task11"].drop()
    map = """
    function () {
        var customer_key = this.customer.name + " " + this.customer.surname
        var hash_str = customer_key + this.customer.address
        for (let phone_id = 0; phone_id < this.customer.phones.length; phone_id++){
            hash_str += this.customer.phones[phone_id].toString();
        }
        var hash_md5 = hex_md5(hash_str);
        var result = {
            name: customer_key,
            total_sum: this.total_sum
        }
        emit(hash_md5, result);
    }
        """
    reduce = """
    function (key, values) {
        var result = {
            name: values[0].name,
            total_sum: 0
        }
        for (let value_id = 0; value_id < values.length; value_id++){
            result.total_sum += values[value_id].total_sum
        }
        return result;
    }
        """
    date_limits = [
        datetime(2020, 1, 1),
        datetime(2021, 1, 1)
    ]


    query = {
        "date": {
            "$gt": date_limits[0],
            "$lt": date_limits[1]
        }
    }
    db['orders'].map_reduce(map=map, reduce=reduce, query=query, out="task11")

    date_limits2 = [
        datetime(2021, 1, 1),
        datetime(2022, 1, 1)
    ]

    query2 = {
        "date": {
            "$gt": date_limits2[0],
            "$lt": date_limits2[1]
        }
    }
    out_params2 = {
        "reduce": "task11"
    }
    db['orders'].map_reduce(map=map, reduce=reduce, query=query2, out=out_params2)


# TASK 12
def monthly_analysis(db: Database, year):
    map = """
function () {
    var customer_key = this.customer.name + " " + this.customer.surname
    var hash_str = customer_key + this.customer.address
    for (let phone_id = 0; phone_id < this.customer.phones.length; phone_id++){
        hash_str += this.customer.phones[phone_id].toString();
    }
    var hash_md5 = hex_md5(hash_str);
    
    var curr_year = this.date.getFullYear()
    var result = {
        name: customer_key,
        month: this.date.getMonth() + 1,
        year_arr: [curr_year]
    }
    result[curr_year] = this.total_sum
    var key = result.month.toString() + "_" + hash_md5.toString();

    emit(key, result);    
}
    """
    reduce = """
function (key, values) {
    var result = {
        name: values[0].name,
        month: values[0].month,
        year_arr: []
    }
    for (let value_id = 0; value_id < values.length; value_id++){
        var curr_year = values[value_id].year_arr[0]
        if (!result.year_arr.includes(curr_year)){
            result.year_arr.push(curr_year)
            result[curr_year] = values[value_id][curr_year]
            continue;
        }
        result[curr_year] += values[value_id][curr_year]
    }
    return result;
}
    """

    finalize = """
function (key, reducedValue) {
    var result = {
        name: reducedValue.name,
        month: reducedValue.month,
        year: check_year,
        amount: 0,
        prev_year_amount: 0,
        diff: 0
    }
    if (reducedValue.year_arr.includes(check_year))
        result.amount = reducedValue[check_year]
    if (reducedValue.year_arr.includes(check_year - 1))
        result.prev_year_amount = reducedValue[check_year - 1]
    result.diff = result.amount - result.prev_year_amount
    
    return result;
}
    """

    scope = {
        'check_year': year
    }

    pprint.pprint(db['orders'].inline_map_reduce(map=map, reduce=reduce, finalize=finalize, scope=scope))


def main():
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]

    #init_lab(db, customers_count=5, items_count=20, orders_count=200)
    #init_capped_collection(db)
    #add_to_capped_collection(db)
    #prod_count(db)
    #prod_count_price(db)
    #customer_count_orders_price(db)
    #customer_count_orders_price_timePeriod(db)
    #orders_avg_price(db)
    #orders_customer_avg(db)
    #orders_count_items(db)
    #orders_get_items_customer(db, 4)
    #orders_top_items(db, 3)
    #map_reduce_incremental(db)
    monthly_analysis(db, 2021)




if __name__ == "__main__":
    main()