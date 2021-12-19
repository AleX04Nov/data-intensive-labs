import json
import pprint
import random
import time
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.cluster import Session
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table, drop_keyspace, create_keyspace_simple, drop_table
from cassandra.cqlengine.query import ContextQuery, BatchQuery
from cassandra.cqlengine.models import QuerySetDescriptor

from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.metadata import TableMetadata

from lab4.src.items import Item
from lab4.src.items_fake import ItemFake, init_fake_items, Phone, TV, Microphone, Display
from lab4.src.orders import Order
from lab4.src.orders_fake import OrdersFake

from faker import Faker

fake = Faker()

'''
def initDBConnection(ip, keyspace=None):
    cluster = Cluster(ip)
    session = cluster.connect(keyspace=keyspace)
    return (cluster, session)


def closeConnection(cluster: Cluster):
    cluster.shutdown()


# Get Current Keyspaces
def getExistingKeyspaces(session: Session):
    # Get keyspaces 1st method
    # result = session.execute("desc keyspaces;")
    # Get keyspaces 2nd method
    keyspaces_list = list()
    result = session.execute("SELECT * FROM system_schema.keyspaces;")
    for row in result:
        keyspaces_list.append(row.keyspace_name)
    return keyspaces_list


# Drop Existing Keyspace
def dropExistingKeyspace(session: Session, keyspace: str):
    request = f"DROP KEYSPACE IF EXISTS {keyspace};"
    result = session.execute(request)
    return result


def createKeyspace(session: Session, keyspace: str):
    request = f"CREATE KEYSPACE IF NOT EXISTS {keyspace} " \
              "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };"
    result = session.execute(request)
    return result
'''


def initDBCluster(ip, keyspace=None):
    cluster = Cluster(ip)
    session = cluster.connect(keyspace=keyspace)
    return (cluster, session)


def closeCluster(cluster: Cluster):
    cluster.shutdown()


def initDBConnection(name, session: Session):
    connection.register_connection(name, session=session, default=True)
    return


def closeConnection(name):
    connection.unregister_connection(name)
    return


# Get Current Keyspaces
def getExistingKeyspaces(session: Session):
    # Get keyspaces 1st method
    # result = session.execute("desc keyspaces;")
    # Get keyspaces 2nd method
    keyspaces_list = list()
    result = session.execute("SELECT * FROM system_schema.keyspaces;")
    for row in result:
        keyspaces_list.append(row.keyspace_name)
    return keyspaces_list


# Drop Existing Keyspace
def dropExistingKeyspace(keyspace: str):
    drop_keyspace(keyspace)
    return


def createKeyspaceSimple(keyspace: str, replication_factor):
    create_keyspace_simple(keyspace, replication_factor=replication_factor)
    return


def createMapIndexes(session: Session, keyspace):
    request1 = f"CREATE INDEX IF NOT EXISTS item_info_keys_idx ON {keyspace}.item(keys(info));"
    request2 = f"CREATE INDEX IF NOT EXISTS item_info_entries_idx ON {keyspace}.item(entries(info));"
    session.execute(request1)
    session.execute(request2)


def create_items(items_count=1):
    init_fake_items()
    classes_list = [Phone, TV, Microphone, Display]
    #classes_list = [Phone, TV]
    items_list = list()
    for i in range(items_count):
        items_list.append(random.choice(classes_list)())
    return items_list


def db_add_items(items):
    items_dicts = list()
    ids = 0
    for item in items:
        item.id = ids
        ids += 1
        item_dict = item.__dict__
        item_dict['info'] = item.info
        items_dicts.append(item_dict)
    with BatchQuery() as b:
        for item_dict in items_dicts:
            Item.batch(b).create(**item_dict)
    sync_table(Item)


def create_orders(orders_count=1):
    items = Item.objects.all().values_list('id', 'price')
    orders_list = list()
    names_list = list()
    for name in range(int(orders_count / 3) + 1):
        names_list.append(fake.first_name() + " " + fake.last_name())
    ids = 0
    for order in range(orders_count):
        items_list = list()
        for item in range(random.randint(1, 10)):
            items_list.append(random.choice(items))
        orders_list.append(OrdersFake(random.choice(names_list), items_list, ids))
        ids += 1

    return orders_list


def db_add_orders(orders):
    orders_dicts = list()
    ids = 0
    for order in orders:
        orders_dicts.append(order.__dict__)
    with BatchQuery() as b:
        for order_dict in orders_dicts:
            Order.batch(b).create(**order_dict)
    sync_table(Order)


def describe_table(cluster: Cluster, table):
    return cluster.metadata.keyspaces[table.__keyspace__].tables[table.__table_name__].export_as_string()




def main():
    myKeyspace = "lab4_keyspace"
    CONNS = ['connection1']
    KEYSPACES = [myKeyspace]
    cluster, session = initDBCluster(["127.0.0.1"])

    """
    
    
    dropExistingKeyspace(session, myKeyspace)
    createKeyspace(session, myKeyspace)
    session.set_keyspace(myKeyspace)
    if session.keyspace != myKeyspace:
        closeConnection(cluster)
        return
    closeConnection(cluster)
    """

    initDBConnection(CONNS[0], session)

    #dropExistingKeyspace(myKeyspace)
    #createKeyspaceSimple(myKeyspace, replication_factor=1)

    Item.__keyspace__ = myKeyspace

    sync_table(Item)

    # need create indexes for Mapped values
    # because cqlengine still underdeveloped
    # THIS PROBLEM EXISTS SINCE Feb 18 2016!!!!
    createMapIndexes(session, Item.__keyspace__)

    #temp_init_local = create_items(200)
    #db_add_items(temp_init_local)

    print("#" * 25)
    print("TASK 1")
    print(describe_table(cluster, Item))

    print("#" * 25)
    print("TASK 2")
    print(Item.objects.all().limit(5))
    print("How big result without limit: ", Item.objects.all().count())
    for item in Item.objects.all().limit(5):
        print(item.json)
    
    print("#" * 25)
    print("TASK 2")
    print(Item.objects.filter(category='TV').order_by("price").limit(5))
    print("How big result without limit: ", Item.objects.filter(category='TV').order_by("price").count())
    for item in Item.objects.filter(category='TV').order_by("price").limit(5):
        print(item.json)
    print("#" * 25)
    print("TASK 3.1")
    print(Item.objects.filter(category='Phone').filter(model="Leon").limit(5))
    print("How big result without limit: ", Item.objects.filter(category='Phone').filter(model="Leon").count())
    for item in Item.objects.filter(category='Phone').filter(model="Leon").limit(5):
        print(item.json)
    print("#" * 25)
    print("TASK 3.2")
    print(Item.objects.filter(category='Phone').filter(Item.price > 600).filter(Item.price <= 1500).limit(5))
    print("How big result without limit: ", Item.objects.filter(category='Phone').filter(Item.price > 600).filter(Item.price <= 1500).count())
    for item in Item.objects.filter(category='Phone').filter(Item.price > 600).filter(Item.price <= 1500).limit(5):
        print(item.json)
    print("#" * 25)
    print("TASK 3.3")
    print(Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons").limit(5))
    print("How big result without limit: ", Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons").count())
    for item in Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons").limit(5):
        print(item.json)

    # ALL 4th TASKS ARE GOING TO BE
    # PROCESSED VIA SESSION.EXECUTE
    # BECAUSE CQLENGINE STILL CAN`T
    # PROCESS MAPPED INPUTS
    print("#" * 25)
    print("TASK 4.1")
    request = f"Select * from {Item.__keyspace__}.{Item.__table_name__} Where info contains key 'has5g' LIMIT 5;"
    request_count = f"Select COUNT(*) from {Item.__keyspace__}.{Item.__table_name__} Where info contains key 'has5g';"
    print(request)
    print("How big result without limit: ", session.execute(request_count).one()['count'])
    for item in session.execute(request):
        json_dict = dict(item)
        json_dict["info"] = dict(item["info"])
        print(json.dumps(json_dict, indent=4))
    print("#" * 25)
    print("TASK 4.2")
    request = f"Select * from {Item.__keyspace__}.{Item.__table_name__} Where info['has5g']='True' LIMIT 5;"
    request_count = f"Select COUNT(*) from {Item.__keyspace__}.{Item.__table_name__} Where info['has5g']='True';"
    print(request)
    print("How big result without limit: ", session.execute(request_count).one()['count'])
    for item in session.execute(request):
        json_dict = dict(item)
        json_dict["info"] = dict(item["info"])
        print(json.dumps(json_dict, indent=4))

    print("#" * 25)
    print("TASK 5.1")
    print(Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons"))
    for item in Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons"):
        inverted = not bool(item.info['has5g'] == "True")
        Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons").update(info__update={'has5g': str(inverted)})
    sync_table(Item)
    for item in Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons"):
        print(item.json)
    print("#" * 25)
    print("TASK 5.2")
    print(Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons"))
    for item in Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons"):
        Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons").update(
            info__update={'WOW_EFFECT': "OMG 100/12!!"})
    sync_table(Item)
    for item in Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons"):
        print(item.json)
    print("#" * 25)
    print("TASK 5.3")
    print(Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons"))
    for item in Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons"):
        Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons").update(
            info__remove={'WOW_EFFECT'})
    sync_table(Item)
    for item in Item.objects.filter(category='Phone').filter(Item.price == 579).filter(producer="Davis and Sons"):
        print(item.json)

    print("#" * 25)
    print("#" * 25)
    print("#" * 25)
    print("TASK 1")
    #Order.__keyspace__ = myKeyspace
    #drop_table(Order)
    #sync_table(Order)
    Order.__keyspace__ = myKeyspace
    sync_table(Order)
    print(describe_table(cluster, Order))

    #temp_init_local = create_orders(44)
    #db_add_orders(temp_init_local)

    print("#" * 25)
    print("TASK 2")
    print(Order.objects.filter(customer_name="Ashley Wolf").order_by("date").limit(5))
    print("How big result without limit: ", Order.objects.filter(customer_name="Ashley Wolf").order_by("date").count())
    for order in Order.objects.filter(customer_name="Ashley Wolf").order_by("date").limit(5):
        print(order.json)

    print("#" * 25)
    print("TASK 3")
    print(Order.objects.filter(customer_name="Ashley Wolf").filter(items_id__contains=55).limit(5))
    print("How big result without limit: ", Order.objects.filter(customer_name="Ashley Wolf").filter(items_id__contains=55).count())
    for order in Order.objects.filter(customer_name="Ashley Wolf").filter(items_id__contains=55).limit(5):
        print(order.json)

    print("#" * 25)
    print("TASK 4")
    print(Order.objects.filter(customer_name="Ashley Wolf").filter(Order.date > datetime(2020, 6, 6)).limit(5))
    print("How big result without limit: ",
          Order.objects.filter(customer_name="Ashley Wolf").filter(Order.date > datetime(2020, 6, 6)).count())
    for order in Order.objects.filter(customer_name="Ashley Wolf").filter(Order.date > datetime(2020, 6, 6)).limit(5):
        print(order.json)

    print("#" * 25)
    print("TASK 5")
    request = f"SELECT customer_name, AVG(sum) FROM {Order.__keyspace__}.{Order.__table_name__} GROUP BY customer_name LIMIT 5"
    print(request)
    for order in session.execute(request):
        json_dict = dict(order)
        print(json.dumps(json_dict, indent=4))

    print("#" * 25)
    print("TASK 6")
    request = f"SELECT customer_name, SUM(sum) FROM {Order.__keyspace__}.{Order.__table_name__} GROUP BY customer_name LIMIT 5"
    print(request)
    for order in session.execute(request):
        json_dict = dict(order)
        print(json.dumps(json_dict, indent=4))

    print("#" * 25)
    print("TASK 7")
    request = f"SELECT customer_name, MAX(sum) FROM {Order.__keyspace__}.{Order.__table_name__} GROUP BY customer_name LIMIT 5"
    print(request)
    for order in session.execute(request):
        json_dict = dict(order)
        print(json.dumps(json_dict, indent=4))

    print("#" * 25)
    print("TASK 8")
    print(Order.objects.filter(customer_name="Laura Rubio").filter(date=datetime(2020, 3, 5)).filter(id=29))
    for order in Order.objects.filter(customer_name="Laura Rubio").filter(date=datetime(2020, 3, 5)).filter(id=29):
        print("BEFORE")
        print(order.json)
        Order.objects.filter(customer_name="Laura Rubio").filter(date=datetime(2020, 3, 5)).filter(id=29).update(
            items_id__append=[1], sum=order.sum + 61)
    sync_table(Item)
    for order in Order.objects.filter(customer_name="Laura Rubio").filter(date=datetime(2020, 3, 5)).filter(id=29):
        print("AFTER")
        print(order.json)

    print("#" * 25)
    print("TASK 9")
    request = f"SELECT WRITETIME(sum), id FROM {Order.__keyspace__}.{Order.__table_name__} LIMIT 5"
    print(request)
    for order in session.execute(request):
        json_dict = dict(order)
        print(json.dumps(json_dict, indent=4))

    print("#" * 25)
    print("TASK 10")
    Order.ttl(10).create(customer_name="Arooka Zooka", date=datetime(2022, 1, 1), id=9999, sum=0, items_id=[])
    print(Order.objects.filter(customer_name="Arooka Zooka").filter(date=datetime(2022, 1, 1)).filter(id=9999))
    for order in Order.objects.filter(customer_name="Arooka Zooka").filter(date=datetime(2022, 1, 1)).filter(id=9999):
        print(order.json)
    time.sleep(10)
    print("After 10 sec")
    print(Order.objects.filter(customer_name="Arooka Zooka").filter(date=datetime(2022, 1, 1)).filter(id=9999))
    for order in Order.objects.filter(customer_name="Arooka Zooka").filter(date=datetime(2022, 1, 1)).filter(id=9999):
        print(order.json)

    print("#" * 25)
    print("TASK 11")
    request = f"SELECT json * FROM {Order.__keyspace__}.{Order.__table_name__} LIMIT 1"
    print(request)
    for order in session.execute(request):
        json_dict = order["[json]"]
        print(json.dumps(json_dict, indent=4))

    print("#" * 25)
    print("TASK 12")
    name = fake.first_name() + " " + fake.first_name() + " " + fake.last_name()
    ids = random.randint(10000, 9999999)
    input_dict = {"customer_name": name, "date": datetime(2002, 2, 2), "id": ids, "items_id": [], "sum": 0}
    request = f"INSERT INTO {Order.__keyspace__}.{Order.__table_name__} JSON " \
              f"'{{\"customer_name\": \"{name}\", \"date\": \"{str(datetime(2002, 2, 2))[:-9]}\"," \
              f" \"id\": \"{ids}\", \"items_id\": {[]}, \"sum\": {0}}}' IF NOT EXISTS"
    print(request)
    session.execute(request)
    print(Order.objects.filter(customer_name=name).filter(date=datetime(2002, 2, 2)).filter(id=ids))
    for order in Order.objects.filter(customer_name=name).filter(date=datetime(2002, 2, 2)).filter(id=ids):
        print(order.json)

    closeConnection(CONNS[0])
    closeCluster(cluster)


if __name__ == "__main__":
    main()

