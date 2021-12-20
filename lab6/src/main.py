import time

import cassandra
from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table, drop_keyspace, create_keyspace_simple, drop_table
from cassandra.cqlengine.query import ContextQuery, BatchQuery
from cassandra.cqlengine.models import QuerySetDescriptor

from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table

from lab6.src.LabTable import LabTable


def main():
    default_keyspaces = ['system_auth', 'system_schema', 'system_views',
                         'system', 'system_distributed',  'system_traces',
                         'system_virtual_schema']
    CONNS = ['cl1', 'cl2', 'cl3']
    CLUSTERS = {'cl1': None, 'cl2': None, 'cl3': None}
    SESSIONS = {'cl1': None, 'cl2': None, 'cl3': None}
    KEYSPACES = ['lab6_ks1', 'lab6_ks2', 'lab6_ks3']
    IPS = ['127.0.0.1']
    PORTS = {'cl1': 9051, 'cl2': 9052, 'cl3': 9053}

    while(True):
        print("Trying to connect to Cluster 1")
        try:
            CLUSTERS['cl1'] = Cluster(IPS, port=PORTS['cl1'])
            SESSIONS['cl1'] = CLUSTERS['cl1'].connect()
            connection.register_connection('cl1', session=SESSIONS['cl1'])
            print("Cluster 1 Connected Successfully")
            break
        except Exception:
            print("Cluster 1 Connection Error")
            continue

    print("Removing old keyspaces with Cluster 1")
    for keyspace in KEYSPACES:
        while True:
            try:
                drop_keyspace(keyspace, connections=[CONNS[0]])
                break
            except cassandra.OperationTimedOut:
                continue
            except cassandra.cqlengine.CQLEngineException:
                break
            except cassandra.InvalidRequest:
                break

    connection.unregister_connection('cl1')
    CLUSTERS['cl1'].shutdown()

    while (True):
        print("Trying to connect to Cluster 2")
        try:
            CLUSTERS['cl2'] = Cluster(IPS, port=PORTS['cl2'])
            SESSIONS['cl2'] = CLUSTERS['cl2'].connect()
            connection.register_connection('cl2', session=SESSIONS['cl2'])
            print("Cluster 2 Connected Successfully")
            break
        except Exception:
            print("Cluster 2 Connection Error")
            continue

    time.sleep(5)
    print("Add new keyspaces with Cluster 2")
    rep_factor = 0
    for keyspace in KEYSPACES:
        rep_factor += 1
        already_exists = 0
        while already_exists < 5:
            try:
                create_keyspace_simple(keyspace, connections=[CONNS[1]], replication_factor=rep_factor)
                break
            except cassandra.OperationTimedOut:
                continue
            except cassandra.AlreadyExists:
                already_exists += 1
                continue

    connection.unregister_connection('cl2')
    CLUSTERS['cl2'].shutdown()

    while (True):
        print("Trying to connect to Cluster 3")
        try:
            CLUSTERS['cl3'] = Cluster(IPS, port=PORTS['cl3'])
            SESSIONS['cl3'] = CLUSTERS['cl3'].connect()
            connection.register_connection('cl3', session=SESSIONS['cl3'])
            print("Cluster 3 Connected Successfully")
            break
        except Exception:
            print("Cluster 3 Connection Error")
            continue

    time.sleep(5)
    print("Add new Tables with Cluster 3")
    #for keyspace in KEYSPACES:
    while True:
        try:
            sync_table(LabTable, keyspaces=KEYSPACES, connections=[CONNS[2]])
            break
        except cassandra.OperationTimedOut:
            continue
        except cassandra.AlreadyExists:
            break
        except cassandra.cqlengine.CQLEngineException:
            print('cassandra.cqlengine.CQLEngineException')
            time.sleep(5)
            continue

    connection.unregister_connection('cl3')
    CLUSTERS['cl3'].shutdown()

    while (True):
        print("Trying to connect to Cluster 1")
        try:
            CLUSTERS['cl1'] = Cluster(IPS, port=PORTS['cl1'])
            SESSIONS['cl1'] = CLUSTERS['cl1'].connect()
            connection.register_connection('cl1', session=SESSIONS['cl1'])
            print("Cluster 1 Connected Successfully")
            break
        except Exception:
            print("Cluster 1 Connection Error")
            continue



    time.sleep(5)
    print("Reading how keyspaces and tables created across clusters")
    print("Cluster 1:")
    keyspaces_in_cluster = CLUSTERS['cl1'].metadata.keyspaces.keys()
    keyspaces_in_cluster = [keyspace for keyspace in keyspaces_in_cluster if keyspace not in default_keyspaces]
    print(keyspaces_in_cluster)
    for keyspace in keyspaces_in_cluster:
        print(f"In {keyspace}:")
        tables = CLUSTERS['cl1'].metadata.keyspaces[keyspace].tables
        print(tables.keys())

    connection.unregister_connection('cl1')
    CLUSTERS['cl1'].shutdown()

    while (True):
        print("Trying to connect to Cluster 2")
        try:
            CLUSTERS['cl2'] = Cluster(IPS, port=PORTS['cl2'])
            SESSIONS['cl2'] = CLUSTERS['cl2'].connect()
            connection.register_connection('cl2', session=SESSIONS['cl2'])
            print("Cluster 2 Connected Successfully")
            break
        except Exception:
            print("Cluster 2 Connection Error")
            continue



    print("Cluster 2:")
    keyspaces_in_cluster = CLUSTERS['cl2'].metadata.keyspaces.keys()
    keyspaces_in_cluster = [keyspace for keyspace in keyspaces_in_cluster if keyspace not in default_keyspaces]
    print(keyspaces_in_cluster)
    for keyspace in keyspaces_in_cluster:
        print(f"In {keyspace}:")
        tables = CLUSTERS['cl2'].metadata.keyspaces[keyspace].tables
        print(tables.keys())

    connection.unregister_connection('cl2')
    CLUSTERS['cl2'].shutdown()

    while (True):
        print("Trying to connect to Cluster 3")
        try:
            CLUSTERS['cl3'] = Cluster(IPS, port=PORTS['cl3'])
            SESSIONS['cl3'] = CLUSTERS['cl3'].connect()
            connection.register_connection('cl3', session=SESSIONS['cl3'])
            print("Cluster 3 Connected Successfully")
            break
        except Exception:
            print("Cluster 3 Connection Error")
            continue
    print("Cluster 3:")
    keyspaces_in_cluster = CLUSTERS['cl3'].metadata.keyspaces.keys()
    keyspaces_in_cluster = [keyspace for keyspace in keyspaces_in_cluster if keyspace not in default_keyspaces]
    print(keyspaces_in_cluster)
    for keyspace in keyspaces_in_cluster:
        print(f"In {keyspace}:")
        tables = CLUSTERS['cl3'].metadata.keyspaces[keyspace].tables
        print(tables.keys())

    connection.unregister_connection('cl3')
    CLUSTERS['cl3'].shutdown()


    """
    # CLOSING ALL CONNECTIONS
    connection.unregister_connection('cl1')
    connection.unregister_connection('cl2')
    connection.unregister_connection('cl3')
    CLUSTERS['cl1'].shutdown()
    CLUSTERS['cl2'].shutdown()
    CLUSTERS['cl3'].shutdown()
    """

    return 0


if __name__ == "__main__":
    main()
