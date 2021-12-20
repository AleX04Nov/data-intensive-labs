import random
import time
from typing import Mapping, Any

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, Session
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import drop_keyspace, create_keyspace_simple, sync_table
from cassandra.query import SimpleStatement
from faker import Faker

from lab2_extra_100k.cASSandra.src.LabTable import LabTable

fake = Faker()


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


# Drop Existing Keyspace
def dropExistingKeyspace(keyspace: str):
    drop_keyspace(keyspace)
    return


def createKeyspaceSimple(keyspace: str, replication_factor):
    create_keyspace_simple(keyspace, replication_factor=replication_factor)
    return


def db_add_rows(kwargs: Mapping[str, Any]):
    for i in range(100_000):
        row_dict = {'id': i, 'text': fake.sentence(nb_words=random.randint(5, 50))}
        LabTable.create(**row_dict)
    sync_table(LabTable)


def db_add_rows_insert(kwargs: Mapping[str, Any]):
    session: Session = kwargs['session']
    keyspace_name: str = kwargs['keyspace']
    table_name: str = kwargs['table']
    for i in range(100_000):
        query = SimpleStatement(f"INSERT INTO {keyspace_name}.{table_name} (id, text) VALUES (%s, %s)")
        session.execute(query, (i, fake.sentence(nb_words=random.randint(5, 50))))


def functionTimer(function, kwargs: Mapping[str, Any]):
    startTime = time.perf_counter()
    function(kwargs)
    stopTime = time.perf_counter()
    return stopTime - startTime


def main():
    myKeyspace = "lab2_extra_keyspace"
    CONNS = ['connection1']
    KEYSPACES = [myKeyspace]
    cluster, session = initDBCluster(["127.0.0.1"])

    initDBConnection(CONNS[0], session)

    dropExistingKeyspace(myKeyspace)
    createKeyspaceSimple(myKeyspace, replication_factor=1)

    LabTable.__keyspace__ = myKeyspace
    sync_table(LabTable)

    #timeSpent = functionTimer(
    #    db_add_rows_insert,
    #    {
    #        'session': session,
    #        'keyspace': LabTable.__keyspace__,
    #        'table': LabTable.__table_name__
    #    }
    #)
    timeSpent = functionTimer(
        db_add_rows, {}
    )
    print(f"Time spent: {timeSpent} sec") # 1911.87 sec

    print("How much records do we have: ", LabTable.objects.filter().count())

    closeConnection(CONNS[0])
    closeCluster(cluster)


if __name__ == "__main__":
    main()
