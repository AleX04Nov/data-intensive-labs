import sqlite3, time, threading
from typing import Any
from typing import Mapping


def prepareDB(dbName: str):
    con = sqlite3.connect(dbName, timeout=600)
    cur = con.cursor()
    cur.execute(f"DELETE FROM main;")
    cur.execute("UPDATE SQLITE_SEQUENCE SET SEQ=0 WHERE NAME='main';")
    con.commit()
    con.close()
    return


def oneThread(kwargs: Mapping[str, Any]):
    dbName: str
    valuesCount: int
    dbName = kwargs['dbName']
    valuesCount = kwargs['valuesCount']
    con = sqlite3.connect(dbName, timeout=600)
    cur = con.cursor()
    for value in range(valuesCount):
        cur.execute(f"INSERT INTO main (value) VALUES ({value})")
    con.commit()
    con.close()
    return


def manyThreads(kwargs: Mapping[str, Any]):
    dbName: str
    valuesCount: int
    threadsCount: int
    threadsList: list
    valuesPerThread: int
    startValue: int
    endValue: int
    synchronizedStart: threading.Event
    newThread: threading.Thread
    threadArgs: Mapping[str, Any]

    dbName = kwargs['dbName']
    valuesCount = kwargs['valuesCount']
    threadsCount = kwargs['threadsCount']
    synchronizedStart = threading.Event()
    threadsList = list()
    valuesPerThread = int(valuesCount / threadsCount)
    startValue = 0

    for threadId in range(threadsCount - 1):

        threadArgs = {
                'dbName': dbName,
                'startValue': startValue,
                'endValue': startValue + valuesPerThread,
                'synchronizedStart': synchronizedStart
            }
        newThread = threading.Thread(
            target=manyThreadsThread,
            args=(threadArgs,)
        )
        threadsList.append(newThread)
        newThread.start()
        startValue += valuesPerThread

    threadArgs = {
            'dbName': dbName,
            'startValue': startValue,
            'endValue': valuesCount,
            'synchronizedStart': synchronizedStart
        }
    newThread = threading.Thread(
        target=manyThreadsThread,
        args=(threadArgs,)
    )
    threadsList.append(newThread)
    newThread.start()

    # Set Event to begin to work with DB simultaneously
    synchronizedStart.set()

    # Wait till every thread done with its job
    for thread in threadsList:
        thread.join()

    return


def manyThreadsThread(kwargs: Mapping[str, Any]):
    dbName: str
    startValue: int
    endValue: int
    synchronizedStart: threading.Event

    dbName = kwargs['dbName']
    startValue = kwargs['startValue']
    endValue = kwargs['endValue']
    synchronizedStart = kwargs['synchronizedStart']
    synchronizedStart.wait()

    con = sqlite3.connect(dbName, timeout=600)
    cur = con.cursor()
    for value in range(startValue, endValue):
        cur.execute(f"INSERT INTO main (value) VALUES ({value})")
    con.commit()
    con.close()
    return


def functionTimer(function, kwargs: Mapping[str, Any]):
    startTime = time.perf_counter()
    function(kwargs)
    stopTime = time.perf_counter()

    return stopTime - startTime


def main():
    prepareDB('../data/databaseA.db')
    prepareDB('../data/databaseB.db')

    requestsCount = 100000
    timeSpentOnOneThread = functionTimer(
        oneThread,
        {
            'dbName': '../data/databaseA.db',
            'valuesCount': requestsCount
        }
    )
    print(f"Time spent on one thread with {requestsCount} requests to database: {timeSpentOnOneThread} sec")

    threadsCount = 150
    timeSpentOnManyThreads = functionTimer(
        manyThreads,
        {
            'dbName': '../data/databaseB.db',
            'valuesCount': requestsCount,
            'threadsCount': threadsCount
        }
    )

    print(f"Time spent on {threadsCount} threads {requestsCount} requests to database: {timeSpentOnManyThreads} sec")


if __name__ == "__main__":
    main()
