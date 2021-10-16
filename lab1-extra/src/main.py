import time
from typing import Mapping, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import pyspark.sql.functions as f

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, size

programmingLanguagesList = ["JavaScript", "Java", "PHP", "Python",
                            "C#", "C++", "Ruby", "CSS", "Objective-C",
                            "Perl", "Scala", "Haskell", "MATLAB", "Clojure",
                            "Groovy"]



outSchema = StructType([StructField('Title', StringType(), False), StructField('Text', StringType(), False)])
titleDelim = "</title><text>"

def getTitleFunc(rawText):
    i = rawText.find(titleDelim)
    title = rawText[14:i]
    return title

def getTextFunc(rawText):
    i = rawText.find(titleDelim)
    text = rawText[i + len(titleDelim): -16]
    return text

def getOccurancesCountFunc(rawText, language):
    res = (rawText.upper()).count(language.upper())
    return res


""" Converting function to UDF """
getTitle = f.udf(lambda rawText: getTitleFunc(rawText), StringType())
getText = f.udf(lambda rawText: getTextFunc(rawText), StringType())
getOccurancesCount = f.udf(lambda title, language: getOccurancesCountFunc(title, language), IntegerType())

def functionTimer(function, kwargs: Mapping[str, Any]):
    startTime = time.perf_counter()
    function(kwargs)
    stopTime = time.perf_counter()
    return stopTime - startTime


def wikiDataPersist(kwargs: Mapping[str, Any]):
    spark = SparkSession.builder.master('local[*]') \
        .config("spark.driver.memory", "15g") \
        .appName("Lab1-extra").getOrCreate()
    wikiData = spark.read.text("../data/wikipedia.dat").persist()



    programmingLanguagesDict = dict()
    for programmingLanguage in programmingLanguagesList:
        programmingLanguagesDict[programmingLanguage] = dict()


        filteredWiki = wikiData.filter(wikiData.value.rlike(programmingLanguage))
        wikiLanguage = filteredWiki.withColumn("Title", getTitle(f.col("value"))).\
            withColumn("Count", getOccurancesCount(f.col("value"), lit(programmingLanguage))).drop(f.col("value"))

        programmingLanguagesDict[programmingLanguage]['count'] = wikiLanguage.agg({'Count': 'sum'}).collect()[0][0]
        programmingLanguagesDict[programmingLanguage]['articles'] = wikiLanguage.select("Title").rdd.flatMap(
           lambda x: x).collect()
        print(
            f"{programmingLanguage} repeats {programmingLanguagesDict[programmingLanguage]['count']} times. In {len(programmingLanguagesDict[programmingLanguage]['articles'])} articles.")
        filteredWiki.unpersist()
        wikiLanguage.unpersist()

    spark.stop()


def main():
    timeSpent = functionTimer(wikiDataPersist, None)
    print(f"Time spent on wikiDataPersist: {timeSpent} sec")

if __name__ == "__main__":
    main()