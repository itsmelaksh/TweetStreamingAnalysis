from __future__ import division
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
import numpy as np
from decimal import Decimal
import matplotlib.pyplot as plt
import sys

print("Hello World")

if __name__ == "__main__":

    sc = SparkContext(appName="StreamingwordCount")
    ssc = StreamingContext(sc,10)
    sparkses = SparkSession(appName="test")
    ssc.checkpoint("file:///Laxman/Project/Python/TweetStreamingAnalysis/src/org/tweetanalysis/data")

    #lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    lines = ssc.socketTextStream("localhost", 9009)

    counts = lines.flatMap(lambda line: line.split(","))\
                  .filter(lambda word: "ERROR" in word)  \
                  .map(lambda word: (word,1))\
                  .reduceByKey(lambda a,b:  a+b)

    counts.pprint()

    ssc.start()
    ssc.awaitTermination()

