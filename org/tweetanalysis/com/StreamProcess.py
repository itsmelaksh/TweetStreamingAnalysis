from __future__ import division
from pyspark import SparkContext, SQLContext, SparkConf, StorageLevel
from pyspark.streaming import StreamingContext
import numpy as np
from decimal import Decimal
import matplotlib.pyplot as plt
import sys

print("Hello World")

if __name__ == "__main__":

    sc = SparkContext(appName="StreamingwordCount")
    ssc = StreamingContext(sc,10)

    ssc.checkpoint("file:///Laxman/Project/Python/TweetStreamingAnalysis/org/tweetanalysis/data")

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    counts = lines.flatMap(lambda line: line.split(","))\
                  .filter(lambda word: "ERROR" in word)  \
                  .map(lambda word: (word,1))\
                  .reduceByKey(lambda a,b:  a+b)

    counts.pprint()

    ssc.start()
    ssc.awaitTermination()

