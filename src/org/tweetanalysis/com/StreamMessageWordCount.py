from __future__ import division
from pyspark import SparkContext, SQLContext, SparkConf, StorageLevel
from pyspark.streaming import StreamingContext
import numpy as np
from decimal import Decimal
import matplotlib.pyplot as plt
import sys

def countWords(newValue, lastSum):
    if lastSum is None:
        lastSum = 0
    return sum(newValue, lastSum)

if __name__ == "__main__":

    sc = SparkContext(appName="TweetWordCount")
    ssc = StreamingContext(sc,20)

    ssc.checkpoint("file:///Laxman/Project/Python/TweetStreamingAnalysis/src/org/tweetanalysis/data")

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    # countbywindow = lines.countByWindow(10,2)
    word_counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .updateStateByKey(countWords)

    word_counts.pprint()

    ssc.start()
    ssc.awaitTermination()