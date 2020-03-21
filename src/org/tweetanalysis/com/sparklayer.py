from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
# create spark configuration
import sparkutilities as su

# ssc = su.setbase()
#
# dataStream = ssc.socketTextStream("localhost", 9009)
#
# tags_totals = su.getTagsTotal(dataStream)
#
# # do processing for each RDD generated in each interval
# tags_totals.foreachRDD(su.process_rdd)
# # start the streaming computation
# ssc.start()
# # wait for the streaming to finish
# ssc.awaitTermination()


conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost",9009)

# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))
# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
# adding the count of each hashtag to its last count
tags_totals = hashtags.updateStateByKey(su.aggregate_tags_count)
# do processing for each RDD generated in each interval
tags_totals.foreachRDD(su.process_rdd)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()

