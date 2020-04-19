from kafka import KafkaConsumer
import json
from pyspark import SparkConf,SparkContext
from pyspark.shell import spark
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import preprocessor as prep
import pandas as pd
from collections import Counter

# consumer =  KafkaConsumer('tweetData', auto_offset_reset='earliest',
#                           bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
from pyspark.streaming.kafka import KafkaUtils

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        print("row_rdd value:", row_rdd.take(10))
        # create a DF from the Row RDD

        hashtags_df = sql_context.createDataFrame(row_rdd)

        # Register the dataframe as table
        hashtags_df.registerTempTable("hashtags")

        # get the top 10 hashtags from the table using SQL and print them
        hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")

        hashtag_counts_df.show()
        # call this method to prepare top 10 hashtags DF and send them
        send_df_to_dashboard(hashtag_counts_df)

    except Exception as ex:
        e = sys.exc_info()[0]
        print("Error: %s" % ex)

def send_df_to_dashboard(df):
    # extract the hashtags from dataframe and convert them into array
    top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
    # extract the counts from dataframe and convert them into array
    tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
    # initialize and send the data through REST API
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)

conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext.getOrCreate(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009 or kafka topic
#dataStream = ssc.socketTextStream("localhost",9092) #reading from tcp

tweetData = 'tweetData'

consumer = KafkaUtils.createStream(ssc,"localhost:2181","raw-event-streaming-consumer",{tweetData:1})


lines = consumer.map(lambda x: x[1])

#lines.pprint()

counts = lines.flatMap(lambda line: line.split(" ")) \
                  .filter(lambda w:'#' in w) \
                  .map(lambda word: (word, 1))

# hashtags = counts.filter(lambda w: ('@' in w) or ('cov' in w))
tags_totals = counts.updateStateByKey(aggregate_tags_count)

#tags_totals.pprint(num=10)

tags_totals.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination()