from __future__ import print_function

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
# create spark configuration

# global consolidatedData=pd.DataFrame()
from pyspark.streaming.kafka import KafkaUtils

def returnHashValue(messageWord):
    if ('#' in messageWord):
        return True
    return False



def string_stream(s, separators="\n"):
    start = 0
    for end in range(len(s)):
        if s[end] in separators:
            yield s[start:end]
            start = end + 1
    if start < end:
        yield s[start:end+1]


def getCountWords(listOfElems,dictOfElems):
    ''' Get frequency count of duplicate elements in the given list '''
    #dictOfElems = dict()
    # Iterate over each element in list
    for elem in listOfElems:
        # If element exists in dict then increment its value else add it in dict
        if elem in dictOfElems:
            dictOfElems[elem] += 1
        else:
            dictOfElems[elem] = 1

            # Filter key-value pairs in dictionary. Keep pairs whose value is greater than 1 i.e. only duplicate elements from list.
    dictOfElems = {key: value for key, value in dictOfElems.items() if value > 1}
    # Returns a dict of duplicate elements and thier frequency count
    return dictOfElems

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
        print("Acutal message----------1")
        sql_context = get_sql_context_instance(rdd.context)
        print("sql : ",sql_context)
        print("Acutal message----------2")
        print(rdd.take(1))
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        print("Acutal message----------3")
        # create a DF from the Row RDD
        print(row_rdd.take(3))
        hashtags_df = sql_context.createDataFrame(row_rdd.map (lambda x: Row(x)))
        print("Acutal message----------4")
        # Register the dataframe as table
        hashtags_df.registerTempTable("hashtags")
        print("Acutal message----------5")
        # get the top 10 hashtags from the table using SQL and print them
        hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
        print("Acutal message----------6")
        hashtag_counts_df.show()
        # call this method to prepare top 10 hashtags DF and send them
        send_df_to_dashboard(hashtag_counts_df)
        print("Acutal message----------ENDS")
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

def updateFunc(new_values, last_sum):
    count = 0
    counts = [field[0] for field in new_values]
    ids = [field[1] for field in new_values]
    if last_sum:
        count = last_sum[0]
        new_ids = last_sum[1] + ids
    else:
        new_ids = ids
    return new_ids,sum(counts) + count

conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext.getOrCreate(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 20)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009 or kafka topic
#dataStream = ssc.socketTextStream("localhost",9092) #reading from tcp

# topic for receiving message
tweetData = 'tweetData'



# dataStream = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
#                           bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)

dataStream = KafkaUtils.createStream(ssc,"localhost:2181","raw-event-streaming-consumer",{tweetData:10})

rows = dataStream.map(lambda x: x[1])
raw = rows.flatMap(lambda x: x.split("     "))
print("message1")
#lines = raw.map(lambda xs: xs[1].split("|"))
words = raw.map(lambda x: (x,1))

#words  = lines.flatMap(lambda line: line.split(" "))
#words = streamMessage.flatMap(lambda line: line.split(" "))
hashtags = words.filter(lambda w: ('#' in w) or ('@' in w)).map(lambda x: (x, 1))

tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
tags_totals.foreachRDD(process_rdd)

#lines.pprint()



ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()

