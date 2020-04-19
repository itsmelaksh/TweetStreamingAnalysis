import socket
import sys
import time

import requests
import requests_oauthlib
import json
import pandas as pd

import tweetUtilites as TU
from kafka import KafkaProducer
from tweepy import StreamListener

df = pd.DataFrame()


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print("Error: %s" % ex)
        print('Exception in publishing message')
        print(str(ex))



def sendTweets(kafkaMessage, Kafka):
    None
if __name__ == "__main__":
    api=TU.readinifile("key_pass")
    tcp=TU.readinifile("tcp_key")
    #print(api)
    conn = None
    # s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # s.bind((tcp['tcp_ip'], int(tcp['tcp_port'])))
    # s.listen(1)
    # print("Waiting for TCP connection...")
    # conn, addr = s.accept()
    print("Connected ! Starting getting tweets.")
    resp = TU.get_tweets(api)
    #TU.send_tweets_to_spark(resp, conn)

    kafka_producer = connect_kafka_producer()
    print("connected Successfully")
    # for line in resp.iter_lines():
    #     full_tweet = json.loads(line)
        #print(full_tweet)
        # print("key starts")
        # for key in full_tweet:
        #     # if ('#' in full_tweet[key]):
        #     test=full_tweet[key]
        #     if (key == 'extended_tweet'):
        #         for key1 in test:
        #             print("testKey:", key1)
        #     print("Key:", key)
        #     #print("value:", full_tweet[key]['full_text'])
        # print("key ends")
    # exit(0)
    for line in resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            # if 'extended_tweet' in full_tweet.keys():
            #     tweet_text = full_tweet['full_text']
            #     print(type(tweet_text))
            # else:
            tweet_text = full_tweet['text']
            #kafka_producer.send('tweetData', value = tweet_text)
            publish_message(kafka_producer, 'tweetData', 'raw', tweet_text )
            print("Tweet Text: " + tweet_text )
            print("------------------------------------------")
            # if kafka_producer is not None:
            #     kafka_producer.close()
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

    time.sleep(1)
    kafka_producer.close()

