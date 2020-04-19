import socket
import sys
import time

import requests
import requests_oauthlib

import tweetUtilites as TU


if __name__ == "__main__":
    api=TU.readinifile("key_pass")
    tcp=TU.readinifile("tcp_key")
    #print(api)
    conn = None
    ''' This is TCP connection setting to write message'''
    # s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # s.bind((tcp['tcp_ip'], int(tcp['tcp_port'])))
    # s.listen(1)
    # print("Waiting for TCP connection...")
    # conn, addr = s.accept()
    print("Connected ! Starting getting tweets.")
    resp = TU.get_tweets(api)
    #TU.send_tweets_to_spark(resp, conn)
    TU.send_tweet_spark_kafka(resp)