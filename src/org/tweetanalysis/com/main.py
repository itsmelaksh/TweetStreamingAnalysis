import socket
import sys
import requests
import requests_oauthlib
import json
import pandas as pd

import tweetUtilites as TU

df = pd.DataFrame()

if __name__ == "__main__":
    api=TU.readinifile("key_pass")
    tcp=TU.readinifile("tcp_key")
    #print(api)
    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((tcp['tcp_ip'], int(tcp['tcp_port'])))
    s.listen(1)
    print("Waiting for TCP connection...")
    conn, addr = s.accept()
    print("Connected ! Starting getting tweets.")
    resp = TU.get_tweets(api)
    TU.send_tweets_to_spark(resp, conn)
