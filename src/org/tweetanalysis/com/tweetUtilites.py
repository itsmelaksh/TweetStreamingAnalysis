import json
import sys
from configparser import SafeConfigParser, ConfigParser

import requests
import requests_oauthlib
import pandas as pd
from pandas import DataFrame
import os




def setauth(myapi):
    return requests_oauthlib.OAuth1(myapi['api_key'], myapi['api_secret_key'], myapi['access_token'], myapi['access_token_secret'])

def get_tweets(myapi):
    my_auth = setauth(myapi)
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    #print(query_url, response)
    return response

def writeData(textData):
    temp = pd.DataFrame([textData], columns=['created_at','id','text','source','favorite_count','coordinates','place'])
    return temp


def send_tweets_to_spark(http_resp, tcp_connection):
    # with open('/Laxman/Project/Python/TweetStreamingAnalysis/src/org/tweetanalysis/data/csvfile.csv', 'wb') as file:
    df = pd.DataFrame()
    # cntMessge = 0
    # fileSeq = 1
    filepath = r'/Laxman/Project/Python/TweetStreamingAnalysis/src/org/tweetanalysis/data/csvfile.csv'
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            df = df.append(writeData(full_tweet))
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            # file.write(tweet_text.encode('utf-8') + "\n".encode("utf-8"))
            # if cntMessge > 20:
            #     print("Writing to file:")
            # filepath = '/Laxman/Project/Python/TweetStreamingAnalysis/src/org/tweetanalysis/data/csvfile.csv'
            df.to_csv(filepath,index=False,mode='a',header=not(os.path.isfile(filepath)))
            #     df=pd.DataFrame()
            #     cntMessge = 0
            #     fileSeq = fileSeq + 1
            # else:
            #     print(cntMessge + "Number of messages")
            #     cntMessge = cntMessge + 1
            tcp_connection.send(tweet_text.encode('utf-8') + "\n".encode("utf-8"))
            #df.to_csv(filepath, index=False, mode='a')
            df = pd.DataFrame()
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

def readinifile(keypass):
    keydetail= {}
    configKey = ConfigParser()
    configKey.read('/Laxman/Project/Python/TweetStreamingAnalysis/src/resource/configfile.ini')
    for name, value in configKey.items(keypass):
        keydetail[name] = value
    return keydetail
