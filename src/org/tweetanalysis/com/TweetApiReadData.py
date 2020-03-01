import sys
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from configparser import SafeConfigParser, ConfigParser



class TweetsListener(object):

    def __init__(self) -> None:
        print("Tweet Listener started or initiated")

    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True

    def on_exception(self, status):
        print(status)
        return True

    def on_connect(self):
        print("Connected Successfully!")
        return True

def readiniFile(keyPass,keyReq):
    configKey = ConfigParser()
    configKey.read('/Laxman/Project/Python/TweetStreamingAnalysis/src/resource/configfile.ini')
    for name, value in configKey.items(keyPass):
        if keyReq == name:
            return value
    return None

if __name__ == "__main__":

    api_key = readiniFile('key_pass','api_key')
    api_secret_key = readiniFile('key_pass','api_secrect_key')

    access_token = readiniFile('key_pass','access_token')
    access_token_secret = readiniFile('key_pass','access_token_secret')

    auth = OAuthHandler(api_key, api_secret_key)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = Stream(auth, TweetsListener())
    twitter_stream.filter(track=['#'])
