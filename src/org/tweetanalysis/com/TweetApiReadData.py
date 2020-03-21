import sys
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from configparser import SafeConfigParser, ConfigParser
from tweetUtilites import readinifile


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



if __name__ == "__main__":

    myapi = readinifile('key_pass')
    auth = OAuthHandler(myapi['api_key'], myapi['api_secret_key'])
    auth.set_access_token(myapi['access_token'], myapi['access_token_secret'])

    twitter_stream = Stream(auth, TweetsListener())
    twitter_stream.filter(track=['#'])
