__author__ = 'dpshee'

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import boto
import json
import time

headers = {'content-type': 'application/json'}
url = 'https://stream.twitter.com/1.1/statuses/filter.json'

# Variables that contains the user credentials to access Twitter API
access_token = '3283934233-xx'
access_secret = 'xx'
consumer_key = 'xx'
consumer_secret = 'xx'
aws_access_key = 'xx'
aws_access_secret = 'xx/X/lNrYLMILRk0z1tEUfHg'


#Kinesis stream
stream_name = 'tweet_test_dps'

#List of keywords
list_of_keywords = ['MMM', 'AXP']

class TweetListener(StreamListener):
    def on_status(self, tweet):
        _tweet = tweet.__dict__['_json']
        print "Tweet: ", _tweet
        self.send_tweet_to_stream(_tweet)

    def send_tweet_to_stream(self, tweet):
        #this block assumes the stream exists and is in a state to accept data
        #conn = boto.connect_kinesis(aws_access_key, aws_access_secret)
        conn.put_record(stream_name, json.dumps(tweet), str(int(time.time())))


if __name__ == '__main__':
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    conn = boto.connect_kinesis(aws_access_key, aws_access_secret)
    l = TweetListener()
    streamer = Stream(auth, listener=l)
    streamer.filter(track = list_of_keywords)