# YouTube Video: https://www.youtube.com/watch?v=wlnx-7cm4Gg
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from google.cloud import pubsub_v1
import logging
import json

import twitter_credentials


# BM added pubsubhelper
class PubsubHelper:

    def __init__(self, project_id, topic_name, pubsub_message):
        self.project_id = project_id
        self.topic_name = topic_name
        self.pubsub_message = pubsub_message

    def send_pubsub_message(self):
        logging.info('sending message..')

        try:
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(self.project_id, self.topic_name)
            self.pubsub_message = json.dumps(self.pubsub_message).encode('utf-8')
            logging.info(self.pubsub_message)
            future = publisher.publish(topic_path, data=self.pubsub_message)
            logging.info(future.result())
            logging.info('done sending message')
        except Exception as e:
            raise


# # # # TWITTER STREAMER # # # #
class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """

    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = StdOutListener(fetched_tweets_filename)
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        stream = Stream(auth, listener)

        # This line filter Twitter Streams to capture data by the keywords:
        stream.filter(track=hash_tag_list)


# # # # TWITTER STREAM LISTENER # # # #
class StdOutListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """

    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename
        self.project_id = 'rdl-socialmedia-prod'
        self.pubsub_topic = 'rdl-tp-twitter'

    def on_data(self, data):
        try:
            print(data)
            # Send to pubsub topic
            PubsubHelper(project_id=self.project_id, topic_name=self.pubsub_topic,
                         pubsub_message=data).send_pubsub_message()

            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # Authenticate using config.py and connect to Twitter Streaming API.
    hash_tag_list = ['@MTNza', 'TelkomZA', '@CellC', '@Vodacom']
    fetched_tweets_filename = "tweets.txt"

    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list)
