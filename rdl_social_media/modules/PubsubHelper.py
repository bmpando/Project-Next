from google.cloud import pubsub_v1
import logging
import json


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
