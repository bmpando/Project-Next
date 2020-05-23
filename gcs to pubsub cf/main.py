import os
from google.cloud import pubsub_v1
import logging
import json
import time
from modules.NotificationsHelper import NotificationsHelper


def queue_file(data, context):
    """get required environment variables"""
    logging.debug('initializing environment variables..')
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    # project_id = 'grpdata-prod-dataandanalytics'
    bucket_name = data['bucket']
    file_name = data['name']
    my_crc32 = data['crc32c']
    file_size = data['size']
    my_event_id = context.event_id
    topic_name = os.getenv('PUBSUB_TOPIC')

    try:
        # sleep_index = (int(my_event_id) % 100)
        # time.sleep(sleep_index)
        my_message = {"my_event_id": my_event_id, "project_id": project_id, "bucket": bucket_name,
                      "name": file_name, "crc32c": my_crc32, "size": file_size}

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
        my_message = json.dumps(my_message).encode('utf-8')
        logging.info(my_message)
        future = publisher.publish(topic_path, data=my_message)
        logging.info(future.result())
    except Exception as e:
        chat_url = os.getenv('CHATROOM_URL')
        NotificationsHelper(chat_url, my_event_id, project_id, file_name, bucket_name,
                            error_folder=topic_name, message=str(e)).notify_chatroom()
