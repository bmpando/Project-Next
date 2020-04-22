import os
from google.cloud import pubsub_v1
import logging
import json
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
    topic_name = os.getenv('PUBSUB_TOPIC') # TODO make env

    # Configure the retry settings. Defaults will be overwritten.
    retry_settings = {
        'interfaces': {
            'google.pubsub.v1.Publisher': {
                'retry_codes': {
                    'publish': [
                        'ABORTED',
                        'CANCELLED',
                        'DEADLINE_EXCEEDED',
                        'INTERNAL',
                        'RESOURCE_EXHAUSTED',
                        'UNAVAILABLE',
                        'UNKNOWN',
                    ]
                },
                'retry_params': {
                    'messaging': {
                        'initial_retry_delay_millis': 100,  # default: 100
                        'retry_delay_multiplier': 1.3,  # default: 1.3
                        'max_retry_delay_millis': 60000,  # default: 60000
                        'initial_rpc_timeout_millis': 5000,  # default: 25000
                        'rpc_timeout_multiplier': 1.0,  # default: 1.0
                        'max_rpc_timeout_millis': 600000,  # default: 30000
                        'total_timeout_millis': 600000,  # default: 600000
                    }
                },
                'methods': {
                    'Publish': {
                        'retry_codes_name': 'publish',
                        'retry_params_name': 'messaging',
                    }
                },
            }
        }
    }

    try:
        my_message = {"my_event_id": my_event_id, "project_id": project_id, "bucket_name": bucket_name,
                      "file_name": file_name, "my_crc32": my_crc32, "file_size": file_size}

        publisher = pubsub_v1.PublisherClient(client_config=retry_settings)
        topic_path = publisher.topic_path(project_id, topic_name)
        my_message = json.dumps(my_message).encode('utf-8')
        logging.info(my_message)
        future = publisher.publish(topic_path, data=my_message)
        logging.info(future.result())
    except Exception as e:
        chat_url = os.getenv('CHATROOM_URL')
        NotificationsHelper(chat_url, my_event_id, project_id, file_name, bucket_name,
                            error_folder=topic_name, message=str(e)).notify_chatroom()


