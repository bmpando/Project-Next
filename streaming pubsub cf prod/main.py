import base64
import logging
import json
import os
import datetime
from datetime import datetime
from modules.BigqueryHelper import BigqueryHelper


def main_streamer(event, context):
    logging.info('inside main streamer')
    try:
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
        logging.info('started main streamer')
        staging_table = os.getenv('STAGING_TABLE')
        dataset = os.getenv('STAGING_DATASET')
        pubsub_topic = os.getenv('PUBSUB_TOPIC')
        staging_errors = os.getenv('STAGING_ERRORS')
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        logging.info(pubsub_message)
        pubsub_message = json.loads(pubsub_message)
        if "_m_retry_status" in pubsub_message:
            _m_retry_status = 'RETRY'
        else:
            _m_retry_status = 'PENDING'

        current_time = datetime.now().strftime("%d-%b-%Y %H:%M:%S.%f")
        my_event_id = str(context.event_id)
        attachment = {"_m_event_id": my_event_id,
                      "_m_timestamp": str(current_time),
                      "_m_retry_status": _m_retry_status}
        pubsub_message.update(attachment)
        BigqueryHelper(project_id=project_id, pubsub_topic=pubsub_topic, dataset=dataset, table=staging_table,
                       staging_errors=staging_errors, document=pubsub_message, event_id=my_event_id,
                       retry_status=_m_retry_status).stream_insert()

    except Exception as e:
        logging.error(e)
        raise
    finally:
        logging.info('finally main streamer')
