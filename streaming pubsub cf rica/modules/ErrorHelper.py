import logging
from google.cloud import bigquery
from datetime import datetime


class ErrorHelper:
    def __init__(self, pubsub_topic, dataset_name, table_name, message_status, error_message, payloadString,
                 retry_status, event_id):
        self.dataset_name = dataset_name
        self.pubsub_topic = pubsub_topic
        self.table_name = table_name
        self.message_status = message_status
        self.error_message = error_message
        self.payloadString = payloadString
        self.event_id = event_id
        self.retry_status = retry_status

    def log_streaming_error(self):
        logging.info('started logging the error')
        _m_timestamp = datetime.timestamp(datetime.now())
        client = bigquery.Client()
        dataset_id = self.dataset_name  # replace with your dataset ID
        table_id = self.table_name
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)  # API request
        row_to_insert = [
            (self.pubsub_topic, self.event_id, _m_timestamp, self.message_status, self.payloadString,
             self.error_message, self.retry_status)
        ]
        errors = client.insert_rows(table, row_to_insert)  # API request
        if len(errors) > 0:
            logging.info('An error occured while trying to log the error..')
            logging.info(errors)
            raise Exception(errors)
