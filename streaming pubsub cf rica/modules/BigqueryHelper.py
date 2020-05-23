from google.cloud import bigquery
import logging
import json
from modules.ErrorHelper import ErrorHelper
from modules.PubsubHelper import PubsubHelper


class BigqueryHelper:
    def __init__(self, project_id, pubsub_topic, dataset, table,
                 staging_errors, document, event_id, retry_status):
        self.project_id = project_id
        self.pubsub_topic = pubsub_topic
        self.dataset = dataset
        self.table = table
        self.staging_errors = staging_errors
        self.document = document
        self.event_id = event_id
        self.retry_status = retry_status

    def stream_insert(self):
        try:
            bigquery_client = bigquery.Client(self.project_id)
            dataset_ref = bigquery_client.dataset(self.dataset)
            table_ref = dataset_ref.table(self.table)
            table = bigquery_client.get_table(table_ref)
            errors = bigquery_client.insert_rows(table, [self.document])
            if len(errors) > 0 and self.retry_status == 'PENDING':
                '''Error occured and there has been no previous retry '''
                logging.info(f'Finished insert json rows into bigquery with errors '
                             f'will now log the error and retry: {errors}')
                self.retry_status = 'PENDING'
                ErrorHelper(dataset_name=self.dataset, pubsub_topic=self.pubsub_topic,
                            table_name=self.staging_errors, message_status='PENDING RETRY',
                            error_message=str(errors),
                            payloadString=str(json.dumps(self.document)),
                            retry_status=self.retry_status,
                            event_id=self.event_id).log_streaming_error()
                PubsubHelper(project_id=self.project_id, topic_name=self.pubsub_topic,
                             pubsub_message=self.document).send_pubsub_message()
            elif len(errors) > 0 and self.retry_status == 'RETRY':
                logging.info('Error occured and there has been a previous retry manual intervention required')
                self.retry_status = 'FAILED'
                ErrorHelper(dataset_name=self.dataset, pubsub_topic=self.pubsub_topic,
                            table_name=self.staging_errors, message_status='FAILED RETRY MANUAL INTERVENTION REQUIRED',
                            error_message=str(errors),
                            payloadString=str(json.dumps(self.document)),
                            retry_status=self.retry_status,
                            event_id=self.event_id).log_streaming_error()
            elif len(errors) > 0:
                logging.info(f'Finished insert json rows into bigquery with errors '
                             f'retry mechanism failed: {errors}')
        except Exception as e:
            logging.error(e)
            raise
        finally:
            logging.info('finally inserted data')
