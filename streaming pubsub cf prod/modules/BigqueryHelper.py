from google.cloud import bigquery
import logging
import json
import io
from modules.ErrorHelper import ErrorHelper
from modules.PubsubHelper import PubsubHelper


def get_table_schema(dataset, table):
    """Get BigQuery Table Schema."""
    logging.info('getting table schema')
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset)
    bg_tableref = bigquery.table.TableReference(dataset_ref, table)
    bg_table = bigquery_client.get_table(bg_tableref)
    return bg_table.schema


def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema


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
            # errors = bigquery_client.insert_rows_json(table, [self.document], ignore_unknown_values)
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
                # PubsubHelper(project_id=self.project_id, topic_name=self.pubsub_topic,
                #      pubsub_message=self.document).send_pubsub_message()
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

    def load_table_from_file(self):
        try:
            logging.info('loading as file')
            logging.info(self.document)
            json_data = self.document

            # stringio_data = io.StringIO(json_data)

            bigquery_client = bigquery.Client(self.project_id)
            dataset_ref = bigquery_client.dataset(self.dataset)
            table_ref = dataset_ref.table(self.table)
            table = bigquery_client.get_table(table_ref)

            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.ignore_unknown_values = True
            job_config.max_bad_records = 100
            job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

            job = bigquery_client.load_table_from_file(json_data, table, job_config=job_config)
            my_result = job.result()
            logging.info(my_result)

        except Exception as e:
            logging.info('WTF')
            logging.error(e)
            raise
        finally:
            logging.info('finally inserted data')

    def insert_rows_json(self):
        try:
            bigquery_client = bigquery.Client(self.project_id)
            dataset_ref = bigquery_client.dataset(self.dataset)
            table_ref = dataset_ref.table(self.table)
            table = bigquery_client.get_table(table_ref)
            # errors = bigquery_client.insert_rows_json(table, [self.document], ignore_unknown_values)
            errors = bigquery_client.insert_rows_json(table, [self.document], ignore_unknown_values=True)

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
                # PubsubHelper(project_id=self.project_id, topic_name=self.pubsub_topic,
                #      pubsub_message=self.document).send_pubsub_message()
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
            logging.info('finally inserted data')

    def load_table_from_json(self):
        try:
            logging.info('loading as file')
            logging.info(self.document)
            json_data = str(self.document)
            stringio_data = io.StringIO(json_data)

            bigquery_client = bigquery.Client(self.project_id)
            dataset_ref = bigquery_client.dataset(self.dataset)
            table_ref = dataset_ref.table(self.table)
            table = bigquery_client.get_table(table_ref)

            # schema = get_table_schema(self.dataset, self.table)
            # formatted_schema = format_schema(schema)

            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.ignore_unknown_values = True
            job_config.write_disposition = 'WRITE_APPEND'
            # job_config.schema = formatted_schema
            job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

            job = bigquery_client.load_table_from_json([self.document], table, num_retries=10, job_config=job_config)
            my_result = job.result()
            logging.info(my_result)

        except Exception as e:
            ErrorHelper(dataset_name=self.dataset, pubsub_topic=self.pubsub_topic,
                        table_name=self.staging_errors, message_status='FAILED RETRY MANUAL INTERVENTION REQUIRED',
                        error_message=str(e),
                        payloadString=str(json.dumps(self.document)),
                        retry_status=self.retry_status,
                        event_id=self.event_id).log_streaming_error()
            logging.error(e)
            raise
        finally:
            logging.info('finally inserted data')
