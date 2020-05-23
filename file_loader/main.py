import os
from google.cloud import bigquery
from google.cloud import datastore
import logging
import time
import traceback
import base64
import json
from datetime import datetime
from google.cloud.bigquery import retry as bq_retry
from modules.DatastoreHelper import DatastoreHelper
from modules.GCSHelper import GCSHelper
from modules.NotificationsHelper import NotificationsHelper

"""main function called from the cloud function"""


def main_loader(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    logging.info(data)
    """get required environment variables"""
    logging.debug('initializing environment variables..')
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    bucket_name = data['bucket']
    archive_bucket = os.getenv('ARCHIVE_BUCKET')
    error_bucket = os.getenv('ERROR_BUCKET')
    config_ds_project = os.getenv('CONFIG_DATASTORE_PROJECT')
    config_kind = os.getenv('CONFIG_KIND')
    logging_kind = os.getenv('LOGGING_KIND')
    file_meta_table = os.getenv('FILE_METADATA_TABLE')
    file_register = os.getenv('FILE_LOG_TABLE')
    file_name = data['name']
    file_regex = get_file_regex(file_name)
    my_event_id = context.event_id
    my_crc32 = data['crc32c']
    table_name = format_table_name(file_name, my_event_id)
    m_time_stamp = datetime.timestamp(datetime.now())
    # Get file data
    # start new
    my_results = DatastoreHelper(project=config_ds_project,
                                 my_kind=config_kind,
                                 event_id=str(my_event_id)
                                 ).get_entity(my_property='file_name_regex',
                                              my_operator='=',
                                              my_value=file_regex)
    assert my_results != [], "Returned nothing"
    # logging.info(my_results)
    my_results = list(my_results)

    for row in my_results:

        try:
            logging.debug('start loading file')
            my_sub_folder = row["archive_folder"]
            dataset_id = row["data_set_id"]
            m_snapshot_date = get_snapshot_date(file_name, row["snap_date_start"], row["snap_date_end"],
                                                row["snap_date_format"])
            if int(data['size']) == 0:
                logging.info('Empty File ' + file_name)
                new_name = file_name + str(my_event_id)
                insert_register(dataset_id, file_register, file_name, my_crc32, 'EMPTY', m_time_stamp, 0,
                                my_event_id, 'File is Empty')
                insert_datastore_log(event_id=str(my_event_id),
                                     file_name=file_name,
                                     file_crc32c=my_crc32,
                                     load_status='DUPLICATE',
                                     load_date=m_time_stamp,
                                     config_ds_project=config_ds_project,
                                     logging_kind=logging_kind)

                GCSHelper(file_name, bucket_name, archive_bucket, new_name, my_sub_folder).move_file()
                return

            if check_duplication_datastore(logging_kind, config_ds_project, file_name, my_crc32):
                insert_register(dataset_id, file_register, file_name, my_crc32, 'DUPLICATE', m_time_stamp, 0,
                                my_event_id, 'File already loaded')
                insert_datastore_log(event_id=str(my_event_id),
                                     file_name=file_name,
                                     file_crc32c=my_crc32,
                                     load_status='DUPLICATE',
                                     load_date=m_time_stamp,
                                     config_ds_project=config_ds_project,
                                     logging_kind=logging_kind)
                logging.info(file_name + ' already loaded will archive file and exit')
                new_name = file_name + str(my_event_id)
                GCSHelper(file_name, bucket_name, archive_bucket, new_name, my_sub_folder).move_file()

            else:
                create_temp_table(row["temp_dataset_id"], dataset_id, table_name, row["template_table"],
                                  row["autodetect"])
                my_rows = load_csv_file(bucket_name, file_name, table_name, row["template_table"], dataset_id,
                                        row["temp_dataset_id"], row["delimiter"], row["create_disposition"],
                                        row["write_disposition"], row["leading_rows"], row["allow_jagged_rows"],
                                        row["autodetect"], max_bad_records=row["max_errors_allowed"],
                                        allow_quoted_newlines=row["allow_quoted_newlines"])

                if row["multiple_tables"]:
                    sleep_index = (int(my_event_id) % 10)
                    time.sleep(sleep_index)
                    tab_index = (int(my_event_id) % 10)
                    tab_index = str(tab_index)
                    st_table_name = row["stg_table_name"] + tab_index
                    logging.debug('staging table ' + st_table_name)
                else:
                    st_table_name = row["stg_table_name"]
                    logging.debug('staging table ' + st_table_name)

                load_staging_table(project_id, file_name, st_table_name, table_name, dataset_id, row["temp_dataset_id"],
                                   m_snapshot_date, my_event_id, row["file_location"], file_meta_table)
                # populate the file register table
                insert_register(dataset_id, file_register, file_name, my_crc32, 'SUCCESS', m_time_stamp, my_rows,
                                my_event_id, 'Successfully loaded')
                insert_datastore_log(event_id=str(my_event_id),
                                     file_name=file_name,
                                     file_crc32c=my_crc32,
                                     load_status='SUCCESS',
                                     load_date=m_time_stamp,
                                     config_ds_project=config_ds_project,
                                     logging_kind=logging_kind)
                new_name = file_name + str(my_event_id)
                GCSHelper(file_name, bucket_name, archive_bucket, new_name, my_sub_folder).move_file()
                # copy_file(bucket_name, file_name, archive_bucket, file_name + str(my_event_id), my_sub_folder)
                # delete_file(bucket_name, file_name)

        except Exception as e:
            message = 'Error loading file \'%s\'. Cause: %s' % (file_name, traceback.format_exc())
            insert_register(dataset_id, file_register, file_name, my_crc32, 'FAILED', m_time_stamp, 0,
                            str(my_event_id), message)
            insert_datastore_log(event_id=str(my_event_id),
                                 file_name=file_name,
                                 file_crc32c=my_crc32,
                                 load_status='FAILED',
                                 load_date=m_time_stamp,
                                 config_ds_project=config_ds_project,
                                 logging_kind=logging_kind)
            handle_error(bucket_name, file_name, error_bucket, str(my_event_id), my_sub_folder, project_id,
                         message=str(e))
            raise e
        finally:
            logging.info('Exiting load process')


def load_staging_table(project_id, file_name, st_table_name, table_name, dataset_id, temp_dataset_id,
                       m_snapshot_date, event_id, location, file_meta_table):
    event_id = """'""" + str(event_id) + """'"""
    m_snapshot_date = str(m_snapshot_date)
    client2 = bigquery.Client()
    insert_job_config = bigquery.QueryJobConfig()
    insert_ref = client2.dataset(dataset_id).table(st_table_name)
    insert_job_config.destination = insert_ref
    insert_job_config.write_disposition = 'WRITE_APPEND'
    insert_sql = "select *  from `" + project_id + "." + temp_dataset_id + "." + table_name + "` ," \
                 "(select CURRENT_TIMESTAMP()  _m_timestamp ," + """cast ('""" + m_snapshot_date + """'as date)""" \
                 "_m_snapshot_date, current_date() _m_process_date " + """, '""" \
                 """""" + file_name + """'"""" _m_file_name , " + event_id + " _m_run_id FROM `" \
                 + project_id + "." + dataset_id + "." + file_meta_table + "`)"""
    logging.info('Insert SQL ' + insert_sql)
    try:
        query_job = client2.query(
            insert_sql,
            location=location,
            job_config=insert_job_config,
            retry=bq_retry.DEFAULT_RETRY.with_deadline(60)
        )
        query_job.result()  # Waits for the query to finish
        logging.debug('finished staging')
    except Exception as e:
        logging.error(e)
        # return retry_job(query, query_params, attempt_nb)


def load_csv_file(bucket_name, file_name, table_name, template_name, dataset_id, temp_dataset_id,
                  delimiter, create_disposition, write_disposition, leading_rows, jagged_rows, auto_detect,
                  max_bad_records, allow_quoted_newlines):
    logging.debug('start loading csv file')
    client = bigquery.Client()
    dataset_ref = client.dataset(temp_dataset_id)
    job_config = bigquery.LoadJobConfig()
    if not auto_detect:
        job_config.schema = get_table_schema(dataset_id, template_name)
    job_config.skip_leading_rows = leading_rows
    job_config.allow_jagged_rows = jagged_rows
    job_config.field_delimiter = delimiter
    job_config.autodetect = auto_detect
    job_config.max_bad_records = max_bad_records
    job_config.allow_quoted_newlines = allow_quoted_newlines
    job_config.create_disposition = create_disposition
    job_config.write_disposition = write_disposition
    job_config.source_format = bigquery.SourceFormat.CSV

    uri = 'gs://' + bucket_name + '/' + file_name

    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table(table_name),
        job_config=job_config)

    load_job.result()  # wait for table load to complete.

    destination_table = client.get_table(dataset_ref.table(table_name))
    return destination_table.num_rows


def create_temp_table(temp_dataset_id, dataset_id, _name, template_name, auto_detect):
    logging.debug('start creating temp table')
    client = bigquery.Client()
    if not auto_detect:
        my_schema = get_table_schema(dataset_id, template_name)
    dataset_id = temp_dataset_id
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(_name)
    if auto_detect:
        table = bigquery.Table(table_ref)
    else:
        table = bigquery.Table(table_ref, schema=my_schema)
    table = client.create_table(table)  # API request
    logging.debug('temp table created')


def get_file_regex(file_name):
    logging.debug('file_name ' + file_name)
    res = ''.join([i for i in file_name if not i.isdigit()])
    return res


def format_table_name(_name, event_id):
    new_name = str.replace(_name, "-", '_')
    new_name = str.replace(new_name, '.', '_')
    return new_name + '_' + str(event_id)


def get_snapshot_date(file_name, snap_date_start, snap_date_end, snap_date_format):
    try:
        my_snapshot = datetime.strptime(file_name[snap_date_start:snap_date_end], snap_date_format).date()
        return my_snapshot
    except Exception as e:
        logging.info('Invalid snapshot date : returned current timestamp')
        return datetime.date(datetime.now())
    finally:
        logging.debug('Snapshot date returned')


def check_duplication(project_id, dataset_id, file_register, file_name, my_crc32):
    logging.debug('start checking duplicates')
    bigquery_client = bigquery.Client()

    check_sql = "select *  from `" + project_id + "." + dataset_id + "." + file_register + "` " \
                "where file_name='" + file_name + "' and file_crc32c ='" + my_crc32 + "" \
                """' and load_status='SUCCESS'"""

    try:
        query_job = bigquery_client.query(check_sql)
        is_exist = len(list(query_job.result())) >= 1
        return is_exist
    except Exception as e:
        logging.error(e)
        return False
    finally:
        logging.info('finally checked for duplicates')


def check_duplication_datastore(logging_kind, config_ds_project, file_name, my_crc32):
    logging.debug('start checking duplicates datastore')

    try:
        client = datastore.Client(project=config_ds_project)
        query = client.query(kind=logging_kind)
        query.add_filter('file_name', '=', file_name)
        query.add_filter('file_crc32c', '=', my_crc32)
        query.add_filter('load_status', '=', 'SUCCESS')
        is_exist = len(list(query.fetch())) >= 1
        return is_exist
    except Exception as e:
        logging.error(e)
        return False
    finally:
        logging.info('finally checked for duplicates datastore')


def insert_register(dataset_name, table_name, file_name, file_crc32c, load_status,
                    load_date, loaded_rows, event_id, error_message):
    client = bigquery.Client()
    end_time_stamp = datetime.timestamp(datetime.now())
    dataset_id = dataset_name  # replace with your dataset ID
    table_id = table_name
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)  # API request
    row_to_insert = [
        (event_id, file_name, file_crc32c, load_date, end_time_stamp, load_status, loaded_rows,
         error_message)
    ]
    errors = client.insert_rows(table, row_to_insert)  # API request
    assert errors == []


def insert_datastore_log(event_id, file_name, file_crc32c, load_status, load_date, config_ds_project, logging_kind):
    dataobj = {'event_id': event_id,
               'file_crc32c': file_crc32c,
               'file_name': file_name,
               'load_start_date': load_date,
               'load_status': load_status}
    json_data = json.dumps(dataobj)
    logging.info(json_data)
    logging.info('logging_kind' + logging_kind)
    DatastoreHelper(project=config_ds_project,
                    my_kind=logging_kind,
                    event_id=event_id
                    ).insert_entity(data_object=dataobj)


def get_table_schema(dataset_id, table_id):
    """Get BigQuery Table Schema."""
    logging.info('getting table schema')
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    bg_tableref = bigquery.table.TableReference(dataset_ref, table_id)
    bg_table = bigquery_client.get_table(bg_tableref)
    return bg_table.schema


def handle_error(bucket_name, file_name, error_bucket, event_id, error_folder, project_id, message):
    new_name = file_name  # + str(event_id)
    chat_url = os.getenv('CHATROOM_URL')
    # GCSHelper(file_name, bucket_name, error_bucket, new_name, error_folder).move_file()
    if len(chat_url) > 1:
        NotificationsHelper(chat_url, event_id, project_id, file_name, error_bucket,
                            error_folder=error_folder, message=message).notify_chatroom()
    else:
        logging.info('invoked retry for ' + file_name)


def retry_job(query, query_params, attempt_nb):
    if attempt_nb < 3:
        print(' ! New BigQuery error. Retrying in 10s')
        time.sleep(10)
        # return run_job(query, query_params, attempt_nb + 1)
    else:
        raise Exception('BigQuery error. Failed 3 times', query)
