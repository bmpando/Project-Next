import logging
from google.cloud import storage
import traceback


def move_file(data, context):
    from_bucket = 'grpdata-prod-dataandanalytics-test-source'
    to_bucket = 'grpdata-prod-dataandanalytics-test-source'
    file_name = data["name"]
    new_file_name = data["name"]

    try:
        logging.info('file name ' + file_name)
        #new_blob_name = new_file_name.replace("/", '')
        new_blob_name = new_file_name
        logging.info('new blob ' + new_blob_name)
        storage_client = storage.Client()
        source_bucket = storage_client.get_bucket(from_bucket)
        source_blob = source_bucket.blob(file_name)
        destination_bucket = storage_client.get_bucket(to_bucket)
        # copy file
        new_blob = source_bucket.copy_blob(source_blob, destination_bucket, new_blob_name)
        # delete file
        source_blob.delete()
        logging.debug('File {} Successfully Moved '.format(source_blob))
    except Exception as e:
        logging.error('Error copying file \'%s\'. Cause: %s' % (source_blob, traceback.format_exc()))
        raise e
    finally:
        logging.info('finally tried to move files')
