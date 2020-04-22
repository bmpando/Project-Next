import logging
from google.cloud import storage
import traceback


class GCSHelper:

    def __init__(self, file_name, from_bucket, to_bucket, new_file_name, sub_folder):
        self.file_name = file_name
        self.from_bucket = from_bucket
        self.to_bucket = to_bucket
        self.new_file_name = new_file_name
        self.sub_folder = sub_folder

    def move_file(self):
        try:
            logging.debug('Started moving file ' + self.file_name)
            new_blob_name = str(self.sub_folder) + str(self.new_file_name)
            storage_client = storage.Client()
            source_bucket = storage_client.get_bucket(self.from_bucket)
            source_blob = source_bucket.blob(self.file_name)
            destination_bucket = storage_client.get_bucket(self.to_bucket)
            # copy file
            new_blob = source_bucket.copy_blob(source_blob, destination_bucket, new_blob_name)
            # delete file
            source_blob.delete()
            logging.debug('File {} Successfully Moved '.format(source_blob))
        except Exception as e:
            logging.error('Error copying file \'%s\'. Cause: %s' % (self.file_name, traceback.format_exc()))
            raise e
        finally:
            logging.info('finally tried to move files')
