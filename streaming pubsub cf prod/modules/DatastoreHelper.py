from google.cloud import datastore
import logging


class DatastoreHelper:

    def __init__(self, project,  my_kind, event_id):
        self.project = project
        self.kind = my_kind
        self.event_id = event_id

    def get_entity(self, my_property, my_operator, my_value):
        try:
            logging.info('started querying datastore')
            logging.info('Project ' + self.project)
            logging.info('Kind ' + self.kind)
            logging.info('my_value ' + my_value)
            client = datastore.Client(project=self.project)
            query = client.query(kind=self.kind)
            query.add_filter(my_property, my_operator, my_value)
            return query.fetch()
        except Exception as e:
            logging.error('Failed to query datastore' + e)
        finally:
            logging.info('finally Datastore config query')

    def insert_entity(self, data_object):
        try:
            client = datastore.Client(project=self.project)
            my_key = client.key(self.kind, self.event_id)
            task = datastore.Entity(key=my_key)
            task.update(data_object)
            client.put(task)
        except Exception as e:
            logging.error('failed to insert into datastore' + e)
            raise e
        finally:
            logging.info('finally insert into datastore')



