"""Database components module"""
import psycopg2

class Connector(object):
    """Connector to PG database"""
    def __init__(self, settings):
        self.settings = settings

    def connect(self):
        """Creates an opened connection to a PG database"""
        return psycopg2.connect(
            database=self.settings.database,
            user=self.settings.credentials.username,
            password=self.settings.credentials.password,
            host=self.settings.host)
