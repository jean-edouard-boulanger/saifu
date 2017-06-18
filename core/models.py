"""Saifu business objects"""
from saifu.core import utils


class Quote(object):
    """Represents a quote"""
    def __init__(self, ticker, price, timestamp):
        self.ticker = ticker
        self.price = price
        self.timestamp = timestamp


class PricingJob(object):
    def __init__(self,
            identifier=None,
            portfolio_id=None,
            snapshot_time=None,
            target_ccy=None,
            started_by=None,
            status=None,
            start_time=None):
        self.identifier = identifier
        self.portfolio_id = portfolio_id
        self.snapshot_time = snapshot_time
        self.target_ccy = target_ccy
        self.started_by = started_by
        self.status = status
        self.start_time = start_time


class BasicCredentials(object):
    """Basic credentials (Username, password)"""
    def __init__(self, username=None, password=None):
        self.username = username
        self.password = password

    def from_json(self, data):
        """Hydrate the current instance with json data"""
        self.username = data.get("username")
        self.password = data.get("password")


class LoggingSettings(object):
    """Logging settings"""
    def __init__(self, category=None, location=None, log_format=None, level=None):
        self.category = category
        self.location = location
        self.log_format = log_format
        self.level = level

    def from_json(self, data):
        """Hydrate the current instance with json data"""
        self.category = data.get("category")
        self.location = data.get("location")
        self.log_format = data.get("format")
        self.level = data.get("level")


class DatabaseSettings(object):
    """Database connection settings"""
    def __init__(self, host=None, database=None, credentials=None):
        self.host = host
        self.database = database
        self.credentials = credentials

    def from_json(self, data):
        """Hydrate the current instance with json data"""
        self.host = data.get("host")
        self.database = data.get("database")
        self.credentials = BasicCredentials()
        self.credentials.from_json(
            data.get("credentials"))


class MQSettings(object):
    """Message queue connection settings"""
    def __init__(self, host=None, credentials=None):
        self.host = host
        self.credentials = credentials

    def from_json(self, data):
        """Hydrate the current instance with json data"""
        self.host = data.get("host")
        self.credentials = BasicCredentials()
        self.credentials.from_json(
            data.get("credentials"))
