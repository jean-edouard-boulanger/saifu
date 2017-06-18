"""Saifu business objects"""
from saifu.core import utils


class Quote(object):
    """Represents a quote"""
    def __init__(self, ticker=None, price=None, timestamp=None):
        self.ticker = ticker
        self.price = price
        self.timestamp = timestamp

    def to_json(self):
        return {
            "ticker": self.ticker,
            "price": self.price,
            "timestamp": utils.to_timestamp(self.timestamp)
        }

    def from_json(self, data):
        self.ticker = data.get("ticker")
        self.price = data.get("price")
        self.timestamp = utils.utc_from_timestamp(data.get("timestamp"))

class PricingJob(object):
    def __init__(self,
            identifier=None,
            portfolio_id=None,
            snapshot_time=None,
            target_ccy=None,
            started_by=None,
            status=None,
            start_time=None,
            end_time=None):
        self.identifier = identifier
        self.portfolio_id = portfolio_id
        self.snapshot_time = snapshot_time
        self.target_ccy = target_ccy
        self.started_by = started_by
        self.status = status
        self.start_time = start_time
        self.end_time = end_time

    def to_json(self):
        return {
            "identifier": self.identifier,
            "portfolio_id": self.portfolio_id,
            "snapshot_time": utils.to_timestamp(self.snapshot_time),
            "target_ccy": self.target_ccy,
            "started_by": self.started_by,
            "status": self.status,
            "start_time": utils.to_timestamp(self.start_time),
            "end_time": utils.to_timestamp(self.end_time),
        }

    def from_json(self, data):
        self.identifier = data.get("identifier")
        self.portfolio_id = data.get("portfolio_id")
        self.snapshot_time = utils.utc_from_timestamp(data.get("snapshot_time"))
        self.target_ccy = data.get("target_ccy")
        self.started_by = data.get("started_by")
        self.status = data.get("status")
        self.start_time = utils.utc_from_timestamp(data.get("start_time"))
        self.end_time = utils.utc_from_timestamp(data.get("end_time"))


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
