"""Saifu business objects"""
from saifu.core import utils

class Quote(object):
    """Represents a quote"""
    def __init__(self, ticker, price, timestamp):
        self.ticker = ticker
        self.price = price
        self.timestamp = timestamp

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