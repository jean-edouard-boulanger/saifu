"""Fetches currencies quotes and publishes them"""
import logging
import time
import sys
import json
import yaml
import pika
import quotesrequester


def _exchange_connect(settings):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=settings.pub_host,
            credentials=pika.credentials.PlainCredentials(
                username=settings.pub_creds["username"],
                password=settings.pub_creds["password"])))

    channel = connection.channel()
    channel.exchange_declare(
        exchange=settings.pub_exchange,
        type='fanout')

    return connection, channel

class Settings(object):
    """Application settings"""
    def __init__(self, store):
        conf = store["conf"]

        log = conf["log"]
        self.log_category = log["category"]
        self.log_location = log["location"]
        self.log_level = log["level"]
        self.log_stdout_level = log["stdout_level"]
        self.log_format = log["format"]

        app = conf["app"]
        self.pull_delay = app["pull_delay"]

        pub = app["pub"]
        self.pub_host = pub["host"]
        self.pub_exchange = pub["exchange"]
        self.pub_creds = pub["creds"]

        self.resource = app["res"]


class Publisher(object):
    """Publishes aggregated updates"""
    def __init__(self, logger, settings):
        self.logger = logger
        self.settings = settings

        self.connection = None
        self.exchange = None
        self._connect()

    def _connect(self):
        self.connection, self.channel = _exchange_connect(self.settings)

    def publish(self, quote):
        """Publishes quotes to the exchange"""
        try:
            self.channel.basic_publish(
                exchange=self.settings.pub_exchange,
                routing_key='',
                body=json.dumps(quote.serialize()))
        except pika.exceptions.ConnectionClosed:
            self.logger.warn("Lost connection with MQ, trying to reconnect")
            self._connect()
            self.logger.info("Connection to MQ re-established")

def create_logger(settings):
    """Creates the application logger from the settings"""
    logger = logging.getLogger(settings.log_category)
    logger.setLevel(logging.DEBUG)

    sth = logging.StreamHandler()
    sth.setLevel(logging.DEBUG)
    formatter = logging.Formatter(settings.log_format)
    sth.setFormatter(formatter)
    logger.addHandler(sth)

    return logger

def main_loop(logger, publisher, requester, pairs):
    """Main loop execution"""
    try:
        for pair in requester.get(pairs):
            logger.debug("Publishing currency pair {}{}@{} (ts={})".format(
                pair.source,
                pair.target,
                pair.price,
                pair.timestamp
            ))
            publisher.publish(pair)
    except quotesrequester.RequesterException as error:
        logger.warn("Failed to get quotes ({})".format(error))


def main():
    """Application entry-point"""
    path = sys.argv[1]
    with open(path) as settings_file:
        settings_data = yaml.load(settings_file)

    settings = Settings(settings_data)
    logger = create_logger(settings)

    logger.info("Initializing mktpub")

    requester = quotesrequester.Requester(logger, settings.resource)
    publisher = Publisher(logger, settings)

    pairs = [tuple(pair.split("_")) for pair in sys.argv[2:]]

    logger.info("Publisher is ready to operate")
    while True:
        main_loop(logger, publisher, requester, pairs)
        time.sleep(settings.pull_delay)

if __name__ == "__main__":
    main()
