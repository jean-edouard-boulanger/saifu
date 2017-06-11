"""Fetches currencies quotes and publishes them"""
import logging
import time
import sys
import json
import yaml
import pika
import quotesrequester


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
        self.source_ccys = app["ccy"]["sources"]
        self.target_ccys = app["ccy"]["targets"]


class Publisher(object):
    """Publishes aggregated updates"""
    def __init__(self, logger, host, exchange, creds):
        self.logger = logger

        self.connection = None
        self.channel = None
        self.host = host
        self.exchange = exchange
        self.creds = creds

        self._connect()

    def _connect(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                credentials=pika.credentials.PlainCredentials(
                    username=self.creds["username"],
                    password=self.creds["password"])))

        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange,
            type='fanout')

    def publish(self, quote):
        """Publishes quotes to the exchange"""
        try:
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key='',
                body=json.dumps(quote.serialize()))
        except pika.exceptions.ConnectionClosed:
            self.logger.warn("""Lost connection with MQ, trying to reconnect""")
            self._connect()
            self.logger.warn("Connection to MQ is back")


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

def main_loop(logger, publisher, requester, settings):
    """Main loop execution"""
    try:
        sources = settings.source_ccys
        targets = settings.target_ccys

        for pair in requester.get(sources, targets):
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
    publisher = Publisher(
        logger,
        settings.pub_host,
        settings.pub_exchange,
        settings.pub_creds)

    logger.info("Publisher is ready to operate")
    while True:
        main_loop(logger, publisher, requester, settings)
        time.sleep(settings.pull_delay)

if __name__ == "__main__":
    main()
