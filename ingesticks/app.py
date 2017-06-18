"""Tick updates ingester (Updates saifudb with the last tick pricers)"""
import sys
import cPickle
import datetime
import contextlib
import psycopg2
import pika
import yaml

from saifu.core import models, runtime

def _exchange_connect(settings):
    """Creates a connection to message queue broker from settings"""
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=settings.mq_host,
            credentials=pika.credentials.PlainCredentials(
                username=settings.mq_creds["username"],
                password=settings.mq_creds["password"])))
    return conn

def _db_connect(settings):
    return psycopg2.connect(
        database=settings.db_schema,
        user=settings.db_creds["username"],
        password=settings.db_creds["password"],
        host=settings.db_host)


class Config(object):
    """Configuration for the current application"""
    def __init__(self, store):
        conf = store["conf"]

        log = conf["log"]
        self.logging = models.LoggingSettings()
        self.logging.from_json(log)

        app = conf["app"]
        dbc = app["db"]
        self.db_host = dbc["host"]
        self.db_schema = dbc["schema"]
        self.db_creds = dbc["creds"]

        mqc = app["mq"]
        self.mq_host = mqc["host"]
        self.mq_exchange = mqc["exchange"]
        self.mq_creds = mqc["creds"]


class Subscriber(object):
    """Receives ticker updates and persists them"""
    def __init__(self, logger, settings, ingester):
        self.logger = logger
        self.settings = settings
        self.ingester = ingester

    def _connect(self):
        connection = _exchange_connect(self.settings)
        channel = connection.channel()
        channel.exchange_declare(
            exchange=self.settings.mq_exchange,
            type='fanout')
        return connection, channel

    def _received(self, channel, method, properties, body):
        updates = cPickle.loads(body)
        self.ingester.ingest(updates)

    def run(self):
        """Subscriber entry point"""
        while True:
            try:
                self.logger.info("Connecting to MQ broker")
                _, channel = self._connect()
                queue_name = channel.queue_declare(exclusive=True).method.queue
                channel.basic_consume(self._received, queue=queue_name, no_ack=True)
                channel.queue_bind(
                    exchange=self.settings.mq_exchange,
                    queue=queue_name)

                channel.start_consuming()
            except pika.exceptions.ConnectionClosed:
                self.logger.warn("Lost connection with MQ, will reconnect")


class Ingester(object):
    def __init__(self, logger, settings):
        self.settings = settings
        self.logger = logger
        self.connection = _db_connect(settings)

    def ingest(self, quotes):
        """Ingests the provided updates"""
        cursor = self.connection.cursor()

        self.logger.debug("Will ingest {} updates".format(len(quotes)))
        for quote in quotes:
            try:
                cursor.execute(
                    """INSERT INTO saifu_ccy_historical_prices (
                          ticker, price, quote_time)
                            VALUES (%s, %s, %s)""",
                    (quote.ticker, quote.price, quote.timestamp))
            except psycopg2.Error as err:
                self.logger.warn("Failed to persist ticker {}: {}".format(
                    quote.ticker, str(err)))


def main():
    """Application entry-point"""
    path = sys.argv[1]
    with open(path) as settings_file:
        settings_data = yaml.load(settings_file)

    config = Config(settings_data)
    logger = runtime.create_logger(config.logging)

    ingester = Ingester(logger, config)

    subscriber = Subscriber(logger, config, ingester)
    subscriber.run()

if __name__ == '__main__':
    main()
