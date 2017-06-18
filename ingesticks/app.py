"""Tick updates ingester (Updates saifudb with the last tick pricers)"""
import sys
import cPickle
import datetime
import contextlib
import psycopg2
import pika
import yaml

from saifu.core import models, runtime
from saifu.core.system import mq, db

class Settings(object):
    """Configuration for the current application"""
    def __init__(self, store):
        conf = store["conf"]

        app = conf["app"]
        self.exchange = app["exchange"]

        self.logging = models.LoggingSettings()
        self.logging.from_json(conf["logging"])

        self.database = models.DatabaseSettings()
        self.database.from_json(app["database"])

        self.mq = models.MQSettings()
        self.mq.from_json(app["mq"])


class Subscriber(object):
    """Receives ticker updates and persists them"""
    def __init__(self, logger, connector, ingester, exchange):
        self.logger = logger
        self.exchange = exchange
        self.ingester = ingester
        self.connector = connector

    def _connect(self):
        self.logger.info("Connecting to MQ broker")
        connection = self.connector.connect()
        channel = connection.channel()
        channel.exchange_declare(exchange=self.exchange, type='fanout')
        return channel

    def _received(self, channel, method, properties, body):
        updates = cPickle.loads(body)
        self.ingester.ingest(updates)

    def run(self):
        """Subscriber entry point"""
        while True:
            try:
                channel = self._connect()
                queue_name = channel.queue_declare(exclusive=True).method.queue
                channel.basic_consume(self._received, queue=queue_name, no_ack=True)
                channel.queue_bind(exchange=self.exchange, queue=queue_name)
                channel.start_consuming()
            except pika.exceptions.ConnectionClosed:
                self.logger.warn("Lost connection with MQ, will reconnect")


class Ingester(object):
    """Ingests quote updates"""
    def __init__(self, logger, connector):
        self.logger = logger
        self.connection = connector.connect()

    def ingest(self, quotes):
        """Ingests the provided quotes"""
        self.logger.debug("Will ingest {} updates".format(len(quotes)))
        with self.connection.cursor() as cursor:
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

    settings = Settings(settings_data)
    logger = runtime.create_logger(settings.logging)

    ingester = Ingester(logger, db.Connector(settings.database))

    subscriber = Subscriber(
        logger,
        mq.Connector(settings.mq),
        ingester,
        settings.exchange)

    subscriber.run()

if __name__ == '__main__':
    main()
