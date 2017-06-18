"""Tick updates ingester (Updates saifudb with the last tick pricers)"""
import sys
import cPickle
import datetime
import contextlib
import psycopg2
import pika
import yaml

from saifu.core import models, runtime
from saifu.core.system import mq, db, mt

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


class Subscriber(mq.GenericSubscriber):
    """Subscribes to quote updates and ingests them"""
    def __init__(self, logger, exchange, connector, ingester):
        super(Subscriber, self).__init__(exchange, connector)
        self.logger = logger
        self.ingester = ingester

    def received(self, message):
        self.ingester.ingest(
            cPickle.loads(message))


def main():
    """Application entry-point"""
    path = sys.argv[1]
    with open(path) as settings_file:
        settings_data = yaml.load(settings_file)

    settings = Settings(settings_data)
    logger = runtime.create_logger(settings.logging)

    subscriber = Subscriber(
        logger.getChild("sub"),
        settings.exchange,
        mq.Connector(settings.mq),
        Ingester(
            logger.getChild("ingest"),
            db.Connector(settings.database)))

    mt.ThreadManager(subscriber).start()

if __name__ == '__main__':
    main()
