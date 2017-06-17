"""Tick updates ingester (Updates saifudb with the last tick pricers)"""
import sys
import json
import logging
import datetime
import contextlib
import psycopg2
import pika
import yaml

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

def _create_logger(settings):
    """Creates the application logger from the settings"""
    logger = logging.getLogger(settings.log_category)
    logger.setLevel(logging.DEBUG)

    sth = logging.StreamHandler()
    sth.setLevel(logging.DEBUG)
    formatter = logging.Formatter(settings.log_format)
    sth.setFormatter(formatter)
    logger.addHandler(sth)

    return logger

@contextlib.contextmanager
def _profile(scope, logger):
    start = datetime.datetime.now()
    yield
    end = datetime.datetime.now()
    logger.debug("{} in {}s".format(scope, (end - start).microseconds / 1e6))


class Config(object):
    def __init__(self, store):
        conf = store["conf"]

        log = conf["log"]
        self.log_category = log["category"]
        self.log_location = log["location"]
        self.log_level = log["level"]
        self.log_stdout_level = log["stdout_level"]
        self.log_format = log["format"]

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
        updates = json.loads(body)
        self.ingester.ingest(updates)

    def run(self):
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

    def ingest(self, updates):
        """Ingests the provided updates"""
        cursor = self.connection.cursor()

        self.logger.debug("Will ingest {} updates".format(len(updates)))
        with _profile("Ingested all updates", self.logger):
            for ticker, update in updates.iteritems():
                try:
                    cursor.execute(
                        """INSERT INTO saifu_ccy_historical_prices (
                              ticker, price, quote_time)
                                VALUES (%s, %s, %s)""",
                        (ticker,
                         update["price"],
                         datetime.datetime.fromtimestamp(update["timestamp"]))
                    )
                except psycopg2.Error as err:
                    self.logger.warn("Failed to persist ticker {}: {}".format(
                        ticker, str(err)))
            self.connection.commit()


def main():
    """Application entry-point"""
    path = sys.argv[1]
    with open(path) as settings_file:
        settings_data = yaml.load(settings_file)

    config = Config(settings_data)
    logger = _create_logger(config)

    ingester = Ingester(logger, config)

    subscriber = Subscriber(logger, config, ingester)
    subscriber.run()

if __name__ == '__main__':
    main()
