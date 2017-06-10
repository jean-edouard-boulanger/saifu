"""Aggregates market data updates in a given time window"""
import logging
import time
import sys
import json
import threading
import Queue
import copy
import datetime
import yaml
import pika


def timestamp():
    """Returns the current timestamp in local time"""
    return int(time.time())


def connect(settings):
    """Creates a connection to message queue broker from settings"""
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=settings.mq_host,
            credentials=pika.credentials.PlainCredentials(
                username=settings.mq_username,
                password=settings.mq_password)))
    return conn


class Aggregation(object):
    def __init__(self):
        self.cache = {}

    def insert(self, update):
        pair = "{}{}".format(update["source"], update["target"])
        self.cache[pair] = update

    def get_all(self):
        return copy.deepcopy(self.cache)

    def clear(self):
        self.cache = {}


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
        self.agg_window = app["agg_window"]

        mqc = app["mq"]
        self.mq_host = mqc["host"]
        self.mq_username = mqc["creds"]["username"]
        self.mq_password = mqc["creds"]["password"]
        self.mq_sub_exchange = mqc["sub_exchange"]
        self.mq_pub_exchange = mqc["pub_exchange"]


class Subscriber(threading.Thread):
    """Asynchronously subscribes to quotes updates and aggregates data"""
    def __init__(self, logger, settings, callback):
        super(Subscriber, self).__init__()
        self.logger = logger
        self.settings = settings
        self.connection = connect(settings)
        self.callback = callback
        self.nextbatch = timestamp() + settings.agg_window
        self.agg = Aggregation()

    def received(self, channel, method, properties, body):
        """Callback when message received"""
        pair = json.loads(body)
        self.logger.debug("Received update for currency pair {}{}@{} (ts={})".format(
            pair["source"],
            pair["target"],
            pair["rate"],
            pair["timestamp"]
        ))

        self.agg.insert(pair)

        if timestamp() >= self.nextbatch:
            agg = self.agg.get_all()
            self.agg.clear()

            self.logger.debug("Updating publisher with {} update(s)".format(
                len(agg)))

            self.callback(agg)

            self.nextbatch = timestamp() + self.settings.agg_window
            self.logger.debug("Next update scheduled from {}".format(
                datetime.datetime.fromtimestamp(self.nextbatch)))

    def run(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange=self.settings.mq_sub_exchange,
                                 type='fanout')

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange=self.settings.mq_sub_exchange,
                           queue=queue_name)

        channel.basic_consume(self.received, queue=queue_name, no_ack=True)
        channel.start_consuming()


class Publisher(threading.Thread):
    """Asynchronously publishes aggregated data updates"""
    def __init__(self, logger, settings):
        super(Publisher, self).__init__()
        self.connection = connect(settings)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=settings.mq_pub_exchange,
            type='fanout')

        self.queue = Queue.Queue()
        self.logger = logger

    def notify(self, update):
        """Notifies the publisher with an update"""
        self.queue.put(update)

    def run(self):
        wait = 10
        while True:
            try:
                updates = self.queue.get(timeout=wait)
                self.logger.debug("Received {} updates for: {}".format(
                    len(updates),
                    ", ".join(updates.keys())))
            except Queue.Empty:
                self.logger.debug("Update queue is empty after {}s".format(wait))


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


def main():
    """Application entry-point"""
    path = sys.argv[1]
    with open(path) as settings_file:
        settings_data = yaml.load(settings_file)

    settings = Settings(settings_data)
    logger = create_logger(settings)

    logger.info("Initializing mktagg")

    logger.info("Starting market data publisher")
    publisher = Publisher(logger, settings)
    publisher.start()

    logger.info("Starting market data subscriber")
    subscriber = Subscriber(logger, settings, publisher.notify)
    subscriber.start()

    logger.info("Ready")

    publisher.join()
    subscriber.join()

if __name__ == "__main__":
    main()
