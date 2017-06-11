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


def _get_timestamp():
    """Returns the current timestamp in local time"""
    return int(time.time())


def _connect(settings):
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
        self.agg = {}

    def insert(self, update):
        """Inserts quote in the aggregation"""
        pair = "{}{}".format(update["source"], update["target"])
        self.agg[pair] = update

    def get_all(self):
        """Returns a copy of the current aggregation"""
        return copy.deepcopy(self.agg)

    def clear(self):
        """Clear the current aggregation"""
        self.agg = {}


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
        self.connection = None
        self.channel = None

        self.logger = logger
        self.settings = settings
        self.callback = callback
        self.agg = Aggregation()
        self.nextbatch = _get_timestamp() + self.settings.agg_window

        self.working = True

        self._connect()

    def _connect(self):
        self.connection = _connect(self.settings)

    def _received(self, channel, method, properties, body):
        """Callback when message received"""
        quote = json.loads(body)
        self.logger.debug("Received update for currency pair {}{}@{} (ts={})".format(
            quote["source"],
            quote["target"],
            quote["price"],
            quote["timestamp"]
        ))

        self.agg.insert(quote)

        if _get_timestamp() >= self.nextbatch:
            self.logger.debug(
                "End of current aggregation window (ts={})".format(
                    self.nextbatch))

            agg = self.agg.get_all()
            self.agg.clear()

            self.logger.debug("Updating publisher with {} update(s)".format(
                len(agg)))

            # Calling output callback
            self.callback(agg)

            self.nextbatch = _get_timestamp() + self.settings.agg_window
            self.logger.debug("Next update scheduled from {}".format(
                datetime.datetime.fromtimestamp(self.nextbatch)))

    def stop(self):
        """Stops the thread"""
        self.working = False
        self.channel.stop_consuming()

    def run(self):
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.settings.mq_sub_exchange,
            type='fanout')

        result = self.channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        self.channel.queue_bind(
            exchange=self.settings.mq_sub_exchange,
            queue=queue_name)

        self.channel.basic_consume(self._received, queue=queue_name, no_ack=True)

        while self.working:
            try:
                self.channel.start_consuming()
            except pika.exceptions.ConnectionClosed:
                self.logger.warn("Lost connection with MQ, trying to reconnect")
                self._connect()
                self.logger.info("Connection to MQ is back")
        self.logger.info("Subscriber is going down")


class Publisher(threading.Thread):
    """Asynchronously publishes aggregated data updates"""
    def __init__(self, logger, settings):
        super(Publisher, self).__init__()
        self.connection = _connect(settings)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=settings.mq_pub_exchange,
            type='fanout')

        self.queue = Queue.Queue()
        self.logger = logger
        self.working = True

    def stop(self):
        self.working = False

    def notify(self, update):
        """Notifies the publisher with an update"""
        self.queue.put(update)

    def run(self):
        wait = 5
        while self.working:
            try:
                updates = self.queue.get(timeout=wait)
                self.logger.debug("Received {} updates for: {}".format(
                    len(updates),
                    ", ".join(updates.keys())))
            except Queue.Empty:
                self.logger.debug("Update queue is empty after {}s".format(wait))

        self.logger.info("Publisher is going down")

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

    threads = []

    logger.info("Initializing market data publisher")
    publisher = Publisher(logger, settings)
    threads.append(publisher)

    logger.info("Initializing market data subscriber")
    subscriber = Subscriber(logger, settings, publisher.notify)
    threads.append(subscriber)

    logger.info("Starting worker threads")
    [t.start() for t in threads]

    logger.info("Market data aggregator is ready to operate")

    should_monitor = True
    while should_monitor:
        if not all([t.isAlive() for t in threads]):
            logger.warn("Detected a thread died, stopping mktagg")
            [t.stop() for t in threads]
            should_monitor = False
        else:
            time.sleep(5)

if __name__ == "__main__":
    main()
