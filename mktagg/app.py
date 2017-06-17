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


def _exchange_connect(settings):
    """Creates a connection to message queue broker from settings"""
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=settings.mq_host,
            credentials=pika.credentials.PlainCredentials(
                username=settings.mq_creds["username"],
                password=settings.mq_creds["password"])))
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
        self.mq_creds = mqc["creds"]
        self.mq_sub_exchange = mqc["sub_exchange"]
        self.mq_pub_exchange = mqc["pub_exchange"]


class Subscriber(threading.Thread):
    """Asynchronously subscribes to quotes updates and aggregates data"""
    def __init__(self, logger, settings, callback):
        super(Subscriber, self).__init__()
        self.logger = logger
        self.settings = settings
        self.callback = callback
        self.agg = Aggregation()
        self.nextbatch = _get_timestamp() + self.settings.agg_window
        self.working = True

        self.connection = None
        self.exchange = None

    def _connect(self):
        self.connection = _exchange_connect(self.settings)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.settings.mq_sub_exchange,
            type='fanout')
        return self.connection, self.channel

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
        """Stops the subscriber thread"""
        self.working = False
        self.channel.stop_consuming()

    def run(self):
        while self.working:
            try:
                self.logger.info("Subscriber connecting to MQ broker")
                _, channel = self._connect()
                queue_name = channel.queue_declare(exclusive=True).method.queue
                channel.basic_consume(self._received, queue=queue_name, no_ack=True)
                channel.queue_bind(
                    exchange=self.settings.mq_sub_exchange,
                    queue=queue_name)

                channel.start_consuming()
            except pika.exceptions.ConnectionClosed:
                self.logger.warn("Lost connection with MQ, will reconnect")

        self.logger.info("Subscriber is going down")


class Publisher(threading.Thread):
    """Asynchronously publishes aggregated data updates"""
    def __init__(self, logger, settings):
        super(Publisher, self).__init__()
        self.logger = logger
        self.settings = settings
        self.queue = Queue.Queue()
        self.working = True

    def _connect(self):
        connection = _exchange_connect(self.settings)
        channel = connection.channel()
        channel.exchange_declare(
            exchange=self.settings.mq_pub_exchange,
            type='fanout')
        return connection, channel

    def _do_run(self, channel):
        wait = 5
        while True:
            try:
                updates = self.queue.get(timeout=wait)
                self.logger.debug("Will publish update for {} pairs".format(
                    len(updates)))
                channel.basic_publish(
                    exchange=self.settings.mq_pub_exchange,
                    routing_key='',
                    body=json.dumps(updates))
            except Queue.Empty:
                self.logger.debug("Queue is empty after {}s".format(wait))

    def stop(self):
        """Stops the publisher thread"""
        self.working = False

    def notify(self, update):
        """Notifies the publisher with an update"""
        self.queue.put(update)

    def run(self):
        while self.working:
            try:
                self.logger.info("Publisher connecting to MQ broker")
                _, channel = self._connect()
                self._do_run(channel)
            except pika.exceptions.ConnectionClosed:
                self.logger.warn("Lost connection with MQ, will reconnect")

        self.logger.info("Publisher is going down")

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


def main():
    """Application entry-point"""
    path = sys.argv[1]
    with open(path) as settings_file:
        settings_data = yaml.load(settings_file)

    settings = Settings(settings_data)
    logger = _create_logger(settings)

    logger.info("Initializing mktagg")

    threads = []

    logger.info("Initializing market data publisher")
    publisher = Publisher(logger, settings)
    threads.append(publisher)

    logger.info("Initializing market data subscriber")
    subscriber = Subscriber(logger, settings, publisher.notify)
    threads.append(subscriber)

    logger.info("Starting worker threads")
    for worker in threads:
        worker.start()

    logger.info("Market data aggregator is ready to operate")

    should_monitor = True
    while should_monitor:
        if not all([t.isAlive() for t in threads]):
            logger.warn("Detected a thread died, stopping mktagg")
            for worker in threads:
                worker.stop()
            should_monitor = False
        else:
            time.sleep(5)

if __name__ == "__main__":
    main()
