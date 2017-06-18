"""Aggregates market data updates in a given time window"""
import sys
import threading
import Queue
import copy
import datetime
import yaml
import pika

from saifu.core import utils, models, runtime
from saifu.core.system import mq, mt


class Settings(object):
    """Application settings"""
    def __init__(self, store):
        conf = store["conf"]

        self.logging = models.LoggingSettings()
        self.logging.from_json(conf["logging"])

        app = conf["app"]
        self.aggregation_window = app["aggregation_window"]
        self.pub_exchange = app["pub_exchange"]
        self.sub_exchange = app["sub_exchange"]

        self.mq = models.MQSettings()
        self.mq.from_json(app["mq"])


class QuoteAggregation(object):
    """Aggregates quotes over a period of time"""
    def __init__(self):
        self.agg = {}

    def insert(self, quote):
        """Inserts a quote in the aggregation"""
        self.agg[quote.ticker] = quote

    def get_all(self):
        """Returns a copy of the current aggregation"""
        return copy.deepcopy(self.agg)

    def reset(self):
        """Clears the current aggregation"""
        self.agg = {}


class WindowAggregator(object):
    """Aggregates data over a pre-defined period of time"""
    def __init__(self, logger, window_size, callback):
        self.logger = logger
        self.window_size = window_size
        self.callback = callback
        self.aggregation = QuoteAggregation()
        self.window_end = utils.utc_time()

    def next_window(self):
        """Find the end of the next window from the current time"""
        return utils.utc_time_with_offset(
            datetime.timedelta(seconds=self.window_size))

    def is_window_end(self):
        """Determines if the current aggregation window is finished"""
        return utils.utc_time() > self.window_end

    def aggregate(self, quote):
        """Aggregates a new piece of data
        Calls the callback with the full aggregation if the current window is
        finished.
        """
        self.aggregation.insert(quote)
        if self.is_window_end():
            self.logger.debug("End of current aggregation window")
            self.callback(self.aggregation.get_all())
            self.aggregation.reset()
            self.window_end = self.next_window()


class Subscriber(mq.GenericSubscriber):
    """Asynchronously subscribes to quotes updates and aggregates market data"""
    def __init__(self, logger, exchange, aggregator, connector):
        super(Subscriber, self).__init__(exchange, connector)
        self.logger = logger
        self.aggregator = aggregator
        self.logger.info("Quotes subscriber is ready")

    def received(self, message):
        quote = utils.unserialize(message, models.Quote)
        self.logger.debug("Received quote {}@{}".format(
            quote.ticker, quote.price))

        self.aggregator.aggregate(quote)

class Publisher(mq.GenericPublisher):
    """publishes aggregated data updates to exchange"""
    def __init__(self, logger, exchange, connector):
        super(Publisher, self).__init__(exchange, connector)
        self.logger = logger
        self.queue = Queue.Queue()
        self.logger.info("Aggregated quotes publisher is ready")

    def notify(self, quote):
        """Notifies the publisher about a new quote"""
        self.queue.put(quote)

    def work(self):
        wait = 5
        while self.running():
            try:
                quotes = self.queue.get(timeout=wait)
                self.logger.debug("Will publish {} quote updates".format(
                    len(quotes)))

                self.publish(utils.serialize(quotes.values()))
            except Queue.Empty:
                self.logger.debug("Queue is empty after {}s".format(wait))

def main():
    """Application entry-point"""
    path = sys.argv[1]
    with open(path) as settings_file:
        settings_data = yaml.load(settings_file)

    settings = Settings(settings_data)
    logger = runtime.create_logger(settings.logging)

    logger.info("Initializing mktagg")

    logger.info("Initializing market data publisher")
    publisher = Publisher(
        logger.getChild("pub"),
        settings.pub_exchange,
        mq.Connector(settings.mq))

    logger.info("Initializing market data subscriber")
    subscriber = Subscriber(
        logger.getChild("sub"),
        settings.sub_exchange,
        WindowAggregator(
            logger.getChild("wagg"),
            settings.aggregation_window,
            publisher.notify),
        mq.Connector(settings.mq))

    mt.ThreadManager(subscriber, publisher).start()

if __name__ == "__main__":
    main()
