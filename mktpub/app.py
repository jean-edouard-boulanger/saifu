"""Fetches crypto currencies quotes and publishes them"""
import logging
import time
import sys
import cPickle
import yaml
import pika

import quotesrequester
from saifu.core import models, runtime


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
        self.logging = models.LoggingSettings()
        self.logging.from_json(log)

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
                body=cPickle.dumps(quote))
        except pika.exceptions.ConnectionClosed:
            self.logger.warn("Lost connection with MQ, trying to reconnect")
            self._connect()
            self.logger.info("Connection to MQ re-established")

def main_loop(logger, publisher, requester, pairs):
    """Main loop execution"""
    try:
        for quote in requester.get(pairs):
            logger.debug("Publishing quote to exchange {}@{}".format(
                quote.ticker,
                quote.price))

            publisher.publish(quote)
    except quotesrequester.RequesterException as error:
        logger.warn("Failed to get quotes ({})".format(error))


def main():
    """Application entry-point"""
    path = sys.argv[1]
    with open(path) as settings_file:
        settings_data = yaml.load(settings_file)

    settings = Settings(settings_data)
    logger = runtime.create_logger(settings.logging)

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
