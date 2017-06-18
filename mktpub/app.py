"""Fetches crypto currencies quotes and publishes them"""
import time
import sys
import yaml
import pika

import quotesrequester
from saifu.core import models, runtime, utils
from saifu.core.system import mq, mt


class Settings(object):
    """Application settings"""
    def __init__(self, store):
        conf = store["conf"]

        app = conf["app"]
        self.pull_delay = app["pull_delay"]
        self.exchange = app["exchange"]
        self.resource = app["res"]

        log = conf["logging"]
        self.logging = models.LoggingSettings()
        self.logging.from_json(log)

        self.mq = models.MQSettings()
        self.mq.from_json(app["mq"])


class Publisher(mq.GenericPublisher):
    """Publishes quote updates"""
    def __init__(self, logger, settings, connector, requester):
        super(Publisher, self).__init__(settings.exchange, connector)
        self.logger = logger
        self.requester = requester
        self.settings = settings

    def work(self):
        while self.running():
            try:
                for quote in self.requester.get():
                    self.logger.debug("Publishing quote to exchange {}@{}".format(
                        quote.ticker,
                        quote.price))
                    self.publish(utils.serialize(quote))
                time.sleep(self.settings.pull_delay)
            except quotesrequester.RequesterException as error:
                self.logger.warn("Failed to get quotes ({})".format(error))
                time.sleep(self.settings.pull_delay)


def main():
    """Application entry-point"""
    path = sys.argv[1]
    with open(path) as settings_file:
        settings_data = yaml.load(settings_file)
    pairs = [tuple(pair.split("_")) for pair in sys.argv[2:]]

    settings = Settings(settings_data)
    logger = runtime.create_logger(settings.logging)

    logger.info("Initializing mktpub")

    publisher = Publisher(
        logger.getChild("pub"),
        settings,
        mq.Connector(settings.mq),
        quotesrequester.Requester(
            logger.getChild("req"),
            settings.resource,
            pairs))

    mt.ThreadManager(publisher).start()

if __name__ == "__main__":
    main()
