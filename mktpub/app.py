"""Fetches currencies quotes and publishes them"""
import logging
import time
import sys
import json
import yaml
import requests
import pika


HTTP_STATUS_CODE_OK = 200


class Settings(object):
    def __init__(self, store):
        conf = store["conf"]

        log = conf["log"]
        self.log_category = log["category"]
        self.log_location = log["location"]
        self.log_level = log["level"]
        self.log_stdout_level = log["stdout_level"]
        self.log_format = log["format"]

        app = conf["app"]
        self.pull_delay = app["pull_delay"]
        self.publisher_type = app["pub"]["type"]
        self.publisher_settings = app["pub"]["settings"]

        self.resource = app["res"]
        self.source_ccys = app["ccy"]["sources"]
        self.target_ccys = app["ccy"]["targets"]


def extract_pairs(tsp, data):
    """Extract the currency pairs from a response"""
    for source, targets in data.iteritems():
        for target, price in targets.iteritems():
            yield {
                "source": source,
                "target": target,
                "rate": price,
                "timestamp": tsp
            }


class QuotesRequester(object):
    def __init__(self, resource):
        self.resource = resource

    def __call__(self):
        try:
            response = requests.post(self.resource)
            if response.status_code != HTTP_STATUS_CODE_OK:
                raise RuntimeError("Service responded with an error (rc={})".format(
                    response.status_code))
            else:
                data = response.json()
                for pair in extract_pairs(timestamp(), data):
                    yield pair
        except Exception as error:
            raise RuntimeError("Unable to send request to service ({})".format(
                str(error)))


class StdoutPublisher(object):
    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def __call__(self, data):
        print data


class RmqPublisher(object):
    def __init__(self, host, exchange, creds):
        self.connection = None
        self.channel = None

        self.host = host
        self.exchange = exchange
        self.username = creds["username"]
        self.password = creds["password"]

    def __enter__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                credentials=pika.credentials.PlainCredentials(
                    username=self.username,
                    password=self.password)))

        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange,
            type='fanout')

        return self

    def __exit__(self, type, value, traceback):
        self.connection.close()

    def __call__(self, data):
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key='',
            body=json.dumps(data))


def build_url(base, sources, targets):
    """Builds resource url from settings"""
    return base.format(**{
        "sources": ",".join(sources),
        "targets": ",".join(targets)
    })

def timestamp():
    """Returns the current timestamp in local time"""
    return int(time.time())

def create_publisher(of_type, settings):
    """Creates a publisher with the settings"""
    publishers = {
        'stdout': StdoutPublisher,
        'rmq': RmqPublisher
    }
    return publishers[of_type](**settings)

def log_level_from_string(level_string):
    """Get python logging from string"""
    mapping = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARN,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }
    return mapping[level_string]

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

def main_loop(logger, publisher, requester):
    """Main loop execution"""
    try:
        for pair in requester():
            logger.debug("Publishing currency pair {}{}@{} (ts={})".format(
                pair["source"],
                pair["target"],
                pair["rate"],
                pair["timestamp"]
            ))
            publisher(pair)
    except RuntimeError as error:
        logger.warn("Failed to get quotes from service ({})".format(error))

def main():
    """Application entry-point"""
    path = sys.argv[1]
    with open(path) as settings_file:
        settings_data = yaml.load(settings_file)

    settings = Settings(settings_data)
    logger = create_logger(settings)

    logger.info("Initializing mktpub")

    request_url = build_url(
        settings.resource,
        settings.source_ccys,
        settings.target_ccys)

    requester = QuotesRequester(request_url)

    pub_type = settings.publisher_type
    with create_publisher(pub_type, settings.publisher_settings) as publisher:
        logger.info("Ready")
        while True:
            main_loop(logger, publisher, requester)
            time.sleep(settings.pull_delay)

if __name__ == "__main__":
    main()
