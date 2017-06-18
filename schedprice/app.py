"""Determines which portfolios need pricing and dispatchs pricing"""
import sys
import time
import uuid
import logging
import datetime
import psycopg2
import psycopg2.extras
import pika
import yaml

from saifu.core import models, utils, runtime

def _exchange_connect(settings):
    """Creates a connection to message queue broker from settings"""
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=settings.mq_host,
            credentials=pika.credentials.PlainCredentials(
                username=settings.mq_creds["username"],
                password=settings.mq_creds["password"])))
    return conn, conn.channel()

def _db_connect(settings):
    return psycopg2.connect(
        database=settings.db_schema,
        user=settings.db_creds["username"],
        password=settings.db_creds["password"],
        host=settings.db_host)

class Settings(object):
    """Application settings"""
    def __init__(self, store):
        conf = store["conf"]

        log = conf["log"]
        self.logging = models.LoggingSettings()
        self.logging.from_json(log)

        app = conf["app"]
        self.pull_delay = app["pull_delay"]

        app = conf["app"]
        dbc = app["db"]
        self.db_host = dbc["host"]
        self.db_schema = dbc["schema"]
        self.db_creds = dbc["creds"]

        mqc = app["mq"]
        self.mq_host = mqc["host"]
        self.mq_exchange = mqc["exchange"]
        self.mq_creds = mqc["creds"]


class Dispatcher(object):
    def __init__(self, logger, settings):
        self.logger = logger
        self.settings = settings
        self.connection = None
        self.exchange = None
        self._connect()

    def _connect(self):
        self.connection, self.channel = _exchange_connect(self.settings)
        self.channel.queue_declare(queue='pricing_queue', durable=True)

    def dispatch(self, job):
        """Dispatch pricing job"""
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key='pricing_queue',
                body={},
                properties=pika.BasicProperties(delivery_mode=2))
        except pika.exceptions.ConnectionClosed:
            self.logger.warn("Lost connection with MQ, trying to reconnect")
            self._connect()
            self.logger.info("Connection to MQ re-established")

class PricingJob(object):
    def __init__(self, identifier=None, portfolio_id=None, snapshot_time=None):
        self.identifier = identifier
        self.portfolio_id = portfolio_id
        self.snapshot_time = snapshot_time

class Accessor(object):
    def __init__(self, logger, settings):
        self.connection = _db_connect(settings)
        self.connection.set_session(readonly=False, autocommit=True)
        self.logger = logger

    def find_dirty_portfolios(self):
        """Find all the portfolios that need pricing"""
        query = """
            SELECT sp.id as portfolio_id
              FROM saifu_portfolios sp
              JOIN saifu_portfolio_pricing_settings spps ON sp.id = spps.portfolio_id
         LEFT JOIN (SELECT portfolio_id,
                           MAX(start_time) as last_start_time
                      FROM saifu_portfolio_pricing_jobs
                  GROUP BY portfolio_id) sppj ON sp.id = sppj.portfolio_id
             WHERE EXTRACT(
                       EPOCH FROM (
                           now() - coalesce(
                                       sppj.last_start_time,
                                       to_timestamp(0)
                               ))) > spps.pricing_interval
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            yield row[0]
        cursor.close()

    def persist_job(self, job):
        """Will persist a job in the database"""
        is_update = True
        if job.identifier is None:
            is_update = False
            job.identifier = uuid.uuid1().hex

        assert job.portfolio_id is not None
        assert job.snapshot_time is not None

        cursor = self.connection.cursor()

        if is_update:
            assert False
        else:
            query = """
                INSERT INTO saifu_portfolio_pricing_jobs
                    (id, portfolio_id, status, started_by, snapshot_time)
                VALUES
                    (%s, %s, %s, %s, %s)
            """
            cursor.execute(
                query,
                (
                    job.identifier,
                    job.portfolio_id,
                    "N",
                    "SCHEDULER",
                    job.snapshot_time
                ))
            cursor.close()

def main():
    """Application entry-point"""
    path = sys.argv[1]
    with open(path) as settings_file:
        settings_data = yaml.load(settings_file)

    settings = Settings(settings_data)
    logger = runtime.create_logger(settings.logging)

    read_accessor = Accessor(logger, settings)
    write_accessor = Accessor(logger, settings)

    dispatcher = Dispatcher(logger, settings)

    while True:
        logger.debug("Will fetch dirty portfolios and require pricing")
        snapshot_time = datetime.datetime.now()

        pricing_jobs_created = 0
        for portfolio_id in read_accessor.find_dirty_portfolios():
            write_accessor.persist_job(
                PricingJob(
                    portfolio_id=portfolio_id,
                    snapshot_time=snapshot_time))
            dispatcher.dispatch(None)
            pricing_jobs_created += 1


        logger.debug("Required pricing for {} portfolio(s)".format(pricing_jobs_created))

        time.sleep(settings.pull_delay)



if __name__ == "__main__":
    main()
