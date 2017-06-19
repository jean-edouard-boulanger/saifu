"""Prices a portfolio"""
import sys
import time
import uuid
import datetime
import pika
import yaml
import terminaltables

from saifu.core import models, runtime, dbac, utils
from saifu.core.system import db, mq, mt


class Settings(object):
    """Application settings"""
    def __init__(self, store):
        conf = store["conf"]
        app = conf["app"]

        self.work_queue = app["work_queue"]

        self.logging = models.LoggingSettings()
        self.logging.from_json(conf["log"])

        self.mq = models.MQSettings()
        self.mq.from_json(app["mq"])

        self.database = models.DatabaseSettings()
        self.database.from_json(app["database"])

class Worker(mq.GenericWorker):
    def __init__(self, logger, pricingrepo, queue, connector):
        super(Worker, self).__init__(queue, connector)
        self.logger = logger
        self.pricingrepo = pricingrepo

    def handle(self, job):
        job = utils.unserialize(job, models.PricingJob)
        self.logger.debug("Received pricing job {} for portfolio {}".format(
            job.identifier, job.portfolio_id))

        results = self.pricingrepo.get_portfolio_positions_prices(
            job.portfolio_id,
            job.snapshot_time,
            job.target_ccy)

        balance = sum(pos[3] for pos in results)
        self.logger.debug("Finished calculating balance for job {}: {} {}".format(
            job.identifier, balance, job.target_ccy))

        self.pricingrepo.persist_portfolio_pricing(
            job.portfolio_id,
            job.snapshot_time,
            balance,
            job.target_ccy)

def main():
    """Application entry-point"""
    path = sys.argv[1]
    with open(path) as settings_file:
        settings_data = yaml.load(settings_file)

    settings = Settings(settings_data)
    logger = runtime.create_logger(settings.logging)

    logger.debug("Initializing portprice")

    worker = Worker(
        logger.getChild("prc"),
        dbac.PricingRepository(db.Connector(settings.database)),
        settings.work_queue,
        mq.Connector(settings.mq))

    mt.ThreadManager(worker).start()

if __name__ == "__main__":
    main()
