"""Determines which portfolios need pricing and dispatchs pricing"""
import sys
import time
import uuid
import datetime
import pika
import yaml

from saifu.core import models, runtime, dbac, utils
from saifu.core.system import db, mq, mt

class Settings(object):
    """Application settings"""
    def __init__(self, store):
        conf = store["conf"]
        app = conf["app"]

        self.pull_delay = app["pull_delay"]
        self.work_queue = app["work_queue"]

        self.logging = models.LoggingSettings()
        self.logging.from_json(conf["log"])

        self.database = models.DatabaseSettings()
        self.database.from_json(app["database"])

        self.mq = models.MQSettings()
        self.mq.from_json(app["mq"])


class Dispatcher(mq.GenericDispatcher):
    def __init__(self, logger, settings, pricingrepo, jobsrepo, connector):
        super(Dispatcher, self).__init__(settings.work_queue, connector)
        self.logger = logger
        self.settings = settings
        self.pricingrepo = pricingrepo
        self.jobsrepo = jobsrepo

    def work(self):
        while self.running():
            snapshot_time = datetime.datetime.now()
            new_jobs = []
            for portfolio_id, target_ccy in self.pricingrepo.find_portfolios_to_price():
                new_jobs.append(models.PricingJob(
                    portfolio_id=portfolio_id,
                    snapshot_time=snapshot_time,
                    target_ccy=target_ccy,
                    started_by="SYSTEM",
                    status="N",
                    start_time=utils.utc_time()))

            self.logger.debug("Will dispatch {} new pricing job(s)".format(
                len(new_jobs)))

            self.jobsrepo.persist_many(new_jobs)
            for job in new_jobs:
                self.dispatch(
                    utils.serialize(job))

            time.sleep(self.settings.pull_delay)


def main():
    """Application entry-point"""
    path = sys.argv[1]
    with open(path) as settings_file:
        settings_data = yaml.load(settings_file)

    settings = Settings(settings_data)
    logger = runtime.create_logger(settings.logging)

    logger.debug("Initializing pricing job scheduler")

    dispatcher = Dispatcher(
        logger.getChild("sch"),
        settings,
        dbac.PricingRepository(db.Connector(settings.database)),
        dbac.JobsRepository(db.Connector(settings.database)),
        mq.Connector(settings.mq))

    mt.ThreadManager(dispatcher).start()

if __name__ == "__main__":
    main()
