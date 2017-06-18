"""Database access layer"""
import uuid


class BaseRepository(object):
    def __init__(self, connector):
        self._conn = connector.connect()

    def _get_conn(self):
        return self._conn

class JobsRepository(BaseRepository):
    def __init__(self, connector):
        super(JobsRepository, self).__init__(connector)

    def _persist_new(self, cursor, job):
        query = """
            INSERT INTO saifu_portfolio_pricing_jobs
                (id, portfolio_id, status, target_ccy, started_by, snapshot_time, start_time)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s)
        """
        job.identifier = uuid.uuid1().hex
        cursor.execute(query,
            (
                job.identifier,
                job.portfolio_id,
                job.status,
                job.target_ccy,
                job.started_by,
                job.snapshot_time,
                job.start_time))

    def persist(self, job):
        """Persist one job"""
        self.persist_many([job])

    def persist_many(self, jobs):
        """Persist many jobs"""
        with self._get_conn().cursor() as cursor:
            for job in jobs:
                if job.identifier is None:
                    self._persist_new(cursor, job)
                else:
                    raise RuntimeError("Not implemented")
            self._get_conn().commit()

class PricingRepository(BaseRepository):
    def __init__(self, connector):
        super(PricingRepository, self).__init__(connector)

    def find_portfolios_to_price(self):
        """Find all the portfolios identifiers that need pricing"""
        query = """
            SELECT sp.id as portfolio_id,
                   spps.target_ccy as target_currency
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
        with self._get_conn().cursor() as cursor:
            cursor.execute(query)
            results = [(row[0], row[1]) for row in cursor.fetchall()]
            self._get_conn().commit()
            return results
