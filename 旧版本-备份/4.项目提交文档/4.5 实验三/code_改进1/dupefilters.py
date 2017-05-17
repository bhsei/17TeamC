from __future__ import print_function
from hashlib import *
import os
import math
import logging

from scrapy.utils.job import job_dir
from scrapy.utils.request import request_fingerprint
from filters import Filters
from mongodb_agent import MongoDBClient


class BaseDupeFilter(object):

    @classmethod
    def from_settings(cls, settings):
        return cls()

    def request_seen(self, request):
        return False

    def open(self):  # can return deferred
        pass

    def close(self, reason):  # can return a deferred
        pass

    def log(self, request, spider):  # log that a request has been filtered
        pass

class RFPDupeFilter(BaseDupeFilter):
    """Request Fingerprint duplicates filter"""

    def __init__(self, m_length, error_rate, db_host, db_port, \
                 dbname, dbcollections, path=None, debug=False):
        self.logdupes = True
        self.debug = debug
        self.logger = logging.getLogger(__name__)

        self.fingerprints = Filters(m_length, error_rate)
        self.logger.info("created bloomfilter({},{}) <----- wangyf"\
                .format(m_length, error_rate))

        self.mongodb = MongoDBClient(db_host, db_port, dbname, dbcollections)

    @classmethod
    def from_settings(cls, settings):
        debug = settings.getbool('DUPEFILTER_DEBUG')
        m_length = settings['MAX_LENGTH']
        error_rate = settings['ERROR_RATE']
        mongo_host = settings['DB_HOST']
        mongo_port = settings['DB_PORT']

        try:
            dbname = settings['DB_NAME']
            dbcollections = settings['DB_COLLECTIONS_NAME']
        except Exception:
            dbname = None
            dbcollections = None

        return cls(m_length, error_rate, mongo_host, mongo_port,\
                   dbname, dbcollections, job_dir(settings), debug)

    def request_seen(self, request):
        fp = self.request_fingerprint(request)
        if fp in self.fingerprints or fp in self.mongodb:
            self.logger.info("filtered request at {}  <-- wangyf".format(request.url))
            return True

        self.fingerprints.add(fp)
        return False

    def request_fingerprint(self, request):
        return request.url

    def close(self, reason):
        self.mongodb.close()
        return

    def log(self, request, spider):
        if self.debug:
            msg = "DupeFilter filtered duplicate request: %(request)s"
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
        elif self.logdupes:
            msg = ("DupeFilter filtered duplicate request: %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False

        spider.crawler.stats.inc_value('dupefilter/filtered', spider=spider)



