from __future__ import print_function
from hashlib import *
import os
import math
import logging

from scrapy.utils.job import job_dir
from scrapy.utils.request import request_fingerprint
from filters import Filters

#from pybloomfilter import BloomFilter


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

    def __init__(self, m_length, error_rate, path=None, debug=False):
        self.file = None
        self.logdupes = True
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        if path:
            filepath = os.path.join(path, 'requests.seen')
            if os.path.exists(filepath):
                self.fingerprints = Filters(m_length, error_rate)
                self.logger.info("created bloomfilter({},{}) from exist file {}"
                        .format(m_length, error_rate, filepath))
            else:
                self.fingerprints = Filters(m_length, error_rate)
                self.logger.info("created bloomfilter({},{}) from new file {}"
                        .format(m_length, error_rate, filepath))
        else:
            self.fingerprints = Filters(m_length, error_rate)
            self.logger.info("created bloomfilter({},{}) from anonymous file"
                    .format(m_length, error_rate))

    @classmethod
    def from_settings(cls, settings):
        debug = settings.getbool('DUPEFILTER_DEBUG')
        m_length = settings['MAX_LENGTH']
        error_rate = settings['ERROR_RATE']
        return cls(m_length, error_rate, job_dir(settings), debug)

    def request_seen(self, request):
        fp = self.request_fingerprint(request)
        if fp in self.fingerprints:
            return True
        self.fingerprints.add(fp)

    def request_fingerprint(self, request):
        return request_fingerprint(request)

    def close(self, reason):
        return

    def log(self, request, spider):
        if self.debug:
            msg = "BloomFilter filtered duplicate request: %(request)s"
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
        elif self.logdupes:
            msg = ("BloomFilter filtered duplicate request: %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False

        spider.crawler.stats.inc_value('dupefilter/filtered', spider=spider)


'''
class RFPDupeFilter(BaseDupeFilter):
    """Request Fingerprint duplicates filter"""

    def __init__(self, m_length, error_rate, path=None, debug=False):
        self.file = None
        self.logdupes = True
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        if path:
            filepath = os.path.join(path, 'requests.seen')
            if os.path.exists(filepath):
                self.fingerprints = BloomFilter.open(filepath)
                self.logger.info("created bloomfilter({},{}) from exist file {}"
                        .format(m_length, error_rate, filepath))
            else:
                self.fingerprints = BloomFilter(m_length, 0.0001, filepath)
                self.logger.info("created bloomfilter({},{}) from new file {}"
                        .format(m_length, error_rate, filepath))
        else:
            self.fingerprints = BloomFilter(m_length, 0.0001, None)
            self.logger.info("created bloomfilter({},{}) from anonymous file"
                    .format(m_length, error_rate))

    @classmethod
    def from_settings(cls, settings):
        debug = settings.getbool('DUPEFILTER_DEBUG')
        m_length = settings['MAX_LENGTH']
        error_rate = settings['ERROR_RATE']
        return cls(m_length, error_rate, job_dir(settings), debug)

    def request_seen(self, request):
        fp = self.request_fingerprint(request)
        if fp in self.fingerprints:
            return True
        self.fingerprints.add(fp)

    def request_fingerprint(self, request):
        return request_fingerprint(request)

    def close(self, reason):
        return

    def log(self, request, spider):
        if self.debug:
            msg = "BloomFilter filtered duplicate request: %(request)s"
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
        elif self.logdupes:
            msg = ("BloomFilter filtered duplicate request: %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False

        spider.crawler.stats.inc_value('dupefilter/filtered', spider=spider)
'''


