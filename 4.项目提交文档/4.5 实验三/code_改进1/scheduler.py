import os
import json
import logging
from os.path import join, exists
from six.moves import cPickle as pickle

from scrapy.utils.reqser import request_to_dict, request_from_dict
from scrapy.utils.misc import load_object
from scrapy.utils.job import job_dir
from scrapy.mongodb_agent import MongoDBClient

logger = logging.getLogger(__name__)


class MongoDbQueue(MongoDBClient):
    """ act as a wrapper to provide api func"""
    def __init__(self, db_host, db_port, db_name, db_collections):
        self.mongo_agent = MongoDBClient(db_host, db_port, db_name, db_collections)
     
    def push(self, url, req):
        try:
            self.mongo_agent.enqueue(url,self._pickle_serialize(req))
            return True
        except Exception, e:
            logger.warning("{} can`t push into the queue".format(req.url))
            logger.warning("error info --> {}".format(e))
            return False

    def pop(self):
        req = self.mongo_agent.dequeue()
        req = self._pickle_deserialzie(req)
        return req
    
    def set_finished(self, request):
        self.mongo_agent.set_finished(request.url)

    def set_link(self, source, target):
        self.mongo_agent.set_link(source, target)
    
    def __len__(self):
        return len(self.mongo_agent)

    def _pickle_serialize(self, obj):
        try:
            return pickle.dumps(obj)
        except (pickle.PicklingError, AttributeError) as e:
            raise ValueError(str(e))

    def _pickle_deserialzie(self, req_str):
        if req_str is None:
            return None
        return pickle.loads(req_str.encode("utf-8"))


class Scheduler(object):

    def __init__(self, dupefilter, db_host, db_port, db_name=None, \
                 db_collections=None, jobdir=None, logunser=False, stats=None):
        self.df = dupefilter
        self.mongodb = MongoDbQueue(db_host, db_port, db_name, db_collections)
        self.logunser = logunser
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        print "initialize scheduler from crawler    <-- wangyf"
        settings = crawler.settings
        dupefilter_cls = load_object(settings['DUPEFILTER_CLASS'])
        dupefilter = dupefilter_cls.from_settings(settings)
        db_host = settings['DB_HOST']
        db_port = settings['DB_PORT']
        try:
            db_name = settings['DB_NAME']
            db_collections = settings['DB_COLLECTIONS_NAME']
        except Exception:
            db_name = None
            db_collections = None
        logunser = settings.getbool('LOG_UNSERIALIZABLE_REQUESTS', settings.getbool('SCHEDULER_DEBUG'))
        return cls(dupefilter, db_host, db_port, db_name, db_collections, \
                   jobdir=job_dir(settings), logunser=logunser, stats=crawler.stats)

    def has_pending_requests(self):
        return len(self) > 0

    def open(self, spider):
        print("initialize scheduler`s spider queues.   <-- wangyf")
        self.spider = spider
        return True

    def close(self, reason):
        logger.info("scheduler closed for {}".format(reason))
        self.mongodb.close()
        return True

    def enqueue_request(self, request):
        logger.info("enqueued request {}  <-- wangyf".format(request.url))
        self._set_link(request)
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return False
        self._push(request)
        self.stats.inc_value('scheduler/enqueued', spider=self.spider)
        return True

    def next_request(self):
        request = self._pop()
        if request:
            self.stats.inc_value('scheduler/dequeued', spider=self.spider)
        return request

    def set_finished(self, request):
        self.mongodb.set_finished(request)

    def __len__(self):
        return len(self.mongodb)

    def _push(self, req):
        url = req.url
        req = request_to_dict(req, self.spider)
        self.mongodb.push(url, req)

    def _pop(self):
        req_s = self.mongodb.pop()
        if req_s is None:
            return None
        return request_from_dict(req_s, self.spider)

    def _set_link(self, request):
        source = request.source_url
        if source is None:
            return
        target = request.url
        self.mongodb.set_link(source, target)


