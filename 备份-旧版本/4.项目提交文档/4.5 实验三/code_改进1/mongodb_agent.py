from pymongo import MongoClient
import pymongo
from pprint import pprint
import hashlib
import datetime

class MongoDBClient(object):
    def __init__(self, ip, port, dbname, collections_name):
        try:
            self.agent = MongoClient("mongodb://{0}:{1}/".format(ip, port))
        except Exception, e:
            print e
            raise

        if dbname is None:
            dbname = "default_dbxx"
        if collections_name is None:
            collections_name = "default_col"

        self.cur_db = self.agent[dbname]
        self.cur_collections = self.cur_db[collections_name]
        self.links_collection = self.cur_db['url_links']

        print "mongodb client initialized : {}:{}".format(ip, port)
        print "               --> ({}, {})".format(dbname, collections_name)

    def enqueue(self, url, req):
        url_info = {
            "_id": hashlib.md5(url).hexdigest(),
            "url": url,
            "req": req,
            "status": "new",
            "priority": 1,
            "enqueue_date": datetime.datetime.utcnow()
        }
        try:
            self.cur_collections.insert_one(url_info)
        except Exception,e:
            print e

    def dequeue(self):
        result = self.cur_collections.find_one_and_update(
            filter={'status': 'new'},
            update={"$set": {'status': 'downloading'}},
            sort=[('enqueue_date', pymongo.ASCENDING)])
        try:
            return result[u'req']
        except TypeError:
            return None

    def dequeue_by_pr(self):
        result = self.cur_collections.find_one_and_update(
            filter={'status': 'new'},
            update={'$set': {'status': 'downloading'}},
            sort=[('priority', pymongo.DESCENDING), ('enqueue_date', pymongo.ASCENDING)])
        try:
            return result[u'req']
        except TypeError:
            return None

    def set_finished(self, url):
        self.cur_collections.update_one(
            filter={"_id": hashlib.md5(url).hexdigest()},
            update={'$set': {'finished_date': datetime.datetime.utcnow(),
                             'status':"finished"}})
        return None

    def __contains__(self, url):
        url_hash = hashlib.md5(url).hexdigest()
        result = self.cur_collections.find_one(
                filter={"_id":url_hash})

        if result is None:
            print "{} has downloaded  <-- ".format(url)
            return False
        else:
            return True

    def set_link(self, url, target_url):
        info = {
            '_id': hashlib.md5(url + target_url).hexdigest(),
            'url': url,
            'target': target_url
        }
        try:
            self.links_collection.insert_one(info)
        except Exception,e:
            print "set link error --> {}".format(e)

    def get_all_links(self):
        for link in self.links_collection.find():
            yield (link[u'url'], link[u'target'])

    def __len__(self):
        try:
            return self.cur_collections.count(filter={"status":"new"})
        except Exception,e:
            print "an error occured when count the db collection  <-- wangyf"
            print "error --> {}".format(e)

    def clear(self):
        print "starting clear database..."
        self.cur_collections.drop()
        self.links_collection.drop()
        print "database cleared"

    def close(self):
        try:
            self.clear()
            return self.agent.close()
        except Exception,e:
            return False


if __name__ == "__main__":
    urls = ['http://www.mafengwo.cn', 'http://www.baidu.cn', 'http://sohu.cn']
    agent = MongoDBClient(ip="10.2.1.173", port='27008')

    agent.clear()

    for url in urls:
        agent.enqueue(url)
        for url_ in urls:
            agent.set_link(url, url_)

    for url in agent.cur_collections.find():
        pprint(url)
    for link in agent.get_all_links():
        pprint(link)
    while True:
        url = agent.dequeue()
        if url is None:
            break
        pprint(url)

    agent.clear()
