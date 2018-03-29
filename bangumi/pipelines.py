# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import re
import json
import redis
from scrapy.exceptions import DropItem
from bangumi.transmission_rpc_client import TransmissionRpcClient


class BasePipepline(object):

    def json_loads_byteified(self, json_text):
        return self._byteify(
            json.loads(json_text, object_hook=self._byteify),
            ignore_dicts=True
        )

    def _byteify(self, data, ignore_dicts=False):
        # if this is a unicode string, return its string representation
        if isinstance(data, unicode):
            return data.encode('utf-8')
        # if this is a list of values, return list of byteified values
        if isinstance(data, list):
            return [self._byteify(item, ignore_dicts=True) for item in data]
        # if this is a dictionary, return dictionary of byteified keys and values
        # but only if we haven't already byteified it
        if isinstance(data, dict) and not ignore_dicts:
            return {
                self._byteify(key, ignore_dicts=True): self._byteify(value, ignore_dicts=True)
                for key, value in data.iteritems()
            }
        # if it's anything else, return it in its original form
        return data


class BangumiPipeline(BasePipepline):

    def __init__(self, redis_host, redis_port, redis_db):
        super(BangumiPipeline, self).__init__()
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            redis_host=crawler.settings.get('REDIS_HOST'),
            redis_port=crawler.settings.getint('REDIS_PORT'),
            redis_db=crawler.settings.getint('REDIS_DB', 0)
        )

    def open_spider(self, spider):
        self.pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        self.conn = redis.Redis(connection_pool=self.pool)

    def process_item(self, item, spider):
        # spider.logger.debug('BangumiPipeline item: title[%s] detail_url[%s] magnet_url[%s]', item['title'], item['detail_url'], item['magnet_url'])
        succ, episode, revision, resolution = self.extract_episode_info(item['title'])
        if not succ:
            raise DropItem('get episode info form [%s] failed' % (item['title']))

        item['episode'] = episode
        item['revision'] = revision
        item['resolution'] = resolution

        # spider.logger.debug('BangumiPipeline item[%s]', json.dumps(dict(item), ensure_ascii=False))

        existed_episide_info_str = self.conn.hget(item['parent_url'], episode)
        if existed_episide_info_str:
            existed_episide_info = self.json_loads_byteified(existed_episide_info_str)
            if existed_episide_info.get('revision') > revision:
                raise DropItem('episode[%s] is already existed' % (item['title']))
            elif existed_episide_info['revision'] == revision:
                existed_resolution = existed_episide_info.get('resolution')
                existed_resolution = int(existed_resolution) if existed_resolution else 0
                resolution = int(resolution) if resolution else 0
                if existed_resolution >= resolution:
                    raise DropItem('episode[%s] is already existed. no resolution advance.' % (item['title']))

        self.conn.hset(item['parent_url'], episode, json.dumps(dict(item), ensure_ascii=False))

        return item

    def extract_episode_info(self, title):
        m1 = re.search(r'[ \[](\d{2}\W??(?:\d{2})?)[ \]]? ?\[?(v\d{1,2})?[ \]]', title)
        if not m1 or len(m1.groups()) < 1:
            return False, None, None, None

        groups = m1.groups()
        episode = groups[0]
        revision = groups[1]
        resolution = None

        if not revision:
            m2 = re.search(r' ?(v\d{1,2}) ?', title)
            if m2 and len(m2.groups()) > 0:
                revision = m2.groups()[0]

        m3 = re.search(r'(?:[ \[]?(\d{3,4})p[ \]]?)|(?:[ \[]?\d{3,4}x(\d{3,4})[ \]]?)', title, re.I)
        if m3 and len(m3.groups()) > 0:
            resolution = m3.groups()[0] if m3.groups()[0] else m3.groups()[1]

        return True, episode, revision, resolution


class TransmissionPipeline(BasePipepline):

    def __init__(self, base_uri, user, passwd, download_dir):
        super(TransmissionPipeline, self).__init__()
        self.base_uri = base_uri
        self.user = user
        self.passwd = passwd
        self.download_dir = download_dir
        self.item_list = []

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            base_uri=crawler.settings.get('TRANSMISSION_BASE_URI'),
            user=crawler.settings.get('TRANSMISSION_USER'),
            passwd=crawler.settings.get('TRANSMISSION_PASSWD'),
            download_dir=crawler.settings.get('TRANSMISSION_DOWNLOAD_DIR')
        )

    def open_spider(self, spider):
        self.client = TransmissionRpcClient(
            base_uri=self.base_uri, user=self.user, passwd=self.passwd, download_dir=self.download_dir,
            logger=spider.logger
        )
        self.pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
        self.conn = redis.Redis(connection_pool=self.pool)

    def process_item(self, item, spider):
        spider.logger.debug('TransmissionPipeline item[%s]', json.dumps(dict(item), ensure_ascii=False))
        # NOTE: for filltering duplicated torrents
        self.item_list.append(item)

        return item

    def close_spider(self, spider):
        # NOTE: filter duplicated torrent with low resulotion
        item_map_to_add = {}
        for item in self.item_list:
            key = item['parent_url'] + item['episode']
            if key in item_map_to_add:
                existed_item = item_map_to_add[key]
                existed_resolution = existed_item.get('resolution')
                existed_resolution = int(existed_resolution) if existed_resolution else 0
                current_resolution = int(item['resolution']) if item['resolution'] else 0
                if existed_resolution >= current_resolution:
                    continue
            item_map_to_add[key] = item

        # spider.logger.debug('item_map_to_add[%s]', item_map_to_add)

        succeded_item = []
        failed_item = []
        for key, item in item_map_to_add.items():
            succ = self.client.add_torrent(item['magnet_url'])
            if not succ:
                spider.logger.error('add torrent[%s] failed', item['title'])
                failed_item.append(failed_item)
            else:
                succeded_item.append(succeded_item)

        # TODO: call tg bot webhook api
