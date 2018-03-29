# -*- coding: utf-8 -*-
import scrapy
import redis
from bangumi.items import BangumiItem


class DmhySpider(scrapy.Spider):
    name = 'dmhy'
    allowed_domains = ['share.dmhy.org']

    def start_requests(self):
        # local test without redis
        # with open('test/urls.txt', 'r') as urls:
        #     for url in urls:
        #         yield scrapy.Request(url, callback=self.parse)

        # with redis
        redis_host = self.settings.get('REDIS_HOST')
        redis_port = self.settings.getint('REDIS_PORT')
        redis_db = self.settings.getint('REDIS_DB', 0)
        bangumi_sub_list = self.settings.get('REDIS_BANGUMI_SUB_LIST_KEY')

        self.logger.info('redis_host[%s] redis_port[%s] redis_db[%s] bangumi_sub_list[%s]', redis_host, redis_port, redis_db, bangumi_sub_list)

        conn = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
        url_list = conn.lrange(bangumi_sub_list, 0, -1)
        for url in url_list:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        # self.logger.debug('response.body[%s]', response.body)

        item_node_list = response.xpath("/rss/channel/item")
        for item_node in item_node_list:
            item = BangumiItem()
            title = item_node.xpath('title/text()').extract()[0]
            detail_url = item_node.xpath('link/text()').extract()[0]
            magnet_url = item_node.xpath('enclosure/@url').extract()[0]

            # self.logger.debug('title[%s] detail_url[%s] magnet_url[%s]', title, detail_url, magnet_url)

            item['title'] = title
            item['detail_url'] = detail_url
            item['magnet_url'] = magnet_url
            item['parent_url'] = response.url

            yield item
