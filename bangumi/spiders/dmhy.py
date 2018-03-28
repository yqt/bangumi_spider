# -*- coding: utf-8 -*-
import scrapy
from bangumi.items import BangumiItem


class DmhySpider(scrapy.Spider):
    name = 'dmhy'
    allowed_domains = ['share.dmhy.org']

    def start_requests(self):
        # self.logger.debug('Existing settings: %s', self.settings.attributes.keys())
        # self.logger.debug('transmission_cfg_base_uri[%s]', self.settings.get('TRANSMISSION_BASE_URI'))

        # local test without redis
        with open('test/urls.txt', 'r') as urls:
            for url in urls:
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
