# -*- coding: utf-8 -*-
import json
import logging
import urllib2
import urlparse
import base64


class TgWebhookClient(object):
    def __init__(self, remote_uri, logger=None):
        self.remote_uri = remote_uri
        self.logger = logger if logger else logging.getLogger()

    def report_summary(self, latest_episode, succeded_item_list, failed_item_list):
        if len(succeded_item_list) == 0 and len(failed_item_list) == 0:
            return False

        sample_item = succeded_item_list[0] if len(succeded_item_list) > 0 else failed_item_list[0]
        keyword = self.get_keyword_from_url(sample_item['parent_url'])
        succeeded_count = len(succeded_item_list)
        failed_count = len(failed_item_list)

        request = urllib2.Request(self.remote_uri)

        data = {
            '__msg_type': 'item_scraped',
            'keyword': keyword,
            'latest_episode': latest_episode,
            'succeeded': succeeded_count,
            'failed': failed_count,
        }
        data_string = json.dumps(data, encoding='utf-8')

        try:
            response = urllib2.urlopen(request, data_string)
            if response.getcode() != 200:
                self.logger.error('request failed.')
                return False
            return True
        except Exception, error:
            self.logger.exception('request failed.')
            return False

    def get_keyword_from_url(self, url):
        return urlparse.parse_qs(urlparse.urlsplit(url).query).get('keyword', [''])[0]
