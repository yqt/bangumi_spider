# -*- coding: utf-8 -*-
import json
import logging
import urllib2
import base64
import re


class TransmissionRpcClient(object):
    def __init__(self, base_uri, user, passwd, download_dir, logger=None):
        self.download_dir = download_dir
        self.base_uri = base_uri
        self.authorization = 'Basic %s' % base64.b64encode('%s:%s' % (user, passwd))

        self.logger = logger if logger else logging.getLogger()

        self.content_type = 'application/json'
        self.session_id = None

        self.init_session_id()

    def init_session_id(self):
        try:
            request = urllib2.Request(self.base_uri)
            request.add_header('Authorization', self.authorization)
            response = urllib2.urlopen(request)
        except urllib2.HTTPError, error:
            self.session_id = error.info().getheader('X-Transmission-Session-Id')
            self.logger.info('session_id[%s]' % (self.session_id))

    def get_request(self):
        request = urllib2.Request(self.base_uri)
        request.add_header('Authorization', self.authorization)
        request.add_header('X-Transmission-Session-Id', self.session_id)
        request.add_header('Content-Type', self.content_type)
        return request

    def add_torrent(self, magnet_link, paused=False):
        request = self.get_request()
        data = {
            'method': 'torrent-add',
            'arguments': {
                'paused': paused,
                'download-dir': self.download_dir,
                'filename': magnet_link
            }
        }
        data_string = json.dumps(data, encoding='utf-8')
        try:
            response = urllib2.urlopen(request, data_string)
            if response.getcode() != 200:
                self.logger.error('add torrnet of url[%s] failed.' % (magnet_link))
                return False
            response_text = response.read()
            result_data = json.loads(response_text, encoding='utf-8')
            if result_data.get('result', None) != 'success':
                self.logger.error('add torrnet of url[%s] failed. result[%s]' % (magnet_link, response_text))
                return False
            self.logger.info('add torrnet of url[%s] success' % (magnet_link))
            return True
        except urllib2.HTTPError, error:
            self.logger.error('add torrnet of url[%s] failed. code[%s]' % (magnet_link, error.code))
            return False
