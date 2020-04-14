# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

from scrapy.exceptions import NotConfigured


class ProxyMiddleware(object):
    """Proxy request through a proxy.

    Settings:
        HTTP_PROXY(str): host to target
        HTTP_PROXY_ENABLED(bool): switch to activate the middleware

    """

    def __init__(self, proxy):
        self.proxy = proxy

    @classmethod
    def from_crawler(cls, crawler):
        proxy = crawler.settings.get('HTTP_PROXY')
        is_enabled = crawler.settings.get('HTTP_PROXY_ENABLED')

        # TODO try to fallback on USER_AGENT
        if not proxy or not is_enabled:
            raise NotConfigured("`HTTP_PROXY` not set or disabled")

        return cls(proxy)

    def process_request(self, request, spider):
        request.meta['proxy'] = self.proxy
        # we don't return anything so all other processing steps continue as is
