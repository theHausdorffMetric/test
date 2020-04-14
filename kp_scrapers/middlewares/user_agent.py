# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from random import choice

from scrapy.exceptions import NotConfigured


class RotateUserAgentMiddleware(object):
    """Rotate user-agent for each request.

    Given `USER_AGENT_LIST` setting was provisioned and
    `USER_AGENT_ROTATION_ENABLED` was set to true, the crawler will rotate its
    `User-Agent` header over them.

    Credit goes to https://gist.github.com/seagatesoft/e7de4e3878035726731d

    """

    def __init__(self, user_agents):
        self.user_agents = user_agents

    @classmethod
    def from_crawler(cls, crawler):
        user_agents = crawler.settings.get('USER_AGENT_LIST')
        is_enabled = crawler.settings.get('USER_AGENT_ROTATION_ENABLED')

        # TODO try to fallback on USER_AGENT
        if not user_agents or not is_enabled:
            raise NotConfigured("`USER_AGENT_LIST` not set or disabled")

        return cls(user_agents)

    def process_request(self, request, spider):
        request.headers.update(
            {
                'User-Agent': choice(self.user_agents),
                # in case crawlera is used, pass the header above
                'X-Crawlera-Profile': 'pass',
            }
        )
