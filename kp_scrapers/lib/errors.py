# -*- coding: utf-8 -*-

from __future__ import unicode_literals


class InvalidSettings(Exception):
    """Syntax sugar and standard for clearer settings exception."""

    MESSAGE = 'mandatory fields were not set in Scrapy `settings`: {}'

    def __init__(self, *mandatory_fields):
        clean_fields = ', '.join(mandatory_fields)
        Exception.__init__(self, self.MESSAGE.format(clean_fields))


class InvalidCliRun(Exception):
    """Syntax sugar and standard for clearer job launch exception."""

    MESSAGE = 'bad arguments while running spider: {}={}'

    def __init__(self, key, value):
        Exception.__init__(self, self.MESSAGE.format(key, value))


class SpiderShouldNotRun(Exception):
    """Stop spiders from running."""

    MESSAGE = 'spider {} is not supposed to run: {}'

    def __init__(self, spider_name, reason):
        Exception.__init__(self, self.MESSAGE.format(spider_name, reason))


def break_contract(name, signature):
    tpl = 'Spider class {} does not implement the `{}` method as it should'
    raise NotImplementedError(tpl.format(name, signature))
