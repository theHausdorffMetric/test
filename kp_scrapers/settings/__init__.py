# -*- coding: utf-8 -*-

"""Scrapy settings for spiders project.

For simplicity, this file contains only the most important settings by
default. All the other settings are documented here:

    http://doc.scrapy.org/en/latest/topics/settings.html

"""

from scrapy.dupefilters import RFPDupeFilter

# finishing by `extensions` as some of them rely on settings to decide if they
# should be activated or not
# make Scrapy thinkgs everything is in `kp_scrapers.settings` module
from kp_scrapers.settings.extensions import *  # noqa
from kp_scrapers.settings.network import *  # noqa
from kp_scrapers.settings.project import *  # noqa
from kp_scrapers.settings.services import *  # noqa
from kp_scrapers.settings.spiders import *  # noqa


class NoFilter(RFPDupeFilter):
    i = 0

    def request_fingerprint(self, _):
        self.i += 1
        return str(self.i)


try:
    # customize local runs
    # usage: put a `local_settings.py` file somewhere in your `PYTHONPATH`
    # (and ignore it in .gitignore if necessary)
    from local_settings import *  # noqa
except ImportError:
    pass
