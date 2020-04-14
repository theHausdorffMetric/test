# -*- coding: utf-8; -*-

"""This module provides the :class:`.PersistSpider` class, which offers
an standard mecanism for implementing stateful spiders. That is spiders
that need to retain some state in between runs.

This implementation relies on the DotScrapy extension.

"""

from __future__ import absolute_import, unicode_literals
from datetime import datetime

from dateutil.parser import parse
from scrapy import signals
from scrapy.spiders import Spider

from kp_scrapers.spiders.bases.persist_data_manager import PersistDataManager


class PersistSpider(Spider):
    """Spider that can persist data accross job execution

    It also offers a mecanism to specify a start_date which is usually used in
    the spider to filter items and only extract items with a date > start_date.
    For example for Operators data which are per day, it allow to limit the
    number of rows to check.

    lag is the number of days we want to look in the past (because some
    provider modifies data in the past) lag is defined by precedence:

    - setting <SPIDER_NAME>_LAG_DAYS
    - setting DEFAULT_LAG_DAYS
    - 10

    start_date is defined by precedence:

    - spider parameter start_data
    - last spider execution datetime - lag
    - 2011-01-01
    """

    def spider_closed(self, spider):
        """Handler for the ``spider_closed`` signal

        Note:
            the assertion ``self is spider`` should always be ``True``

        Args:
            spider (PersistSpider):

        Returns:
            None: signal handlers don't return anything
        """
        if spider is not self:
            return
        self.persisted_data['spider_exec'] = str(datetime.today().replace(microsecond=0))
        self.persisted_data.save()

    def spider_opened(self, spider):
        """Handler for the ``spider_opened`` signal

        Note:
            the assertion ``self is spider`` should always be ``True``

        Args:
            spider (PersistSpider): the opened spider

        Returns:
            None: signal handlers don't return anything
        """
        if spider is not self:
            return

        # Find a suitable min date for row to extract if no start_date provided at init
        # cannot be done in init because settings are not available at that time
        if not self.start_date:
            # Reload lag days data in the past, to account for modification in sources
            default_lag = self.settings.getint('DEFAULT_LAG_DAYS', 10)
            lag = self.settings.getint(self.name.upper() + '_LAG_DAYS', default_lag)
            self.logger.info('Lag to take for start date: {}'.format(lag))

            lastexec_datetime = self.persisted_data.get_last_spider_exec(day_diff=lag)
            self.logger.info('Calculated start date: {}'.format(lastexec_datetime))

            # Default start is nothing else
            first_date = parse('2011-01-01')

            self.start_date = max(d for d in [first_date, lastexec_datetime] if d is not None)

            # we record start time since a spider can run for a long time
            # we use `str` here for consistence with `spider_exec` key but
            # `isoformat()` could go a long way
            self.persisted_data["spider_start"] = str(self.start_date)

        self.logger.info("Start date for data extraction: {}".format(self.start_date))

    def __init__(self, start_date=None, *args, **kwargs):
        # Use spider name as filename
        self.persisted_data = PersistDataManager(kwargs.get('state_file') or self.name)
        self.logger.debug("Data from last execution: {}".format(self.persisted_data))

        self.today = datetime.today()
        if start_date:
            self.start_date = parse(start_date)
            self.logger.info('Start date provided as a parameter: {}'.format(start_date))
        else:
            self.start_date = None

        # Parameter from the spider to delete file
        delete_file = kwargs.pop('delete_files', False)
        if delete_file:
            self.persisted_data.clean_file()

        super(PersistSpider, self).__init__(*args, **kwargs)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Factory methods for spider classes that inherit from :class:`.PersistSpider`

        Args:
            crawler (scrapy.crawler.Crawler): a crawler provider by the framework

        Returns:
            .PersistSpider: a subclass of PersistSpider.
        """
        spider = super(PersistSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        crawler.signals.connect(spider.spider_opened, signals.spider_opened)
        return spider
