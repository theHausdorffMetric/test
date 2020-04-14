# -*- coding: utf-8 -*-

"""S3 raw export pipeline.

Contains pipeline class for exporting items as .jl files to an S3 backend.

"""

from __future__ import absolute_import
import datetime as dt
import logging
import os
from urllib.parse import urlparse

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.exporters import JsonLinesItemExporter
from scrapy.extensions.feedexport import S3FeedStorage

from kp_scrapers.lib.date import system_tz_offset


# get rid of verbose third-party loggers
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('boto3').setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)


ITEMS_BUCKET = 'kp-datalake'


class S3RawStorage(object):
    """Store items as JSON lines on S3.

    Currently almost equivalent to the default scrapy built-in S3 exporter.
    It just brings full control over the process and start leveraging it with
    custom Scrapy metrics. It helps us distinguish items scraped and data
    actually stored.

    """

    STATS_TPL = 'pipeline/storage/{metric}'

    def __init__(self, stats):
        self.stats = stats

    @staticmethod
    def _validate_settings(settings):
        """Disable pipeline if criteria are not met."""
        feed_uri_tpl = settings.get('KP_RAW_FEED_URI')

        if not feed_uri_tpl:
            raise NotConfigured('no feed uri defined')

        builtin_feed_uri_tpl = settings.get('FEED_URI')
        if builtin_feed_uri_tpl:
            raise NotConfigured('conflict: built-in Scrapy feed exporter is already configured')

    @classmethod
    def _namespace(cls, metric):
        """Namespace metrics to distinguish them in Scrapy stats.

        Examples:
            >>> S3RawStorage._namespace('foo')
            'pipeline/storage/foo'

        """
        return cls.STATS_TPL.format(metric=metric)

    @classmethod
    def from_crawler(cls, crawler):
        cls._validate_settings(crawler.settings)
        pipeline = cls(crawler.stats)

        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)

        return pipeline

    @staticmethod
    def feed_uri(spider):
        """Generate item storage URI.

        Args:
            spider (scrapy.Spider):

        Returns:
            str: S3 object key to which data should be uploaded to

        """
        # use spider finish_time as default, else UTC time
        _spider_finish = spider.crawler.stats._stats.get('finish_time')
        if _spider_finish:
            _time = _spider_finish - dt.timedelta(hours=system_tz_offset())
        else:
            _time = dt.datetime.utcnow()

        uri_opts = {
            'name': spider.name,
            'time': _time.isoformat(),
            'job_id': spider.job_id,
            'bucket': ITEMS_BUCKET,
            # use the same semantic as on the ETL
            # default env is a safe playground where we can dump whatever items
            # we want without taking the risk of polluting production
            # environments
            # con: actual production env MUST specify this setting
            # pro: new environment will have by default somewhere to upload
            # items, withput displaying an error because an env-dependant
            # bucket was not created.
            #
            # we allow fallback on the env since alternative runtimes like EC2 dont
            # benefit from scrapinghub settings interface.
            'env': spider.settings.get('KP_ENV', os.getenv('KP_ENV', 'pre-production')),
        }

        return spider.settings.get('KP_RAW_FEED_URI') % uri_opts

    def spider_opened(self, spider):
        self.stats.set_value(self._namespace('backend'), 'rawS3')
        # spider finish time only available when `spider_closed`
        # uri used here only as a filler to fulfil feed storage contract
        self.storage = S3FeedStorage(
            uri=f's3://{ITEMS_BUCKET}',
            access_key=spider.settings['AWS_ACCESS_KEY_ID'],
            secret_key=spider.settings['AWS_SECRET_ACCESS_KEY'],
        )

        self.raw_content = self.storage.open(spider)
        self.exporter = JsonLinesItemExporter(self.raw_content)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        # push items to json lines feed
        self.exporter.finish_exporting()

        # update object key to use job finish time
        uri = urlparse(self.feed_uri(spider))
        self.storage.keyname = uri.path[1:]  # remove first "/"
        logger.debug(f"Data will be uploaded to `{self.storage.keyname}`")

        # push items to S3
        self.raw_content.file.seek(0)
        if len(self.raw_content.file.read()) != 0:
            self.storage.store(self.raw_content)
        else:
            logger.info("No items are scrapped, not pushing to s3")

    def process_item(self, item, spider):
        self.stats.inc_value(self._namespace('items_stored'))
        self.exporter.export_item(item)

        # running jobs on scrapinghub will still store them
        # in their database. The point of this pipeline is
        # obviously to stop relying on it but that way it
        # remains a cheap fallback/backup
        return item
