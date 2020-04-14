# -*- coding: utf-8 -*-

"""Google Drive raw export pipeline.

Contains pipeline class for exporting items as Google Sheets files (CSV format)
to a Drive backend.

"""

from __future__ import absolute_import, unicode_literals
import logging
import os

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.exporters import CsvItemExporter

from kp_scrapers.models.base import strip_meta_fields
from kp_scrapers.pipelines.backends.drive import DriveBackend


logger = logging.getLogger(__name__)


class DriveStorage(object):
    """Store items as CSV on Google Sheets.

    This class is merely copy-pasted from `S3RawStorage` since it works well enough,
    and we want to maintain a consistent API for better clarity.

    """

    STATS_TPL = 'pipeline/drive/{metric}'

    def __init__(self, stats):
        self.stats = stats
        self.include_meta = False

    @staticmethod
    def _validate_settings(settings):
        """Disable pipeline if no folder provided.
        """
        # cast setting to string since SHUB assumes all custom settings as strings
        if str(settings.get('KP_DRIVE_ENABLED')) != 'True':
            raise NotConfigured('`KP_DRIVE_ENABLED` is not `True`')
        if settings.get('KP_DRIVE_CUSTOM_EXPORT'):
            logger.debug(f"Exporting custom data from `cls.{settings.get('KP_DRIVE_CUSTOM_DATA')}`")
        if not settings.get('KP_DRIVE_FEED_URI'):
            raise NotConfigured('`KP_DRIVE_FEED_URI` is not set')

    @classmethod
    def _namespace(cls, metric):
        """Namespace metrics to distinguish them in Scrapy stats.

        Examples:
            >>> DriveStorage._namespace('foo')
            'pipeline/drive/foo'

        """
        return cls.STATS_TPL.format(metric=metric)

    @classmethod
    def from_crawler(cls, crawler):
        cls._validate_settings(crawler.settings)
        pipeline = cls(crawler.stats)

        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)

        return pipeline

    def spider_opened(self, spider):
        self.stats.set_value(self._namespace('backend'), 'rawDrive')
        self.storage = DriveBackend(spider.settings.get('KP_DRIVE_FEED_URI'), spider)
        self.include_meta = str(spider.settings.get('KP_DRIVE_INCLUDE_META')) == 'True'
        self.sheet_id = spider.settings.get('KP_DRIVE_SHEET_ID')

        self.raw_content = self.storage.open(spider)
        # NOTE exporter does not handle nested JSONs, instead returning its representation
        # TODO inherit from CsvItemExporter and handle nested JSONs as a custom class
        self.exporter = CsvItemExporter(self.raw_content)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        # push arbitrary data to drive storage if `KP_DRIVE_CUSTOM_DATA` is enabled
        if spider.settings.get('KP_DRIVE_CUSTOM_EXPORT'):
            custom_data = getattr(spider, spider.settings['KP_DRIVE_CUSTOM_EXPORT'], [])
            # we assume default is an iterable
            if isinstance(custom_data, dict):
                custom_data = custom_data.values()

            for item in custom_data:
                self.stats.inc_value(self._namespace('items_stored'))
                self.exporter.export_item(item)

        logger.debug('exporting items to drive storage')
        self.exporter.finish_exporting()

        # push items to Google Drive and store url for later retrieval
        sheet_id = self.storage.store(self.raw_content, sheet_id=self.sheet_id)

        # bind items url so spider or extensions can eventually make use of it
        spider.job_items_url = 'https://docs.google.com/spreadsheets/d/{}'.format(sheet_id)
        logger.debug('items will be available at `{}`'.format(spider.job_items_url))

        # remove temporary client credentials
        os.remove(os.path.join(os.getcwd(), 'client_secret.json'))
        os.remove(os.path.join(os.getcwd(), 'auth_token.json'))

    def process_item(self, item, spider):
        # NOTE `CsvItemExporter` expects bytes, not unicodes
        # NOTE if custom data exporting is enabled, don't export yielded items automatically
        if not spider.settings.get('KP_DRIVE_CUSTOM_EXPORT'):
            self.stats.inc_value(self._namespace('items_stored'))
            self.exporter.export_item(item if self.include_meta else strip_meta_fields(item))

        # running jobs on scrapinghub will still store them
        # in their database. The point of this pipeline is
        # to allow analysts an easy interface to view and edit
        # data if needed
        return item
