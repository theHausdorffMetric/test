import logging

from scrapy import signals
from scrapy.exceptions import NotConfigured
from sqlalchemy.exc import OperationalError

from kp_scrapers.lib.services.kp_redshift import upsert_metrics
from kp_scrapers.settings.utils import determine_env


logger = logging.getLogger(__name__)


class RedshiftStats(object):
    """Upload stats in reporter.py to redshift"""

    def __init__(self, stats):
        self.stats = stats

    @staticmethod
    def _validate_settings(spider):
        """Disable pipeline if criteria are not met."""

        if determine_env() != 'production':
            logging.info('Skipping spider stats monitoring upload')
            raise NotConfigured('environment is not production')

    @classmethod
    def from_crawler(cls, crawler):
        cls._validate_settings(crawler.settings)
        pipeline = cls(crawler.stats)

        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)

        return pipeline

    def spider_opened(self, spider):
        pass

    def spider_closed(self, spider):
        item_args = self.get_extra_crawler_stats(spider.crawler.stats.get_stats())
        item_args.update(
            sh_spider_name=spider.crawler.spider.name,
            sh_job_id=spider.crawler.spider.job_name,
            datatype=spider.crawler.spider.produces,
        )

        if not item_args.get('spider_attribute_stats') or (
            item_args.get('total_items') == 0
            and item_args.get('error_count') == 0
            and item_args.get('exception_count') == 0
        ):
            # do not upload if spider run is uneventful, i.e no new emails
            logging.info('Uneventful spider run, skipping redshift upload')
            return

        logging.info('Uploading spider stats to Redshift...')
        try:
            upsert_metrics(item_args)
            logging.info('Redshift monitoring pipeline successfully ran')
        except OperationalError as exc:
            # catch operational errors so as not to hinder other pipelines
            logger.exception(
                'Failed to run: %(exception)s', {'exception': exc},
            )

    @staticmethod
    def get_extra_crawler_stats(stat_dict):
        """
        Get extra information in addtion to spider attributes like error,
        warning and exception messages
        """
        # exception might not be in dict
        exception_count = 0
        for _k, _v in stat_dict.items():
            if 'spider_exceptions' in _k:
                exception_count = 1
                break

        return {
            'warning_count': stat_dict.get('log_count/WARNING', 0),
            'error_count': stat_dict.get('log_count/ERROR', 0),
            'exception_count': exception_count,
            'start_time': stat_dict.get('start_time'),
            'finish_time': stat_dict.get('finish_time'),
            'total_items': stat_dict.get('item_scraped_count', 0),
            'spider_attribute_stats': stat_dict.get('spider_attribute_stats'),
        }
