import datetime as dt
from unittest import TestCase
from unittest.mock import MagicMock, patch

from nose.tools import raises
from scrapy.exceptions import NotConfigured

from kp_scrapers.pipelines.s3 import S3RawStorage
from tests._helpers.date import DateTimeWithChosenNow


FIRST_OF_JAN_LOCAL = dt.datetime(2016, 1, 1, 0, 0, 0)


def _raw_settings():
    return {
        'KP_RAW_FEED_URI': (
            's3://%(bucket)s/%(env)s/%(name)s/stream/%(time)s--%(name)s--%(job_id)s.jl'
        )
    }


def _builtin_settings():
    return {'FEED_URI': 's3://foo/bar/%(name)s'}


class S3StorageTestCase(TestCase):
    def setUp(self):
        # TODO could be more generic and moved to `mocks` module
        self.spider = MagicMock()
        self.spider.name = 'FakeSpider'
        self.spider.project_id = 434
        self.spider.job_id = '3939846'
        self.spider.crawler.stats._stats = {'finish_time': FIRST_OF_JAN_LOCAL}
        self.spider.settings = _raw_settings()

    @raises(NotConfigured)
    def test_empty_settings_are_invalid(self):
        S3RawStorage(None)._validate_settings({})

    @raises(NotConfigured)
    def test_feed_uri_settings_is_deprecated(self):
        S3RawStorage(None)._validate_settings(_builtin_settings())

    @raises(NotConfigured)
    def test_invalid_settings_are_checked_at_from_crawler(self):
        self.spider.settings = _builtin_settings()
        S3RawStorage(None).from_crawler(self.spider)

    @patch('kp_scrapers.pipelines.s3.system_tz_offset', return_value=0)
    @patch('kp_scrapers.pipelines.s3.dt.datetime', new=DateTimeWithChosenNow)
    def test_pipeline_utc_time(self, mock_system_tz_offset):
        DateTimeWithChosenNow.chosen_now = FIRST_OF_JAN_LOCAL
        uri = S3RawStorage(None).feed_uri(self.spider)
        self.assertEqual(
            uri,
            (
                's3://kp-datalake/pre-production/FakeSpider/stream'
                '/2016-01-01T00:00:00--FakeSpider--3939846.jl'
            ),
        )

    @patch('kp_scrapers.pipelines.s3.system_tz_offset', return_value=8)
    @patch('kp_scrapers.pipelines.s3.dt.datetime', new=DateTimeWithChosenNow)
    def test_pipeline_singapore_time(self, mock_system_tz_offset):
        DateTimeWithChosenNow.chosen_now = FIRST_OF_JAN_LOCAL
        uri = S3RawStorage(None).feed_uri(self.spider)
        self.assertEqual(
            uri,
            (
                's3://kp-datalake/pre-production/FakeSpider/stream'
                '/2015-12-31T16:00:00--FakeSpider--3939846.jl'
            ),
        )

    @patch('kp_scrapers.pipelines.s3.system_tz_offset', return_value=-5)
    @patch('kp_scrapers.pipelines.s3.dt.datetime', new=DateTimeWithChosenNow)
    def test_pipeline_houston_time(self, mock_system_tz_offset):
        DateTimeWithChosenNow.chosen_now = FIRST_OF_JAN_LOCAL
        uri = S3RawStorage(None).feed_uri(self.spider)
        self.assertEqual(
            uri,
            (
                's3://kp-datalake/pre-production/FakeSpider/stream'
                '/2016-01-01T05:00:00--FakeSpider--3939846.jl'
            ),
        )
