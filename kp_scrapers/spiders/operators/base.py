# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime, timedelta

from scrapy import Spider
from scrapy.exceptions import CloseSpider


def format_date(raw_date, date_format=None):
    """Apply the given format or fallback on good ISO 8601.

    Example:
        >>> format_date(datetime(2017, 6, 23))
        '2017-06-23T00:00:00'
        >>> format_date(datetime(2017, 6, 23), '%Y-%m-%d')
        '2017-06-23'

    """
    # fallback by default on ISO 8601
    return raw_date.strftime(date_format) if date_format else raw_date.isoformat()


class BaseOperatorSpider(Spider):

    name = None
    start_date = None
    end_date = None

    # String using strftime directive
    # See https://docs.python.org/2/library/datetime.html#strftime-strptime-behavior
    # Use isoformat by default : YYYY-MM-DD
    date_format = None

    min_date = None  # datetime object, when data start to be available on the website
    lag_future = 0  # Day to look at in the future
    lag_past = 0  # Day to look at in the past

    @staticmethod
    def _parse_input_date(raw_date):
        """parse and validate user-given date.

        Example:
            >>> BaseOperatorSpider._parse_input_date('2017/06/23')# doctest:+IGNORE_EXCEPTION_DETAIL
            Traceback (most recent call last):
                ...
            CloseSpider
            >>> BaseOperatorSpider._parse_input_date('2017-06-23')
            datetime.datetime(2017, 6, 23, 0, 0)

        """
        try:
            return datetime.strptime(raw_date, '%Y-%m-%d')
        except ValueError as e:
            raise CloseSpider(str(e))

    @property
    def default_start_date(self):
        return datetime.today() - timedelta(days=self.lag_past)

    @property
    def default_end_date(self):
        return datetime.today() + timedelta(days=self.lag_future)

    def __init__(self, start_date=None, end_date=None, all=None, *args, **kwargs):
        if start_date is not None:
            self.start_date = self._parse_input_date(start_date)
        else:
            self.start_date = self.default_start_date

        if end_date is not None:
            self.end_date = self._parse_input_date(end_date)
        else:
            self.end_date = self.default_end_date

        # TODO `all` overwrites the built-in method, change it
        if all is not None:
            # NOTE don't we check for min_date not None ?
            self.start_date = self.min_date
            self.end_date = self.default_end_date

        self.start_date = format_date(self.start_date, self.date_format)
        self.end_date = format_date(self.end_date, self.date_format)
