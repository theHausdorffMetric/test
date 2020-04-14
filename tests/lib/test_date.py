# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime, timedelta
from unittest import TestCase
from unittest.mock import patch

from kp_scrapers.lib.date import (
    create_str_from_time,
    may_parse_date_str,
    str_month_day_time_to_datetime,
)
from tests._helpers.date import DateTimeWithChosenNow


first_of_jan = datetime(2016, 1, 1, 0, 0, 0)
first_of_jan_ten_oclock = datetime(2016, 1, 1, 10, 0, 0)


class DateUtilsTestCase(TestCase):
    def test_create_date_with_default_format(self):
        whatever_date = datetime(2016, 10, 1, 16, 23, 4)
        str_date = create_str_from_time(whatever_date)
        self.assertEqual(str_date, '2016-10-01 16:23:04')

    def test_create_date_with_custom_format(self):
        whatever_date = datetime(2016, 10, 1)
        some_fmt = '%Y-%m-%d'
        str_date = create_str_from_time(whatever_date, some_fmt)
        self.assertEqual(str_date, '2016-10-01')

    def test_create_date_str_with_default_format(self):
        whatever_date = '2016-10-01 16:23:04'
        obj_date = may_parse_date_str(whatever_date)
        self.assertEqual(obj_date, datetime(2016, 10, 1, 16, 23, 4))

    def test_create_date_str_with_custom_format(self):
        whatever_date = '2016-10-01'
        obj_date = may_parse_date_str(whatever_date, '%Y-%m-%d')
        self.assertEqual(obj_date, datetime(2016, 10, 1))


class MonthDayTimeDateTestCase(TestCase):
    def tearDown(self):
        DateTimeWithChosenNow.chosen_now = None

    @patch('kp_scrapers.lib.date.dt.datetime', new=DateTimeWithChosenNow)
    def test_month_day_date(self):
        DateTimeWithChosenNow.chosen_now = first_of_jan_ten_oclock
        the_datetime = str_month_day_time_to_datetime('01-01 10:00', max_ahead=timedelta(0))

        self.assertEqual(the_datetime, datetime(2016, 1, 1, 10, 0, 0).isoformat())

    @patch('kp_scrapers.lib.date.dt.datetime', new=DateTimeWithChosenNow)
    def test_month_day_date_returns_none_when_input_not_valid(self):
        DateTimeWithChosenNow.chosen_now = first_of_jan_ten_oclock
        # Intput is invalid (- not : as hour/minute separator)
        the_datetime = str_month_day_time_to_datetime('01-01 10-00', max_ahead=timedelta(0))

        self.assertIsNone(the_datetime)

    @patch('kp_scrapers.lib.date.dt.datetime', new=DateTimeWithChosenNow)
    def test_month_day_date_when_late_and_no_tolerance(self):
        DateTimeWithChosenNow.chosen_now = first_of_jan
        the_datetime = str_month_day_time_to_datetime('12-31 10:00', max_ahead=timedelta(0))

        # Without proper tolerance weird results arise
        self.assertIsNone(the_datetime)

    @patch('kp_scrapers.lib.date.dt.datetime', new=DateTimeWithChosenNow)
    def test_month_day_time_when_late_and_60days_ahead_and_36_hours_lag_tolerance(self):
        DateTimeWithChosenNow.chosen_now = datetime(2016, 1, 1, 10, 0, 0)

        the_datetime = str_month_day_time_to_datetime(
            '12-31 10:00', max_ahead=timedelta(days=60), max_lag=timedelta(hours=36)
        )

        # Without proper tolerance weird results arise
        self.assertEqual(the_datetime, datetime(2015, 12, 31, 10, 0, 0).isoformat())

    @patch('kp_scrapers.lib.date.dt.datetime', new=DateTimeWithChosenNow)
    def test_month_day_time_when_late_and_60days_ahead_and_24_hours_lag_tolerance(self):
        DateTimeWithChosenNow.chosen_now = datetime(2016, 1, 1, 10, 0, 0)

        # Here we lag by exactly 24 hours.
        the_datetime = str_month_day_time_to_datetime(
            '12-31 10:00', max_ahead=timedelta(days=60), max_lag=timedelta(hours=24)
        )

        # Without proper tolerance weird results arise
        self.assertEqual(the_datetime, datetime(2015, 12, 31, 10, 0, 0).isoformat())

    @patch('kp_scrapers.lib.date.dt.datetime', new=DateTimeWithChosenNow)
    def test_month_day_time_when_too_late_and_60d_ahead_and_24h_lag_tolerance(self):
        DateTimeWithChosenNow.chosen_now = datetime(2016, 1, 1, 10, 1, 0)
        # Here we lag by 24 hours and 1min.
        the_datetime = str_month_day_time_to_datetime(
            '12-31 10:00', max_ahead=timedelta(days=60), max_lag=timedelta(hours=24)
        )

        self.assertIsNone(the_datetime)

    @patch('kp_scrapers.lib.date.dt.datetime', new=DateTimeWithChosenNow)
    def test_month_day_time_when_too_late_and_60d_ahead_and_24h_lag_tolerance2(self):
        DateTimeWithChosenNow.chosen_now = first_of_jan_ten_oclock
        # Here we lag by 24 hours and 1min (because of the ETA)
        the_datetime = str_month_day_time_to_datetime(
            '12-31 09:59', max_ahead=timedelta(days=60), max_lag=timedelta(hours=24)
        )

        self.assertIsNone(the_datetime)
