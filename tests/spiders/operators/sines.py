# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
import datetime as dt
import unittest

from dateutil.relativedelta import relativedelta

from kp_scrapers.spiders.operators.sines import (
    format_tr,
    get_full_months,
    get_month_end,
    is_wanted_date,
    true_end,
)


class SinesOperatorTestCase(unittest.TestCase):
    def test_is_wanted_date(self):
        start_date = dt.date(year=2016, month=6, day=6)
        end_date = dt.date(year=2017, month=3, day=3)
        item_date = dt.date(day=1, month=1, year=2017)
        self.assertEqual(is_wanted_date(item_date, start_date, end_date), True)
        item_date = dt.date(year=2017, month=4, day=4)
        self.assertEqual(is_wanted_date(item_date, start_date, end_date), False)
        item_date = dt.date(year=2016, month=2, day=1)
        self.assertEqual(is_wanted_date(item_date, start_date, end_date), False)

    def test_true_end(self):
        end_date = dt.date(year=2017, month=3, day=2)
        true_end_date = dt.date(year=2017, month=3, day=31)
        self.assertEqual(true_end(end_date), true_end_date)
        end_date = dt.date(year=2500, month=3, day=2)
        today = dt.datetime.today()
        true_end_date = (
            dt.date(year=today.year, month=today.month, day=1)
            + relativedelta(months=1)
            - relativedelta(days=1)
        )
        self.assertEqual(true_end(end_date), true_end_date)

    def test_get_full_months(self):
        start_date = dt.date(year=2016, month=12, day=2)
        end_date = dt.date(year=2017, month=2, day=2)
        months = [
            (dt.date(year=2016, month=12, day=1), dt.date(year=2016, month=12, day=31)),
            (dt.date(year=2017, month=1, day=1), dt.date(year=2017, month=1, day=31)),
            (dt.date(year=2017, month=2, day=1), dt.date(year=2017, month=2, day=28)),
        ]
        self.assertListEqual(
            get_full_months(start_date, end_date), [(str(m[0]), str(m[1])) for m in months]
        )

    def test_format_tr(self):
        tr = [
            u'\r\n\t\t\t',
            u'01-04-2017',
            u'1131420,891',
            u'0',
            u'49480,046',
            u'1503,943',
            u'0',
            u'50983,9890',
            u'0',
            u'432,901',
            u'1080004,001',
            u'\r\n\t\t',
        ]
        processed_list = [
            u'01-04-2017',
            u'1131420,891',
            u'0',
            u'49480,046',
            u'1503,943',
            u'0',
            u'50983,9890',
            u'0',
            u'432,901',
            u'1080004,001',
        ]
        self.assertListEqual(format_tr(tr), processed_list)

    def test_get_month_end(self):
        self.assertEqual(
            get_month_end(dt.date(year=2017, month=1, day=1)), dt.date(year=2017, month=1, day=31)
        )
