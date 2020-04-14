# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock

from kp_scrapers.lib.pdf import ErosionPdfTable


class MyErosionPdfTable(ErosionPdfTable):
    def __init__(self, content):
        super(MyErosionPdfTable, self).__init__(content, '<string>', Mock())


class ErodeDateFromTheLeft(MyErosionPdfTable):
    _HEADER_STOP = []
    _START = 'left'
    _COLUMNS = [
        ('str', 'First column name'),
        ('date+%d-%m-%Y %H:%M', 'Second column name'),
        ('date*+%d-%m-%Y %H:%M', 'Third column name'),
    ]


class ErodeDateFromTheRight(MyErosionPdfTable):
    _HEADER_STOP = []
    _START = 'right'
    _COLUMNS = [
        ('str', 'First column name'),
        ('date+%d-%m-%Y %H:%M', 'Second column name'),
        ('date*+%d-%m-%Y %H:%M', 'Third column name'),
    ]


class ErodeEnumFromTheLeft(MyErosionPdfTable):
    _HEADER_STOP = []
    _START = 'left'
    _COLUMNS = [
        ('enum+SOME OTHER VALUE+VALUE+SOME VALUE', 'First column name'),
        ('str', 'Second column name'),
        ('enum*+SOME OTHER VALUE+VALUE+SOME VALUE', 'Third column name'),
    ]


class ErodeEnumFromTheRight(MyErosionPdfTable):
    _HEADER_STOP = []
    _START = 'right'
    _COLUMNS = [
        ('str', 'First column name'),
        ('enum+SOME OTHER VALUE+VALUE+SOME VALUE', 'Second column name'),
        ('enum*+SOME OTHER VALUE+VALUE+SOME VALUE', 'Third column name'),
    ]


class ErodeFloatFromTheLeft(MyErosionPdfTable):
    _HEADER_STOP = []
    _START = 'left'
    _COLUMNS = [
        ('float', 'First column name'),
        ('str', 'Second column name'),
        ('float*', 'Third column name'),
    ]


class ErodeFloatFromTheRight(MyErosionPdfTable):
    _HEADER_STOP = []
    _START = 'right'
    _COLUMNS = [
        ('str', 'First column name'),
        ('float', 'Second column name'),
        ('float*', 'Third column name'),
    ]


class ErodeIntFromTheLeft(MyErosionPdfTable):
    _HEADER_STOP = []
    _START = 'left'
    _COLUMNS = [
        ('int', 'First column name'),
        ('str', 'Second column name'),
        ('int*', 'Thrid column name'),
    ]


class ErodeIntFromTheRight(MyErosionPdfTable):
    _HEADER_STOP = []
    _START = 'right'
    _COLUMNS = [
        ('str', 'First column name'),
        ('int', 'Second column name'),
        ('int*', 'Thrid column name'),
    ]


class ErodeLineFromTheLeft(MyErosionPdfTable):
    _HEADER_STOP = []
    _COLUMNS = [
        ('str', 'First column name'),
        ('enum+SOME OTHER VALUE+VALUE+SOME VALUE', 'Second column name'),
        ('date+%Y', 'Third column name'),
    ]


class ErodeLineFromTheRight(MyErosionPdfTable):
    _HEADER_STOP = []
    _START = 'right'
    _COLUMNS = [
        ('str', 'First column name'),
        ('enum+SOME OTHER VALUE+VALUE+SOME VALUE', 'Second column name'),
        ('date+%Y%m%d%H%M', 'Third column name'),
    ]


class ErosionPdfTableTestCase(TestCase):
    def setUp(self):
        self._date_content_right = 'some content 01-01-2015 00:00'
        self._date_content_left = '01-01-2015 00:00 some content'
        self._enum_content_right = 'mdkm pzeork,cm , qmé poc, SOME VALUE'
        self._enum_content_left = 'SOME VALUE mdkm pzeork,cm , qmé poc,'
        self._line_content = 'some unquoted string of arbitrary length SOME VALUE 201501010000'
        self._float_content_left = '18,0000 mdkm pzeork,cm , qmé poc'
        self._float_content_right = 'mdkm pzeork,cm , qmé poc 18,0000'
        self._int_content_left = '100 mdkm pzeork,cm , qmé poc'
        self._int_content_right = 'mdkm pzeork,cm , qmé poc 100'
        self._string_content = 'no date, int or float on either side of this string'

    def test_parse_line_with_date_on_the_left(self):
        parser = ErodeDateFromTheLeft('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._date_content_left

        res, match = parser._parse_date(1)

        self.assertEqual(match, '01-01-2015 00:00')
        self.assertEqual(res, datetime(year=2015, month=1, day=1, hour=0, minute=0))

    def test_parse_line_with_date_on_the_right(self):
        parser = ErodeDateFromTheRight('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._date_content_right

        res, match = parser._parse_date(1)

        self.assertEqual(match, '01-01-2015 00:00')
        self.assertEqual(res, datetime(year=2015, month=1, day=1, hour=0, minute=0))

    def test_parse_optional_date_on_the_left(self):
        parser = ErodeDateFromTheLeft('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._string_content

        res, matched = parser._parse_date(2)

        self.assertEqual(matched, '')
        self.assertEqual(res, None)

    def test_parse_optional_date_on_the_Right(self):
        parser = ErodeDateFromTheRight('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._string_content

        res, matched = parser._parse_date(2)

        self.assertEqual(matched, '')
        self.assertEqual(res, None)

    def test_parse_line_with_enum_on_the_left(self):
        parser = ErodeEnumFromTheLeft(self._enum_content_left)
        parser._line_columns = parser._COLUMNS
        parser._line = self._enum_content_left

        res = parser._parse_enum(0)

        self.assertEqual(res, 'SOME VALUE')

    def test_parse_line_with_enum_on_the_right(self):
        parser = ErodeEnumFromTheRight('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._enum_content_right

        res = parser._parse_enum(1)

        self.assertEqual(res, 'VALUE')

    def test_parse_optional_enum_on_the_left(self):
        parser = ErodeEnumFromTheLeft('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._string_content

        res = parser._parse_enum(2)

        self.assertEqual(res, None)

    def test_parse_optional_enum_on_the_Right(self):
        parser = ErodeEnumFromTheRight('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._string_content

        res = parser._parse_enum(2)

        self.assertEqual(res, None)

    def test_parse_line_with_float_on_the_left(self):
        parser = ErodeFloatFromTheLeft('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._float_content_left

        res, matched = parser._parse_float(1)

        self.assertEqual(matched, '18,0000')
        self.assertEqual(res, 18.0)

    def test_parse_line_with_float_on_the_right(self):
        parser = ErodeFloatFromTheRight('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._float_content_right

        res, matched = parser._parse_float(1)

        self.assertEqual(matched, '18,0000')
        self.assertEqual(res, 18.0)

    def test_parse_optional_float_on_the_left(self):
        parser = ErodeFloatFromTheLeft('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._string_content

        res, matched = parser._parse_float(2)

        self.assertEqual(matched, '')
        self.assertEqual(res, None)

    def test_parse_optional_float_on_the_Right(self):
        parser = ErodeFloatFromTheRight('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._string_content

        res, matched = parser._parse_float(2)

        self.assertEqual(matched, '')
        self.assertEqual(res, None)

    def test_parse_line_with_integer_on_the_left(self):
        parser = ErodeIntFromTheLeft('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._int_content_left

        res, matched = parser._parse_int(1)

        self.assertEqual(matched, '100')
        self.assertEqual(res, 100)

    def test_parse_line_with_integer_on_the_right(self):
        parser = ErodeIntFromTheRight('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._int_content_right

        res, matched = parser._parse_int(1)

        self.assertEqual(matched, '100')
        self.assertEqual(res, 100)

    def test_parse_optional_integer_on_the_left(self):
        parser = ErodeIntFromTheLeft('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._string_content

        res, matched = parser._parse_int(2)

        self.assertEqual(matched, '')
        self.assertEqual(res, None)

    def test_parse_optional_integer_on_the_Right(self):
        parser = ErodeIntFromTheRight('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._string_content

        res, matched = parser._parse_int(2)

        self.assertEqual(matched, '')
        self.assertEqual(res, None)

    def test_parse_line_from_the_right(self):
        parser = ErodeLineFromTheRight('')
        parser._line_columns = parser._COLUMNS
        parser._line = self._line_content

        res = parser._parse_line()

        self.assertEqual(
            res,
            {
                'First column name': 'some unquoted string of arbitrary length SOME',
                'Second column name': 'VALUE',
                'Third column name': datetime(2015, 1, 1, 0, 0),
            },
        )

    def test_parse_line_from_the_left(self):
        parser = ErodeLineFromTheLeft(self._line_content)
        parser._line_columns = parser._COLUMNS
        parser._line = self._line_content

        res = parser._parse_line()

        self.assertEqual(
            res,
            {
                'First column name': 'some unquoted string of arbitrary length',
                'Second column name': 'SOME VALUE',
                'Third column name': datetime(2015, 1, 1, 0, 0),
            },
        )
