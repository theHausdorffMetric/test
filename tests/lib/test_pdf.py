# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import unittest

from six.moves import range

from kp_scrapers.lib.pdf import _remove_first_on_the_left, _remove_first_on_the_right, PdfTable


text_table0 = """
header0  header1     header2    header3
toto0    toto1       toto2      toto3
tata0   tata1    tata2           tata3
"""

text_table1 = """
header0  header1     header2    header3
toto0    toto1       toto2      toto3
tata0          tata1     tata2         tata3
"""


class PdfTableTestCase(unittest.TestCase):
    def test_table0(self):
        table = PdfTable(text_table0[1:])
        self.assertEqual(table.columns, ['header0', 'header1', 'header2', 'header3'])
        self.assertEqual(
            list(table.parse()),
            [
                {'header{}'.format(i): 'toto{}'.format(i) for i in range(4)},
                {'header{}'.format(i): 'tata{}'.format(i) for i in range(4)},
            ],
        )

    def test_table1(self):
        table = PdfTable(text_table1[1:])
        self.assertEqual(table.columns, ['header0', 'header1', 'header2', 'header3'])

        # Smart distance assumes the field is always farther than the header
        self.assertEqual(
            list(table.parse(smart_distance=True)),
            [
                {'header{}'.format(i): 'toto{}'.format(i) for i in range(4)},
                {'header{}'.format(i): 'tata{}'.format(i) for i in range(4)},
            ],
        )


class RemoveFirstOnTheRightTestCase(unittest.TestCase):
    def test_remove_first_on_the_right_when_many(self):
        res = _remove_first_on_the_right('some content aaa with aaas in it', 'aaa')
        self.assertEqual(res, 'some content aaa with s in it')

    def test_remove_first_on_the_right_when_one(self):
        res = _remove_first_on_the_right('some content with aaas in it', 'aaa')
        self.assertEqual(res, 'some content with s in it')

    def test_remove_first_one_the_right_when_none(self):
        res = _remove_first_on_the_right('some content with s in it', 'aaa')
        self.assertEqual(res, 'some content with s in it')


class RemoveFirstOnTheLeftTestCase(unittest.TestCase):
    def test_remove_first_on_the_left_when_many(self):
        res = _remove_first_on_the_left('some content aaa with aaas in it', 'aaa')
        self.assertEqual(res, 'some content  with aaas in it')

    def test_remove_first_on_the_left_when_one(self):
        res = _remove_first_on_the_left('some content with aaas in it', 'aaa')
        self.assertEqual(res, 'some content with s in it')

    def test_remove_first_one_the_left_when_none(self):
        res = _remove_first_on_the_left('some content with s in it', 'aaa')
        self.assertEqual(res, 'some content with s in it')
