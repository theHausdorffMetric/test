# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals
from datetime import timedelta
import logging
import re

import six

from kp_scrapers.lib.date import create_str_from_time, may_parse_date_str
from kp_scrapers.lib.excel import ExcelExtractorException, GenericExcelExtractor
from kp_scrapers.lib.parser import str_to_float
from kp_scrapers.models.items_inventory import IOItem


logger = logging.getLogger(__name__)

ITALIAN_DATES = {
    'january': 1,
    'genuary': 1,
    'february': 2,
    'march': 3,
    'april': 4,
    'may': 5,
    'mayl': 5,
    'june': 6,
    'july': 7,
    'august': 8,
    'september': 9,
    'october': 10,
    'november': 11,
    'december': 12,
    'dicember': 12,
}


def to_datetime(raw_dt):
    """Safely wrap the conversion."""
    try:
        return may_parse_date_str(raw_dt, '%B %Y')
    except ValueError:
        return None


class ExcelExtractorGLETTemplate(GenericExcelExtractor):
    def create_cursors(self):
        cells = {
            'date': 'Days',
            'level_o': 'LNG in Storage',
            'boat_io': 'Unloaded LNG',
            'output': 'Send out',
        }
        self.create_aliases(cells)
        # Just Some checkers, raise error if unit change
        self.row['level_o'] = self.choose_units(self.row['level_o'])
        self.row['boat_io'] = self.choose_units(self.row['boat_io'])
        self.row['output'] = self.choose_units(self.row['output'])
        self.y_beg += 1

    def is_xls_end(self, line):
        day_num = str_to_float(self.rowl_at(line, 'date'))

        if day_num is None:
            return True
        (month, year) = self.get_month_from_itstr(self.sheet.name)
        date_str = str(year) + '-' + str(month) + '-' + str(int(day_num)) + ' 00:00:00'
        try:
            may_parse_date_str(date_str)
        except ValueError:
            return True
        return False

    def choose_units(self, curs):
        x_pos_unit = curs + 1
        if self.sheet.cell_value(self.y_beg, x_pos_unit).lower() == '(kwh)':
            return x_pos_unit

        raise ExcelExtractorException('old unit (Kwh) changed')

    def are_sheets_end(self):
        try:
            date = self.get_month_from_itstr(self.sheet.name)
            if date is not None:
                return False
        except ExcelExtractorException as e:
            logger.error('failed to extract month: {}'.format(e))
            return True

        return True

    def parse_row(self, line):
        item = IOItem()
        (month, year) = self.get_month_from_itstr(self.sheet.name)
        day = self.rowl_at(line, 'date')
        date_str = str(year) + '-' + str(month) + '-' + str(int(day)) + ' 00:00:00'
        row_datetime = may_parse_date_str(date_str) + timedelta(hours=6)
        if self.start_date is not None and row_datetime < self.start_date:
            return None

        item['date'] = create_str_from_time(row_datetime)
        item['unit'] = 'KWH'
        item['level_o'] = str_to_float(self.rowl_at(line, 'level_o'))
        item['output_o'] = self.rowl_at(line, 'output')
        item['src_file'] = self.url

        cargo_val = str_to_float(self.rowl_at(line, 'boat_io'))
        if cargo_val is not None:
            item['input_cargo'] = cargo_val
        else:
            item['input_cargo'] = 0

        return item

    def get_month_from_itstr(self, sheet_name):
        title_cell = ''

        # Try a First match type
        try:
            title_cell = self.sheet.cell_value(0, 0)
            regex_string = r'.*GNL\s.*-[\s]*(.*)\s([0-9]+)'
            res = re.match(regex_string, title_cell)
            if res is not None:
                for date_key, value in six.iteritems(ITALIAN_DATES):
                    res_date = re.search(res.group(1), date_key, flags=re.IGNORECASE)
                    if res_date is not None:
                        return value, res.group(2)
        except IndexError:
            pass

        # Try a Second match Type
        res = re.match(r'^([a-zA-Z]+)\s*([0-9]+)', sheet_name)
        if res is not None:
            if res.group(2) < 2000:
                raise ExcelExtractorException('Date parsing error')
            for date_key, value in six.iteritems(ITALIAN_DATES):
                res_date = re.search(res.group(1), date_key, flags=re.IGNORECASE)
                if res_date is not None:
                    return value, res.group(2)
            my_datetime = to_datetime(res.group(0))
            if my_datetime is not None:
                return my_datetime.month, res.group(2)
        raise ExcelExtractorException(
            'Warning : These formats for date are not recognized : '
            '*** [' + title_cell + '] *** [' + sheet_name + ']'
        )
