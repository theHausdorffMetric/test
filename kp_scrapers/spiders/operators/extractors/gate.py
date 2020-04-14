# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals
import re

from kp_scrapers.lib.date import create_str_from_time
from kp_scrapers.lib.excel import GenericExcelExtractor, xldate_to_datetime
from kp_scrapers.lib.parser import str_to_float
from kp_scrapers.models.items_inventory import IOItem
from kp_scrapers.spiders.operators.utils import unit_exist


class ExcelExtractorGate(GenericExcelExtractor):
    unit_str = ''

    def create_cursors(self):
        cells = {
            'date': 'Date',
            'level_o': 'Amount of Gas in LNG facility',
            'input_o': 'Inflow',
            'output_o': 'Outflow',
        }
        self.create_aliases(cells)
        unit_str_row = self.row_at('level_o', 0)  # We get the text were unit was written
        self.unit_str = unit_exist(re.search(r'\(([A-Za-z0-9_]+)\)', unit_str_row).group(1))

    def parse_row(self, line):
        item = IOItem()
        row_datetime = xldate_to_datetime(self.rowl_at(line, 'date'), self.book.datemode)

        if self.start_date is not None and row_datetime < self.start_date:
            return None

        item['unit'] = self.unit_str
        item['date'] = create_str_from_time(row_datetime)
        item['level_o'] = str_to_float(self.rowl_at(line, 'level_o'))
        item['input_o'] = self.rowl_at(line, 'input_o')
        item['output_o'] = self.rowl_at(line, 'output_o')
        item['src_file'] = self.url

        return item
