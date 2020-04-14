# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals

from kp_scrapers.lib.excel import GenericExcelExtractor


class ExcelExtractorZeebrugge(GenericExcelExtractor):
    def create_cursors(self):
        cells = {
            'date': 'Gas day',
            'output_o': 'PF',
            'level_o': 'GIS',
            'output_forecast_o': 'DANSO',
        }
        self.create_aliases(cells)
        self.y_beg += 1

    def parse_row(self, line):
        item = self.build_default_item_curs(line, 'KWH', date_frmt='%d/%m/%Y', comp_date=False)

        if item['output_o'] != '':
            item['output_o'] = -int(item['output_o'])

        if item['level_o'] != '0' and item['level_o'] != '0.0':
            return item
