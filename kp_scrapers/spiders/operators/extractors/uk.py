# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals

import six

from kp_scrapers.lib.excel import GenericExcelExtractor


class ExcelExtractorUK(GenericExcelExtractor):
    def create_cursors(self):
        cells = {
            'date': 'Gasday',
            'city': 'Site Name',
            'inflow_o': 'Inflow',
            'outflow_o': 'Outflow',
            'level_o': 'Opening\sStock',
            'type': 'Operator\sType',
        }
        self.create_aliases(cells)

    def are_sheets_end(self):
        return self.sheet.name == 'Definitions'

    def is_xls_end(self, line):
        for (cell_name, cell_val) in six.iteritems(self.row):
            if self.rowl_at(line, cell_name) == '':
                return True

        return False

    def parse_row(self, line):
        if self.rowl_at(line, 'type') == 'LNG':
            item = self.build_default_item_curs(line, 'KWH', '%d-%b-%Y', ['type'])
            if item is not None:
                # From the data scraped :
                # netflow = level(J) - level(J-1)
                # outflow = netflow if netflow < 0
                # inflow = netflow if netflow > 0
                if item['inflow_o'] > 0:
                    item["net_flow_o"] = item['inflow_o']
                else:
                    item['net_flow_o'] = -item['outflow_o']

                if item["level_o"] != '0' and item['level_o'] != '0.0':
                    return item

        return None
