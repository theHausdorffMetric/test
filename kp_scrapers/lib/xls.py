# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import six
from six.moves import range, zip
import xlrd


class Workbook(object):
    def __init__(self, filepath=None, content=None, first_title=None):
        """
        Args:
            filepath (str | unicode | None)
            content (str | unicode | None)
            first_title (str | unicode | None)
        """
        self._workbook = xlrd.open_workbook(filename=filepath, file_contents=content)
        self._first_title = first_title

    def __iter__(self):
        for sheet in self._workbook.sheets():
            yield Sheet(sheet, self._workbook.datemode, self._first_title)

    @property
    def items(self):
        for sheet in self:
            for item in sheet.items:
                yield item


class Sheet(object):
    def __init__(self, sheet, datemode, first_title):
        """
        Args:
            sheet (xlrd.sheet.Sheet)
            datemode ()
            first_title (str | unicode | None): the name of the first element of the title row

        """
        self._sheet = sheet
        self._datemode = datemode
        self._first_title = first_title

    @property
    def items(self):
        """
        Yields:
            dict: {'title_name': 'value'}

        """

        title_row, first_data_nrow, first_data_ncol = self._get_title_row()
        for nrow in range(first_data_nrow, self._sheet.nrows):
            processed_row = self._get_row(nrow)[first_data_ncol:]
            if self._is_empty_row(processed_row):
                break
            else:
                yield dict(list(zip(title_row, processed_row)))

    def _get_title_row(self):
        """
        Identify the row that represents the title of each column, and returns it
        along with its row number and the column number where it begins.

        Returns:
            list[str | unicode], int, int
        """
        for nrow in range(self._sheet.nrows):
            row = [v.lower() if v else v for v in self._get_row(nrow)]
            if not self._first_title or self._first_title in row:

                ncol = next(
                    i
                    for i, title in enumerate(row)
                    if title is not None
                    and (self._first_title is None or title == self._first_title)
                )
                return row[ncol:], nrow + 1, ncol
        return [], self._sheet.nrows, 0

    def _get_row(self, nrow):
        return [self._get_cell_value(cell) for cell in self._sheet.row(nrow)]

    def _is_empty_row(self, processed_row):
        """
        Checks whether an item is empty (all its values are None)

        Args:
            processed_row (list[str | unicode])
        Returns:
            bool
        """
        return all(v is None for v in processed_row)

    def _get_cell_value(self, cell):
        """
        Get the value of a cell as a string. If the cell contains a date,
        we return its isoformat. If an error occurs, we return an empty string
        which will be discarded later.
        Args:
            cell (xlrd.sheet.Cell)
        Returns:
            unicode | None
        """
        try:
            value = cell.value
            if cell.ctype == xlrd.XL_CELL_DATE:
                value = xlrd.xldate.xldate_as_datetime(value, self._datemode).isoformat()
            if isinstance(value, six.string_types):
                value = value.strip()
            return six.text_type(value) if value else None
        except Exception:
            return None
