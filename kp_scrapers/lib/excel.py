from datetime import datetime
from io import BytesIO
import re

import msoffcrypto
import six
from six.moves import range
import xlrd

from kp_scrapers.lib.date import create_str_from_time, may_parse_date_str
from kp_scrapers.models.items_inventory import IOItem


def xldate_to_datetime(xldate_float, sheet_datemode):
    """ Converts xldate float to datetime

    For reference: http://www.lexicon.net/sjmachin/xlrd.html#xlrd.xldate_as_tuple-function

    Args:
        xldate_float (float):
        sheet_datemode (int):

    Returns:
        datetime:

    """
    return datetime(*xlrd.xldate_as_tuple(xldate_float, sheet_datemode))


def is_xldate(cell):
    """Check if cell type is a formatted date.

    See https://xlrd.readthedocs.io/en/latest/api.html#xlrd.sheet.Cell

    Args:
        cell (xlrd.sheet.Cell):

    Returns:
        bool: True if cell is formatted as a date

    """
    return cell.ctype == 3


def format_cells_in_row(raw_row, sheet_mode):
    """Handle xlrd.xldate.XLDateAmbiguous

    Using sheet.book.datemode will try to convert certain cells into a date
    when it should not. This function is to handle xlrd.xldate.XLDateAmbiguous cases
    and return the original cell value

    Args:
        raw_row List[str]:
        sheet_mode: sheet object

    Returns:
        List[str]:

    """
    row = []
    for cell in raw_row:
        if is_xldate(cell):
            try:
                cell = xldate_to_datetime(cell.value, sheet_mode).isoformat()
            except Exception:
                cell = str(cell.value)

        else:
            cell = str(cell.value)
        row.append(cell)

    return row


def decrypt(contents, password='VelvetSweatshop'):
    """Decrypt a password-protected spreadsheet file.

    NOTE some spreadsheets generated with MS Excel may be saved in encrypted formated,
    in which case it is done so automatically with a magic password "VelvetSweatshop"

    Args:
        contents (bytestring):

    Returns:
        Workbook:

    Raises:
        xlrd.biffh.XLRDError:

    """
    try:
        return xlrd.open_workbook(file_contents=contents)

    # xlrd module doesn't handle passwords (see https://github.com/python-excel/xlrd)
    # using msoffcrypto to decrypt file instead
    except xlrd.biffh.XLRDError as err:
        if 'Workbook is encrypted' not in str(err):
            raise err

        # write to in-memory buffer
        output = BytesIO()
        xls_file = msoffcrypto.OfficeFile(BytesIO(contents))
        xls_file.load_key(password=password)
        xls_file.decrypt(output)

        output.seek(0)
        return xlrd.open_workbook(file_contents=output.read())


class ExcelExtractorException(Exception):
    pass


class GenericExcelExtractor(object):
    """Simple Mixin Which manage open/Read on xls files on memory + basic parse methods.

    """

    def __init__(self, content, url, start_date):
        xl_open = xlrd.open_workbook(file_contents=content)
        self.book = xl_open
        self.sheet = xl_open.sheet_by_index(0)
        self.url = url
        self.start_date = start_date

    # A very basic parse method
    def parse_sheets(self):
        for nsheet in range(self.book.nsheets):
            self.sheet = self.book.sheet_by_index(nsheet)
            if self.are_sheets_end():
                break
            for item in self.parse_excel():
                yield item

    def parse_named_sheet(self, sheet_name):
        self.sheet = self.book.sheet_by_name(sheet_name)
        for item in self.parse_excel():
            yield item

    # A very basic parse method
    def parse_excel(self):
        self.create_cursors()
        y_curs = self.y_beg
        while y_curs < self.sheet.nrows and self.is_xls_end(self.sheet.row_values(y_curs)) is False:
            yield self.parse_row(self.sheet.row_values(y_curs))
            y_curs += 1

    # Return a simple (y, x) tuple, of a position of a specified text
    def key_pos(self, key):
        for rownum in range(self.sheet.nrows):
            x = 0
            for x_obj in self.sheet.row_values(rownum):
                if x_obj == key:
                    return (rownum, x)
                x += 1
        return None

    def key_pos_regex(self, key):
        pattern = re.compile(key, flags=re.IGNORECASE)
        for rownum in range(self.sheet.nrows):
            x = 0
            for x_obj in self.sheet.row_values(rownum):
                if pattern.search(x_obj):
                    return (rownum, x)
                x += 1
        return None

    """
    Abstracts Method
    """

    def parse_row(self, row):
        return

    # Create your cursors to select a specific column in rows
    def create_cursors(self):
        return

    def is_xls_end(self, row):
        return False

    def are_sheets_end(self):
        return False

    """
    Public Methods
    """

    # dict of (string_key, x_pos)
    row = {}
    y_beg = 0

    # Return Item in the y(n) row in the column "cell_name"
    def row_at(self, cell_name, y):
        return self.sheet.cell_value(y, self.row[cell_name])

    # Return item in the column "key" in the row "line"
    def rowl_at(self, line, key):
        return line[self.row[key]]

    def create_aliases(self, cells, is_fixed=False):
        for cell_alias, cell_name in six.iteritems(cells):
            if is_fixed:
                (y, x) = self.key_pos(cell_name)
            else:
                (y, x) = self.key_pos_regex(cell_name)
            self.row[cell_alias] = x
            self.y_beg = y + 1

    def build_default_item_curs(self, line, unit_str, date_frmt=None, no_curses=[], comp_date=True):
        """
        Allow to build a default item easily
        Take in param the unit and the date format (which allow us to convert in datetime format)
        """
        item = IOItem()
        row_datetime = self._extract_date(self.rowl_at(line, 'date'), date_frmt)
        if comp_date:
            if self.start_date is not None and row_datetime < self.start_date:
                return None
        item['unit'] = unit_str
        for key, value in self.row.items():
            take_data = True
            for no_curs in no_curses:
                if no_curs == key:
                    take_data = False
            if take_data:
                item[key] = self.rowl_at(line, key)
        item['src_file'] = self.url
        item['date'] = create_str_from_time(row_datetime)
        return item

    def _extract_date(self, raw_date, date_frmt):
        # Sometimes, the date value may be inconsistently entered in the excel file
        # e.g. '28.02.2018' when parsed by xlrd returns either the string itself or
        # a float 43158.0. Hence, we check that date parsed is not a float first.
        if date_frmt is not None and not isinstance(raw_date, float):
            return may_parse_date_str(raw_date, fmt=date_frmt)
        else:
            return xldate_to_datetime(raw_date, self.sheet.book.datemode)
