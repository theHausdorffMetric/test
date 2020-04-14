# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime
import os
import re

from kp_scrapers.spiders.bases.pdf import PdfSpider


class SlotBaseFrance(PdfSpider):
    """Convert pdf to text and extract date from table where # noqa
        - date are represented as a cross in table
        - columns have variable size

    We get something with random spaces, what we want is extract start and end index of each column :
        ex:  '     JANUARY   FEBRUARY         MARCH            APRIL           MAY      JUNE          JULY   AUGUST   SEPTEMBER   OCTOBER   NOVEMBER   DECEMBER'
        ex:  '                     x                                                     x                     x'
    result:    | january |    february    |      march     | etc...

    Then we check if a cross is between start and end, then there is a slot
    """

    months = [
        'JANUARY',
        'FEBRUARY',
        'MARCH',
        'APRIL',
        'MAY',
        'JUNE',
        'JULY',
        'AUGUST',
        'SEPTEMBER',
        'OCTOBER',
        'NOVEMBER',
        'DECEMBER',
    ]
    pdf_name = 'pdf_slotbasefrance.pdf'

    def _get_text_from_response(self, pdf, page_to_extract):
        """Extract text from pdf

        Arguments:
            pdf {str} -- pdf from scrapy.body
            page_to_extract {int} --

        Returns:
            list -- list of row (str)
        """
        self.save_file(self.pdf_name, pdf)
        text = self.pdf_to_text(
            filepath=os.path.join(self.data_path, self.pdf_name), page=page_to_extract
        )
        return text.splitlines()

    def _get_header_and_start_line(self, rows):
        """Extract header and index of header

        Arguments:
            rows {list} -- list of row (str)

        Returns:
            (str, int) -- text of header and index of header
        """
        for i, row in enumerate(rows):
            if all(month in row for month in self.months):
                header = row
                start_line = i
                break
        return header, start_line

    def _get_columns_size(self, header):
        """Get column size from header

        Arguments:
            header {str} --

        Returns:
            dict -- {column_name: {start: int, end: int}}
        """
        column_sizes = {}  # column_sizes represent the start and end index of a month
        for window in re.finditer('[A-Z]* *', header):  # regex = any char + any space
            column_sizes[header[window.start() : window.end()].strip()] = {
                'start': window.start(),
                'end': window.end(),
            }
        return column_sizes

    def _get_dates(self, rows, column_sizes):
        """get date where there is a 'X' in cell

        Arguments:
            rows {list} -- list of row (str)
            column_sizes {dict} -- output of _get_columns_size

        Returns:
            list -- list of date with format %d-%m-%Y
        """
        dates = []
        for row in rows:  # + 2 to pop header
            for month in self.months:
                # a slot is represented by a x in a cell
                if 'x' in row[column_sizes[month]['start'] : column_sizes[month]['end']].lower():
                    day = ''.join([c for c in row if c.isdigit()])
                    date = datetime.strptime(
                        day + '-' + month.title() + '-' + self.year, '%d-%B-%Y'
                    ).strftime('%d-%m-%Y')
                    dates.append(date)
        return dates
