from datetime import timedelta
import logging
import re

from dateutil.parser import parse as date_parse

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.i18n import SPANISH_TO_ENGLISH_MONTHS
from kp_scrapers.lib.parser import may_strip


logger = logging.getLogger(__name__)


def tabula_options_identify_port_activity_page(page):
    return {
        '--pages': [str(page)],  # pages to check for port activity table
        '--stream': [],  # stream-mode extraction (more reliable if no ruling lines between cells)
        '--area': ['16.44,18.495,204.473,804.533'],  # fixed dimensions of table to extract from
    }


def tabula_options_barcelona(page):
    return {
        '--pages': [str(page)],  # page to extract table from
        '--stream': [],  # stream-mode extraction (more reliable if no ruling lines between cells)
        '--area': ['340.441,55.485,703.324,767.543'],  # fixed dimensions of table to extract from
        '--columns': ['130,206,286,367,423,480,542,607,688,762'],  # table column coordinates
    }


def tabula_options_tarragona(page):
    return {
        '--pages': [str(page)],  # page to extract table from
        '--stream': [],  # stream-mode extraction (more reliable if no ruling lines between cells)
        '--area': ['744.424,413.055,1100.966,766.515'],  # fixed dimensions of table to extract from
        '--columns': ['499,579,676,761'],  # table column coordinates
    }


def tabula_options_reported_date():
    return {
        '--pages': ['1'],  # page to extract reported date from
        '--stream': [],  # stream-mode extraction (more reliable if no ruling lines between cells)
        '--area': ['69.87,660.683,84.255,760.35'],  # fixed dimensions of page to extract from
    }


def parse_raw_date(raw_date):
    """Parse raw date string as ISO-8601 formatted date string.

    Args:
        raw_date (str): input date in this format: '<DATE> de <MONTH> del <YEAR>'

    Returns:
        str: datetime stamp in ISO-8601 format

    """
    raw_date = raw_date.lower().replace('del', '').replace('de', '')
    day, month, year = raw_date.split()
    month = SPANISH_TO_ENGLISH_MONTHS[month]
    return to_isoformat(' '.join([day, month, year]))


class MenCarParser(object):
    # 0-based column index where product data is stored, for tarragona port
    tarragona_product_idx = 3
    # page where we can find port activity tables
    port_activity_page = 1

    def init_port_activity_page(self, response):
        """Find and initialise page number from which we will extract port activity data.

        Since the source is in the form of a newsletter that has variable length, the page on which
        port activity data falls on will change from day to day.

        Iterating across all pages, extract top portion of page as a text stream and try to find
        relevant string identifying page as containing port activity.

        As a safeguard against an infinite loop, this function is hardcoded to return
        if 30 pages have been traversed and the port activity page has not been located.

        Args:
            response (scrapy.HtmlResponse):

        Returns:
            bool: True if port activity page is located, else False

        """
        while True:
            # safeguard to prevent spider from running indefinitely
            # `30` is a conservative upper bound, since the pdf is usually 15 pages long
            if self.port_activity_page >= 30:
                return False

            for line in self.extract_pdf_table(
                response,
                lambda x: x,
                **tabula_options_identify_port_activity_page(page=self.port_activity_page),
            ):
                if 'PUERTO DE BARCELONA' in line:
                    logger.info('Port activity found on page {}'.format(self.port_activity_page))
                    return True

            # go to next page to try and identify port activity
            self.port_activity_page += 1

    def extract_table_and_headers(self, response, port):
        """Extract table and headers from table corresponding to specified port

        Args:
            response (scrapy.HtmlResponse):
            port (str): accepts only `barcelona` or `tarragona`

        Returns:
            List[List[str]], List[str]: table, headers

        """
        if port == 'Barcelona':
            table = self.extract_pdf_table(
                response,
                self._process_barcelona_rows,
                **tabula_options_barcelona(page=self.port_activity_page),
            )
        elif port == 'Tarragona':
            table = self.extract_pdf_table(
                response,
                self._process_tarragona_rows,
                **tabula_options_tarragona(page=self.port_activity_page),
            )
            table = self.combine_tarragona_rows(table)
        else:
            raise ValueError('Only "Barcelona" and "Tarragona" ports supported')

        # 1st table row contain headers
        return table[1:], table[0]

    def _process_barcelona_rows(self, table):
        """Process rows for barcelona port.

        Decode all rows as unicode strings since tabula outputs byte strings by default.
        Extract matching date for each table section based on the section's description.
        Yield only rows that contain table data, skipping table section description.

        Known table section headers (each section has a different matching date):
            - "Buques que efectuaron operaciones durante la noche del día anterior"
            - "Buques que efectuaron operaciones durante las 20 horas del viernes 16.3.18 hata el domingo 18.3.18"  # noqa
            - "Buques que efectuaron operaciones durante el lunes 18.3.18"
            - "Ultima hora"

        Args:
            table (List[List[str]]): list of table rows from pdf

        Yields:
            List[str]:

        """
        matching_date = None
        for idx, row in enumerate(table):
            # tabula stores string data as bytes by default
            row = [cell for cell in row]

            # try deciphering matching_date of subsequent rows
            if any('Buques que efectuaron' in cell for cell in row):
                raw_matching_date = ''.join(row)
                date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{2})', raw_matching_date)
                # matching date is mentioned explictly in table section description
                if date_match:
                    matching_date = to_isoformat(date_match.group(1))
                    logger.debug('Found matching date: {}'.format(matching_date))

                # sometimes matching date is described implicitly in words in the pdf
                elif 'anterior' in raw_matching_date:
                    matching_date = to_isoformat(
                        str(date_parse(self.reported_date, dayfirst=False) - timedelta(days=1)),
                        dayfirst=False,
                    )
                    logger.debug('Found matching date: {}'.format(matching_date))
                else:
                    raise ValueError('Unable to find matching date: {}'.format(raw_matching_date))
            elif any('ltima hora' in cell for cell in row):
                matching_date = self.reported_date
                logger.debug('Found matching date: {}'.format(matching_date))

            # do not yield table section headers
            if not (
                '/EXPORT' in row
                or any('Buques que efectuaron' in cell for cell in row)
                or any('ltima hora' in cell for cell in row)
            ):
                row.append('matching_date' if idx == 0 else matching_date)
                yield row

    def _process_tarragona_rows(self, table):
        """Process rows for tarragona port.

        Decode all rows as unicode strings since tabula outputs byte strings by default.
        Extract matching date for each table section based on the section's description.
        Yield only rows that contain table data, skipping table section description.

        Matching date in table header can have the following format:
            - "Buques atracados el día 18 de marzo del 2018 (Información actualizada a las 8:30 horas)"  # noqa
            - "Buques atracados el día18 de marzo del 2018 (Información actualizada a las 8:30 horas)"  # noqa

        raw table row order:
            - table name (discard):        ['PUE', 'RTO DE', 'TARRAGO', 'NA']
            - matching_date (extract):     ['Buques atracados el d\xc3\xada', '29 de marzo del 2018 (', 'Informaci\xc3\xb3n actualizada a la', 's 8:00 horas)']  # noqa
            - header (keep):               ['BUQUE', 'MUELLE', 'CONSIGNATARIO/', 'TONS']
            - 2nd header (discard):        ['', '', 'ESTIBADOR', '']
            - data row (keep):             ['CANNETO M', 'FONDEADO ZONA II', 'IB\xc3\x89RICA MAR\xc3\x8dTIMA', '-']  # noqa
            - data row (keep):             ['OTTOMANA', 'PANTAL\xc3\x81 REPSOL', 'MARITIME/REPSOL', 'P. PETROL\xc3\x8dFEROS/23.300-D']  # noqa
            - subsequent data rows (keep): ...
            - ...

        Args:
            table (List[List[str]]): list of table rows from pdf

        Yields:
            List[str]:

        """
        for idx, row in enumerate(table):
            # tabula stores string data as bytes by default
            row = [cell for cell in row]

            # try deciphering matching_date in first row
            if idx == 1:
                matching_date = parse_raw_date(
                    may_strip(''.join(row).split('atracados el día')[1].split('(Info')[0])
                )
                logger.debug('Found matching date: {}'.format(matching_date))

            if idx >= 2:
                if not ('ESTIBADOR' in ' '.join(row) or 'PUERTO DE' in ''.join(row)):
                    # third row contains headers
                    row.append('matching_date' if idx == 2 else matching_date)
                    yield row

    def combine_tarragona_rows(self, table_rows):
        """Combine table rows for Tarragona port.

        Due to the possbility of split rows for tarragona port, we need to conduct further
        pre-processing and combining of rows for completeness of data.

        For example:
            Row 1: ['YUHSAN', 'PANTAL', 'E. ERHARDT', u'GASES ENERGETICOS']
            Row 2: ['', '', '', 'DEL PETROLEO/45.000-D']

        Row 2's product info will be combined with Row 1, and Row 2 discarded entirely.

        Args:
            table_rows (List[str]):

        Returns:
            List[List[str]]:

        """
        for idx, row in enumerate(table_rows):
            # current row only has product info; to be combined with the previous row
            if self._has_product_only(row):
                # modify previous row product column (i.e. combine next row's data with it)
                table_rows[idx - 1][self.tarragona_product_idx] += row[self.tarragona_product_idx]

        # remove extraneous rows now that we've combined them
        return [row for row in table_rows if row[0]]

    def _has_product_only(self, row):
        """Check if list contains only product data, with other cells being empty strings.

        For example:
            Row 1 has all data:          ['YUHSAN', 'PANTAL', 'E. ERHARDT', u'GASES ENERGETICOS', 'matching_date']  # noqa
            Row 2 has only product data: ['', '', '', 'DEL PETROLEO/45.000-D', '2018-03-21T00:00:00']  # noqa

        Args:
            row (List[str]):

        Returns:
            bool: True if list contains product data in correct cell index

        """
        stripped_row = [cell for cell in row if cell]

        # list is an ordinary row and also contains additional data, so we don't combine this
        if len(stripped_row) != 2:
            return False
        # product info will always be in specified column index (i.e., index 3)
        if row.index(stripped_row[0]) != self.tarragona_product_idx:
            return False

        return True

    def extract_reported_date(self, response):
        """Extract reported date from pdf.

        `date_str` is expected to follow this format:
            - "Jueves 29 DE MARZO DE 2018" (<day_of_week> <date> DE <spanish_month> DE <year>)

        Args:
            response (scrapy.HtmlResponse):

        Returns:
            str: reported_date

        """
        date_str = self.extract_pdf_table(response, lambda x: x, **tabula_options_reported_date())[
            0
        ][0]
        # remove day of week (occurs as first word) from datestring with `str.partition`
        return parse_raw_date(date_str.partition(' ')[2])
