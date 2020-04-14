"""SpainOperator spider module.

Source: https://www.enagas.es/enagas/en/Gestion_Tecnica_Sistema/Operacion_del_Sistema_Gasista/Plan_de_Operacion_del_Sistema  # noqa

Website provides data on slots for a number of Spanish terminals.
Each slot corresponds to a vessel's date of arrival (realised or scheduled).

"""
import datetime as dt

from dateutil.relativedelta import relativedelta
from scrapy import Request
import xlrd

from kp_scrapers.lib import excel
from kp_scrapers.models.items_inventory import IOItem
from kp_scrapers.spiders.operators import OperatorSpider
from kp_scrapers.spiders.operators.base import BaseOperatorSpider


ONE_MONTH = relativedelta(months=1)
ONE_DAY = dt.timedelta(days=1)

DATE_COLUMNS = {'producción': 2, 'production': 1}  # C  # B

INSTALLATIONS_COLUMNS = {
    'producción': {
        'barcelona': 3,  # column D of xls file
        'cartagena': 7,  # H
        'huelva': 11,  # L
        'bilbao': 15,  # P
        'sagunto': 19,  # T
        'mugardos': 23,  # X
    },
    'production': {
        'barcelona': 2,  # column C of xls file
        'cartagena': 7,  # H
        'huelva': 12,  # M
        'bilbao': 17,  # R
        'sagunto': 22,  # W
        'mugardos': 27,  # AB
    },
}

OFFSET_COLUMN_IDX = {'level_o': 0, 'output_o': 1, 'input_cargo': 2}

START_ROW_IDX = 8
END_ROW_IDX = 40
INSTALLATION_UNIT = 'M3LNG'
NOT_AVAILABLE_HOUR = 9


class SpainOperatorSpider(OperatorSpider, BaseOperatorSpider):
    name = 'SpainOperator'
    provider = 'Enagas'
    version = '0.2.0'
    # TODO define an Operator model

    start_urls = [
        # unofficial API for slots in specified year
        'https://www.enagas.es/web-corporativa-ext-templating/webcorp/PoWeb/getHTMLTable?anio={year}&language=en'  # noqa
    ]

    # configuration expected by the parent class for initialization
    min_date = dt.datetime(2011, 8, 31)
    date_format = '%d/%m/%Y'
    lag_past = 2

    def __init__(self, date=None, *args, **kwargs):
        # be aware that you can pass start_date and end_date as parameters for BaseOperatorSpider
        # but the spider will scrape data for the whole month ...
        super().__init__(*args, **kwargs)

        # source provides data on a monthly basis:
        # e.g. "July 2019", "August 2019", ...
        self.month, self.year = (
            date.split() if date else dt.datetime.utcnow().strftime('%B %Y').split()
        )

    def start_requests(self):
        """Entrypoint of SpainOperator spider."""
        yield Request(
            url=self.start_urls[0].format(year=self.year), method='POST', callback=self.get_file
        )

    def get_file(self, response):
        do_process = False
        # source lists dates in reverse chronological order
        for tag in response.xpath('//div[@class="mod_descargas"]//a')[::-1]:
            file_desc = tag.xpath('text()').extract_first()
            # check first if file is what was specified in the spider args
            if f'{self.month} {self.year}' in file_desc and 'Monthly' in file_desc:
                do_process = True

            # Force processing of data for all subsequent months after desired month.
            # We don't need to process the "M1" or "M2" file since they often contain outdated
            # data as opposed to the "Monthly" file.
            if not do_process:
                continue

            file_url = tag.xpath('@href').extract_first()
            yield response.follow(url=file_url, callback=self.parse_file)

    def parse_file(self, response):
        workbook = excel.decrypt(contents=response.body)
        worksheet = workbook.sheet_by_index(0)
        worksheet_type = self._detect_worksheet_type(worksheet)
        self.logger.info('Worksheet type detected: %s', worksheet_type)

        for row_idx in range(START_ROW_IDX, END_ROW_IDX):
            # Transforme xls date into python datetime
            xldate = worksheet.cell(row_idx, DATE_COLUMNS[worksheet_type]).value
            try:
                raw_date = xlrd.xldate.xldate_as_datetime(float(xldate), 0)
            except ValueError:
                self.logger.warning('failed to parse date, skipping: {}'.format(xldate))
                continue

            for inst, col_idx in INSTALLATIONS_COLUMNS[worksheet_type].items():
                item = IOItem(date=raw_date.isoformat(' '), city=inst, unit=INSTALLATION_UNIT)

                for field, delta in OFFSET_COLUMN_IDX.items():
                    cell = worksheet.cell(row_idx, col_idx + delta).value
                    # Make sure the cell is not empty, it is useful for
                    # field like output_cargo that may be empty
                    if not cell:
                        self.logger.warning('failed to find cell, skipping')
                        continue

                    item[field] = int(cell * 1000)  # per m3 instead of per thousand of m3

                yield item

    @staticmethod
    def _detect_worksheet_type(worksheet):
        """Detect the structure of the XLS worksheet.

        There are two worksheet structures known thus far:
            - old: contains header string of "Producción"
            - new: contains header string of "Production"

        The offsets differ slightly for each worksheet structure, so we need to detect it,
        and use a different offset accordingly.

        """
        header = []
        for head_idx in range(START_ROW_IDX):
            header.extend([str(c).lower() for c in worksheet.row_values(head_idx)])

        # check for string presence in header to determine file structure
        if 'producción' in ''.join(header):
            structure = 'producción'
        elif 'production' in ''.join(header):
            structure = 'production'
        else:
            raise ValueError('Unknown file structure detected')

        return structure
