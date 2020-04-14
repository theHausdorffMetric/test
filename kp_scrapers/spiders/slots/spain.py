"""SlotSpain spider module.

Source: https://www.enagas.es/enagas/en/Gestion_Tecnica_Sistema/Operacion_del_Sistema_Gasista/Plan_de_Operacion_del_Sistema  # noqa

Website provides data on slots for a number of Spanish terminals.
Each slot corresponds to a vessel's date of arrival (realised or scheduled).

"""
import datetime as dt

from scrapy import Request, Spider
import xlrd

from kp_scrapers.lib import excel
from kp_scrapers.models.items import Slot
from kp_scrapers.spiders.slots import SlotSpider


# FIXME a database id has nothing to do here
INSTALLATION_ID = {
    'barcelona': 3500,  # Barcelona LNG
    'cartagena': 3498,  # Cartagena ESP
    'huelva': 3503,  # Huelva LNG
    'bilbao': 3497,  # Bilbao LNG
    'sagunto': 3499,  # Sagunto LNG
    'mugardos': 3504,  # Reganosa Ferrol LNG
}

DATE_COLUMNS = {'producción': 2, 'production': 1}  # C  # B

INSTALLATIONS_COLUMNS = {
    'producción': {
        'barcelona': 6,  # column G of xls file
        'cartagena': 10,  # K
        'huelva': 14,  # O
        'bilbao': 18,  # S
        'sagunto': 22,  # W
        'mugardos': 26,  # AA
    },
    'production': {
        'barcelona': 6,  # column G of xls file
        'cartagena': 11,  # L
        'huelva': 16,  # Q
        'bilbao': 21,  # V
        'sagunto': 26,  # AA
        'mugardos': 31,  # AF
    },
}

START_ROW_IDX = 8
END_ROW_IDX = 40


class SlotSpainSpider(SlotSpider, Spider):
    name = 'SlotSpain'
    provider = 'Enagas'
    version = '0.3.1'
    # TODO define a Slot model

    start_urls = [
        # unofficial API for slots in specified year
        'https://www.enagas.es/web-corporativa-ext-templating/webcorp/PoWeb/getHTMLTable?anio={year}&language=en'  # noqa
    ]

    def __init__(self, date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # source provides data on a monthly basis:
        # e.g. "July 2019", "August 2019", ...
        self.month, self.year = (
            date.split() if date else dt.datetime.utcnow().strftime('%B %Y').split()
        )

    def start_requests(self):
        """Entrypoint of SlotSpain spider."""
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
                date = xlrd.xldate.xldate_as_datetime(xldate, 0)
            except (ValueError, TypeError):
                continue

            for inst, col_idx in INSTALLATIONS_COLUMNS[worksheet_type].items():
                # We check if there is a cargo, if yes, there is a slot
                cell = worksheet.cell(row_idx, col_idx).value
                if cell:
                    item = Slot()
                    item['date'] = date.strftime('%d-%m-%Y')
                    item['installation_id'] = INSTALLATION_ID[inst]
                    item['on_offer'] = False

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
