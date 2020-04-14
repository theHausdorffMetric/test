import datetime as dt
import re

from scrapy import Spider
import xlrd

from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.mombasa import normalize


MAX_ROW_LENGTH = 14
HEADER_SIGN = 'VESSEL NAME'
ETA_COL_IDX = 5

RESUME_PROCESSING = [
    'CONVENTIONAL VESSELS',
    'TANKERS VESSELS',
    'WAITERS FOR CONVENTIONAL BERTHS',
    'WAITERS FOR KOT',
    'WAITERS FOR SOT/MBK',
]

PAUSE_PROCESSING = ['OTHERS VESSELS', 'WAITERS FOR SHIP CONVINIENCE']


class MombasaSpider(PortAuthoritySpider, Spider):
    name = 'Mombasa'
    provider = 'Mombasa'
    version = '2.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        # this source changed the way it presents data -- excel service powered by xlviewer
        # 'https://www.kpa.co.ke/Pages/14DaysList.aspx',
        'https://www.ksaa.co.ke/downloads/check_14_day_lists'
    ]

    reported_date = (
        dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
    )

    def parse(self, response):
        """Entry point of Mombasa port authority spider.

        Args:
            response:

        Returns:
            PortCall:

        """
        latest_report = response.xpath('//td/a/@href').extract_first()
        yield response.follow(url=latest_report, callback=self.parse_file)

    def parse_file(self, response):
        sheet = xlrd.open_workbook(file_contents=response.body, on_demand=True).sheet_by_index(0)

        headers = None
        do_process = False
        for idx, raw_row in enumerate(sheet.get_rows()):
            row = [
                xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                if is_xldate(cell)
                else may_strip(str(cell.value))
                for cell in raw_row
            ][:MAX_ROW_LENGTH]

            # filtering unwanted portions for easier processing of remarks field (to retrieve
            # cargo information)
            # this hold true when the field names don't change
            # if any(row[0] == alias for alias in NO_PROCESSING):
            #     processing = False

            if row and any(sub in row[0] for sub in RESUME_PROCESSING):
                do_process = True

            if row and any(sub in row[0] for sub in PAUSE_PROCESSING):
                do_process = False

            if not do_process:
                continue

            # vessels expected table
            if HEADER_SIGN in row:
                headers = row
                continue

            if headers and row[ETA_COL_IDX]:
                raw_item = {headers[cell_idx]: cell for cell_idx, cell in enumerate(row)}
                raw_item.update(self.meta_field)

                yield normalize.process_item(raw_item)

            # waiters info in text
            waiters = self.parse_waiters(row[0])
            if waiters:
                raw_item = {str(cell_idx): cell for cell_idx, cell in enumerate(waiters)}
                raw_item.update(self.meta_field)

                yield normalize.process_item(raw_item)

    @staticmethod
    def parse_waiters(cell):
        """Parse waiters info with regex from row.

        Args:
            cell (str):

        Returns:

        """
        pattern = (
            # date and time
            r'(\d{2}.\d{2}.\d{4} \d{4})\s'
            # vessel name
            r'([\w\s\']+)\s'
            # length
            r'([\d.]+)\s'
            # draft
            r'([\d.]+)\s'
            # unknown field
            r'(OBJ|EXP|SAL|STR|ALB|BFL)'
            # remarks, equivalent to table field `REMARKS`
            r'\s(.+)'
        )

        _match = re.match(pattern, cell)
        return list(_match.groups()) if _match else None

    @property
    def meta_field(self):
        return {
            'port_name': self.name,
            'provider_name': self.provider,
            'reported_date': self.reported_date,
        }
