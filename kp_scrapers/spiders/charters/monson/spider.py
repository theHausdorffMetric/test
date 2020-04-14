from datetime import datetime

from scrapy import Request, Spider
import xlrd

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.monson import normalize


SHEETS_IDX = range(1, 9)
HEADER_SIGN = 'ETA'

PORT_MAPPING = {'HAY POINT': 'HAY REEF', 'DALRYMPLE BAY': 'HAY REEF'}


class MonsonSpider(CharterSpider, Spider):
    name = 'Monson_Fixtures_COAL'
    version = '1.0.0'
    provider = 'Monson'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    start_urls = ['https://www.monson.com.au/research/']

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    reported_date = None
    port_name = None

    def parse(self, response):
        """Entry point of Monson_COAL spider.

        Args:
            response (scrapy.Response):

        Yields:
            PortCall | SpotCharter:

        """
        file_link = response.xpath(
            '//a[@title="Download Australian Coal Schedule 1"]/@href'
        ).extract_first()

        if not file_link:
            self.logger.error('Monson report url has been changed.')
            return

        # take utcnow as reported date, because for each tab the date might not be accurate
        # after transforming
        self.reported_date = (
            datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
        )

        yield Request(url=file_link, callback=self.parse_xlsx)

    def parse_xlsx(self, response):
        """Parse xlsx file.

        Args:
            response (scrapy.Response):

        Yields:
            PortCall | SpotCharter:

        """
        workbook = xlrd.open_workbook(file_contents=response.body, on_demand=True)
        for idx in SHEETS_IDX:
            sheet = workbook.sheet_by_index(idx)
            self.port_name = sheet.name.strip()

            current_header = None
            critical_idx = None
            for raw_row in sheet.get_rows():
                row = [
                    cell.value.upper().strip() if isinstance(cell.value, str) else cell.value
                    for cell in raw_row
                ]

                if HEADER_SIGN in row:
                    current_header = row
                    critical_idx = current_header.index(HEADER_SIGN)

                    # the first cell might be missing, and it would always be `VESSEL`
                    current_header[0] = 'VESSEL'
                    continue

                if current_header and row[critical_idx]:
                    raw_item = {header: row[idx] for idx, header in enumerate(current_header)}
                    raw_item.update(self.meta_field)

                    yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {
            'provider_name': self.provider,
            'reported_date': self.reported_date,
            'port_name': PORT_MAPPING.get(self.port_name, self.port_name),
        }
