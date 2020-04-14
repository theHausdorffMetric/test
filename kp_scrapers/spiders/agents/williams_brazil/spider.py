from scrapy.http import Request
from scrapy.spiders import Spider
import xlrd

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.williams_brazil import normalize


TABLE_HEADER_PATTERN = 'VESSEL'
COLUMN_INDICES = {
    'port': 1,
    'vessel': 1,
    'eta': 2,
    'etb': 3,
    'ets': 4,
    'load': 5,
    'discharge': 6,
    'product': 7,
    'previous_zone': 8,
    'next_zone': 9,
    'installation': 10,
}


class WBBrazilSpider(ShipAgentMixin, Spider):
    name = 'WB_Brazil'
    provider = 'Williams Brazil'
    version = '0.1.0'

    start_urls = ['http://www.udop.com.br/index.php?item=itens_boletins&id_indicador=123']
    base_url = 'http://www.udop.com.br/'

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def __init__(self, reported_date=None):
        """Init WBBrazilSpider.

        This spider by default extracts data from the latest report. The source updates itself with
        the latest report each week, as the first link in the page. However, source may be late to
        update sometimes, and would update multiple weeks at once if it missed updating the
        previous weeks.

        One can supply the `reported_date` to override this default behaviour of obtaining the
        latest report.

        Args:
            reported_date (str): date of report to obtain, formatted as DD/MM/YYYY

        """
        self.reported_date = reported_date

    def parse(self, response):
        """Extract report links from main page and download xls.

        Args:
            response (scrapy.Response):

        Yields:
            Request:

        """
        reports = response.xpath('//table//a[contains(@href, "download")]')
        if self.reported_date:
            self.logger.info(f'Processing specified report: {self.reported_date}')
            # only process report as specified in spider argument
            for report in reports:
                if report.xpath('./b/text()').extract_first() == self.reported_date:
                    yield Request(
                        self.base_url + report.xpath('./@href').extract_first(),
                        callback=self.extract_xls,
                    )
        else:
            # memoise `reported_date` for use later
            self.reported_date = reports[0].xpath('./b/text()').extract_first()
            # only process latest report as the default
            self.logger.info(
                f'No date provided, default to process latest report: {self.reported_date}'
            )
            yield Request(
                self.base_url + reports[0].xpath('./@href').extract_first(),
                callback=self.extract_xls,
            )

    def extract_xls(self, response):
        """Extract data inside spreadsheet file.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        # file only contains one spreadsheet
        sheet = xlrd.open_workbook(file_contents=response.body, on_demand=True).sheet_by_name(
            'Tankers Line Up'
        )

        current_port, previous_row = None, None

        for idx, row in enumerate(sheet.get_rows()):
            # NOTE headers don't need to be retrieved since we hardcode them as constants at
            # the start of this file
            row = [cell.value for cell in row]

            # get port name from title row (second element will be filled, the rest will be empty)
            if ' PORT' in row[COLUMN_INDICES['port']] and not any(
                row[COLUMN_INDICES['port'] + 1 :]
            ):
                current_port = may_strip(row[1])
                continue

            # discard all rows not within relevant table,
            # ignore empty rows and `NIL` vessel field
            if not current_port or not any(row) or row[COLUMN_INDICES['vessel']] == 'NIL':
                continue

            # discard row if vessel field is empty and date field (overflow) is present
            if not row[COLUMN_INDICES['vessel']] and (
                row[COLUMN_INDICES['eta']] or row[COLUMN_INDICES['etb']]
            ):
                continue

            # product field overflows to next line
            # product field is present but vessel field is empty
            # replace all fields (except for `product`, `load` and `discharge`)
            # with the values from the previous row
            if not row[COLUMN_INDICES['vessel']] and row[COLUMN_INDICES['product']]:
                for field in [
                    COLUMN_INDICES['product'],
                    COLUMN_INDICES['load'],
                    COLUMN_INDICES['discharge'],
                ]:
                    previous_row[field] = row[field]
                row = previous_row

            raw_item = {column: row[idx] for column, idx in COLUMN_INDICES.items()}
            # contextualise raw item with meta info
            raw_item.update(
                port_name=current_port,
                provider_name=self.provider,
                reported_date=self.reported_date,
            )
            yield from normalize.process_item(raw_item)

            # store previous row's values to use in backfilling data for some incomplete rows
            previous_row = row
