from scrapy import Spider
import xlrd

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.jordan import normalize, parser


class JordanSpider(PortAuthoritySpider, Spider):
    name = 'Jordan'
    provider = 'Jordan SA'
    version = '1.0.4'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.shipping.com.jo/page/port-position']

    reported_date_row = 1
    reported_date_column = 10
    header_start_row = 4
    header_end_row = 6
    table_start_row = 8

    def parse(self, response):
        # the last one contains the latest report
        url = response.css('span.file-link a::attr(href)').extract()[-1]

        yield response.follow(url, self.parse_xlsx)

    def parse_xlsx(self, response):
        sheet = xlrd.open_workbook(file_contents=response.body, on_demand=True).sheet_by_name(
            'PORTPOS&'
        )

        # get reported date
        reported_date = parser.get_reported_date(
            sheet.cell(self.reported_date_row, self.reported_date_column).value
        )

        # reconstruct the header
        headers = parser.get_table_header(sheet, self.header_start_row, self.header_end_row)

        for row_idx, row in enumerate(sheet.get_rows()):
            # title and header before row 8
            if row_idx <= self.table_start_row:
                continue

            # skip empty rows and header rows
            res = parser.check_row_type(row)
            if not res or res in parser.SUB_TABLE_TITLES:
                continue

            # data row
            data = [cell.value for cell in row]

            # assemble raw item and contextualise with meta info
            raw_item = parser.map_row_to_dict(
                headers,
                data,
                reported_date=reported_date,
                port_name='Aqaba',
                provider_name=self.provider,
            )

            # process raw item
            yield normalize.process_item(raw_item)
