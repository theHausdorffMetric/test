from scrapy import FormRequest, Request, Spider

from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.tema import normalize, parser


class TemaSpider(PortAuthoritySpider, Spider):
    name = 'Tema'
    provider = 'Tema'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.exaf.eu/exaf/actus.php']

    # flags to indicate if we are extracting arrival/berthed/eta table rows
    arrival_table = False
    berthed_table = False
    eta_table = False

    # memoise table headers
    arrival_header = []
    berthed_header = []
    eta_header = []

    def start_requests(self):
        """Entrypoint for Tema spider.

        Website expects one to supply the country ("pays") and the port ("port").
        Note that Tema (223) is a port within Ghana (23).

        This function will get the HTML page containing the link to the Tema port spreadsheet.

        Yields:
            scrapy.FormRequest:

        """
        yield FormRequest(
            url=self.start_urls[0],
            # pays refers to country(Ghana), port is Tema
            formdata={'pays': '23', 'port': '223'},
            callback=self.get_spreadsheet,
        )

    def get_spreadsheet(self, response):
        """Get spreadsheet body from the HTML port page.

        Args:
            response (scrapy.Response):

        Returns:
            scrapy.Request:

        """
        link = 'http://www.exaf.eu' + response.xpath('//div[@id="result"]//@href').extract_first()
        yield Request(url=link, callback=self.parse_workbook_content)

    def parse_workbook_content(self, response):
        """Parse workbook content.

        Excel file contains 3 tables: berthed table, arrival table and eta table in that order.
        Both arrival and berthed tables use the same headers.

        Args:
            response (scrapy.HtmlResponse):

        Returns:
            dict[str, str]:

        """
        sheet = parser.get_xlsx_sheet(response)

        for idx, row in enumerate(sheet.get_rows()):
            row = [
                xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                if is_xldate(cell)
                else str(cell.value)
                for cell in row
            ]

            # flag to indicate extraction of arrival table
            if not self.arrival_table and 'No' in row[0]:
                self.arrival_table = True
                self.berthed_table = False
                self.eta_table = False
                self.arrival_header = row
                continue

            # flag to indicate extraction of berthed table
            if not self.berthed_table and 'NAME OF SHIP' in row[2]:
                self.arrival_table = False
                self.berthed_table = True
                self.eta_table = False
                self.berthed_header = row
                continue

            # flag to indicate extraction of eta table
            if not self.eta_table and 'EXPECTED VESSEL' in row[2]:
                self.arrival_table = False
                self.berthed_table = False
                self.eta_table = True
                # eta table does not have headers; they are derived from arrival table
                self.eta_header = self.arrival_header
                continue

            # third table row contains `reported_date`
            if idx == 2:
                reported_date = parser._extract_reported_date(row[0])
                continue

            # extract berthed rows, berthed rows have a value for the 8th cell
            if self.berthed_table and row[1] and 'THIS MAY ALTER' not in row[1]:
                # vessels that are 'ships to follow' are processed in the other tables
                if 'AT BERTH' not in row[1]:
                    continue
                raw_item = parser._map_row_to_dict(
                    row,
                    self.berthed_header,
                    event='berthed',
                    port_name=self.provider,
                    provider_name=self.provider,
                    reported_date=reported_date,
                )
                yield normalize.process_item(raw_item)

            # extract eta rows, eta rows have a value for the 4th cell
            if self.eta_table and row[3]:
                raw_item = parser._map_row_to_dict(
                    row,
                    self.eta_header,
                    event='eta',
                    port_name=self.provider,
                    provider_name=self.provider,
                    reported_date=reported_date,
                )
                yield normalize.process_item(raw_item)

            # extract arrival rows, arrival rows have a value for the 4th cell
            if self.arrival_table and row[3]:
                raw_item = parser._map_row_to_dict(
                    row,
                    self.arrival_header,
                    event='arrival',
                    port_name=self.provider,
                    provider_name=self.provider,
                    reported_date=reported_date,
                )
                yield normalize.process_item(raw_item)
