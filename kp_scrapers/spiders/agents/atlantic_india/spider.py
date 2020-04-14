import datetime as dt

from scrapy.spiders import Request, Spider

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.atlantic_india.parser import (
    ASLiquidExcelExtractor,
    extract_reported_date,
    parse_expected_vessels,
    parse_vessel_movement,
)
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


# Order of table column names in the pdf table
ROW_HEADERS = [
    'port',
    'jetty',
    'm_vessel',
    'm_movement',
    'm_cargo',
    'm_qty',
    'm_arrived',
    'm_berthed',
    'm_sailed',
    'm_npoc',
    'm_remark',
    'e_vessel',
    'e_movement',
    'e_cargo',
    'e_qty',
    'e_eta',
]


class AtlanticSpider(ShipAgentMixin, PdfSpider, MailSpider):
    """Parse Liquid West/East Coast from Email

    This portion is maintained to process dry coast and serve
    as a backup for liquid

    """

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    # '0,0,825,580' is the boundary which ignores the bottom of the page
    # (page num and other irrelevant information)
    tabula_options = {'--lattice': [], '--pages': ['all'], '--area': ['0,0,825,580']}

    reported_date_regex = r'\d+\.\d+\.\d+'

    def parse_mail(self, mail):
        """Parse each email that was matched with the spider filter arguments.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        for attachment in mail.attachments():
            # disregard irrelevant attachments
            if not any(name in attachment.name.lower() for name in self.allowed_file_name):
                continue

            # memoise reported date so it won't have to be called repeatedly for each raw item
            reported_date = extract_reported_date(
                mail.envelope['subject'], self.reported_date_regex, mail.envelope['date']
            )

            if attachment.is_pdf:
                yield from self.parse_pdf(attachment.body, reported_date)
            if attachment.is_spreadsheet:
                yield from self.parse_spreadsheet(attachment.body, reported_date)

    def parse_pdf(self, body, reported_date):
        raw_rows = self.extract_pdf_io(body, **self.tabula_options)
        return self.parse_rows(raw_rows, reported_date)

    def parse_spreadsheet(self, body, reported_date):
        ie_xl = ASLiquidExcelExtractor(body, '', dt.datetime.now())
        raw_rows = [x for x in list(ie_xl.parse_sheets()) if x]
        return self.parse_rows(raw_rows, reported_date)

    def parse_rows(self, raw_rows, reported_date):
        current_port = None
        row_headers = ROW_HEADERS.copy()

        # reports have inconsistent headers
        if 'NPOC' not in raw_rows[1] + raw_rows[2]:
            row_headers.remove('m_npoc')
        if 'Remark' not in raw_rows[1] + raw_rows[2]:
            row_headers.remove('m_remark')

        # convert raw rows to a raw dict item
        for idx, raw_row in enumerate(raw_rows):
            # skip table headers and irrelevant rows(they have fewer number of cells)
            if idx <= 2 or len(raw_row) < len(row_headers):
                continue

            row = map_row_to_dict(raw_row, row_headers)
            current_port = row['port'] or current_port

            # report has 2 tables side by side
            yield from parse_expected_vessels(row, reported_date, current_port, self.provider)
            yield from parse_vessel_movement(row, reported_date, current_port, self.provider)


class AtlanticLiquidSpider(AtlanticSpider):
    name = 'AS_India_Grades'
    allowed_file_name = ['west coast', 'east coast']
    produces = [DataTypes.Vessel, DataTypes.Cargo, DataTypes.PortCall]


class AtlanticDrySpider(AtlanticSpider):
    name = 'AS_DryCoast'
    allowed_file_name = ['dry cargo', 'wci', 'eci']
    produces = [DataTypes.Vessel, DataTypes.Cargo, DataTypes.PortCall]


class AtlanticWebSpider(ShipAgentMixin, Spider):
    """Parse Liquid West/East Coast from Website

    Website provides the same report that is sent through
    via email.

    """

    name = 'AS_IndiaAgent_Grades'
    provider = 'Atlantic Shipping'
    version = '1.0.0'
    produces = [DataTypes.Vessel, DataTypes.Cargo, DataTypes.PortCall]

    start_urls = [
        'http://www.atlanticshpg.com/wp-content/uploads/{}/{}/West-Coast-{}.htm',
        'http://www.atlanticshpg.com/wp-content/uploads/{}/{}/East-Coast-{}.htm',
    ]

    def __init__(self, raw_reported_date=None):
        """Init AS_IndiaAgent_Grades

        allow reported date argument to be keyed in manually


        Args:
            reported_date (str): date of report to obtain, formatted as DD.MM.YYYY
        """
        if raw_reported_date:
            self.reported_date = raw_reported_date
        else:
            self.reported_date = dt.datetime.utcnow().strftime('%d.%m.%Y')

    def start_requests(self):
        reported_date = self.reported_date
        _, month, year = self.reported_date.split('.')

        for start_url in self.start_urls:
            yield Request(url=start_url.format(year, month, reported_date), callback=self.parse)

    def parse(self, response):
        table = response.xpath('//table//tr')
        return self.parse_rows(table, self.reported_date)

    def parse_rows(self, table_report, reported_date):
        current_port = None
        row_headers = ROW_HEADERS.copy()

        for idx, raw_row in enumerate(table_report):
            row = [
                may_strip(''.join(cell.xpath('.//text()').extract()))
                for cell in raw_row.xpath('.//td')
                if ''.join(cell.xpath('.//text()').extract())
            ]

            # skip table headers and irrelevant rows(they have fewer number of cells)
            if idx <= 2 or len(row) < len(row_headers):
                continue

            row = map_row_to_dict(row, row_headers)
            current_port = row['port'] or current_port

            # report has 2 tables side by side
            yield from parse_expected_vessels(row, reported_date, current_port, self.provider)
            yield from parse_vessel_movement(row, reported_date, current_port, self.provider)
