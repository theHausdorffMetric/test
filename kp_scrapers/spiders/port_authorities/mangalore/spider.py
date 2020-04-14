import datetime as dt

from scrapy.spiders import Request

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.mangalore import normalize


BERTHED_HEADERS = [
    'B.NO.',
    'NAME OF VESSEL',
    'IND/',
    'LOA',
    'ARRIVAL',
    'BERTHING',
    'CARGO',
    'AGENT',
    'RECEIVER',
    '_redundant',
    'DAY',
    'TOTAL',
    'BALANCE',
    'ETD',
]


class MangaloreSpider(PortAuthoritySpider, PdfSpider):

    name = 'Mangalore'
    provider = 'Mangalore'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]
    start_url = 'http://newmangaloreport.gov.in:8080/daily-vessels/english/{}.pdf'

    tabula_options = {'--pages': ['all'], '--lattice': []}

    reported_date = dt.datetime.utcnow().strftime('%-d-%-m-%Y')

    def start_requests(self):
        yield Request(url=self.start_url.format(self.reported_date), callback=self.parse)

    def parse(self, response):

        table = self.extract_pdf_io(response.body, **self.tabula_options)

        for raw_row in table:
            row = [may_strip(cell).upper() for cell in raw_row]

            if 'NAME OF VESSEL' in row or 'NAME OF THE VESSEL' in row:
                if 'DATE OF' in row:
                    header = BERTHED_HEADERS
                else:
                    header = row
                continue

            if '-' in row[1] or row[1] == '':
                continue

            raw_item = {may_strip(header[idx]): cell for idx, cell in enumerate(row)}
            raw_item.update(self.meta_field)
            yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {
            'port_name': self.name,
            'provider_name': self.provider,
            'reported_date': to_isoformat(self.reported_date, dayfirst=True),
        }
