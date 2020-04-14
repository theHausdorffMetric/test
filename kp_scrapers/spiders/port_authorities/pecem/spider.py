from scrapy import Spider

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.pecem import normalize


ETA_SIGN = 'PROGRAMADO'
ETA_HEADER = [
    'Navio',
    'Nº Programação',
    'DUV',
    'ETA',
    'Serviço',
    'Armador',
    'Agência',
    'Mercadoria',
    'Confirmada Atracação',
]
ARRIVAL_HEADER = [
    'Navio',
    'Nº Programação',
    'DUV',
    'ARRIVAL',
    'Serviço',
    'Armador',
    'Agência',
    'Mercadoria',
    'Confirmada Atracação',
]

HEADER_MAPPING = {'ETA': ETA_HEADER, 'ARRIVAL': ARRIVAL_HEADER}

DATA_ROW_LEN = 9


class PecemSpider(PortAuthoritySpider, Spider):
    name = 'Pecem'
    provider = 'Pecem'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        'http://sic-tos.cearaportos.ce.gov.br/sictossite/pesquisa.aspx?WCI=relEmitirLineUpExt_002'
    ]

    reported_date = None

    def parse(self, response):
        """Entry point of Pecem spider.

        Args:
            response (Response):

        Returns:
            PortCall:

        """
        self.reported_date = response.xpath('(//td[@class="rptNormal"]//text())[2]').extract_first()

        sign = 'ETA'
        for raw_row in response.xpath('//tr'):
            row = [
                may_strip(td.xpath('.//text()').extract_first()) for td in raw_row.xpath('.//td')
            ]

            table_name = may_strip(raw_row.xpath('.//th[@colspan="20"]//text()').extract_first())
            if table_name:
                sign = 'ETA' if table_name == ETA_SIGN else 'ARRIVAL'
                print(sign)

            if len(row) == DATA_ROW_LEN:
                raw_item = {header: row[idx] for idx, header in enumerate(HEADER_MAPPING[sign])}
                raw_item.update(self.meta_field)

                yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {
            'port_name': self.name,
            'provider_name': self.provider,
            'reported_date': self.reported_date,
        }
