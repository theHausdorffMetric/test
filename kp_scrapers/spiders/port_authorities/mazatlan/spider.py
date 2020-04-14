import datetime as dt

from scrapy import Request, Spider

from kp_scrapers.lib.parser import may_strip, row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.mazatlan import normalize


class MazatlanSpider(PortAuthoritySpider, Spider):
    name = 'Mazatlan'
    provider = 'Mazatlan'
    version = '1.1.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        # overview of vessel schedules
        'https://mediport.puertomazatlan.com.mx/cctm/Programados.aspx',
        # details of vessel attributes
        'https://mediport.puertomazatlan.com.mx/cctm/detalleServicio.aspx?&nid=0&viaje={vid}',
    ]

    def start_requests(self):
        yield Request(url=self.start_urls[0], callback=self.parse_overview)

    def parse_overview(self, response):
        raw_header, raw_table = response.xpath(
            '//div[@id="ContenidoForma_WebDataGrid21"]/table/tr/td/table/tbody'
        )[:2]

        header = raw_header.xpath('.//th/text()').extract()
        table = raw_table.xpath('./tr/td//tr')

        for row in table:
            raw_item = row_to_dict(
                row,
                header,
                # contextualise raw item with meta info
                port_name=self.name,
                provider_name=self.provider,
                reported_date=dt.datetime.utcnow()
                .replace(hour=0, minute=0, second=0)
                .isoformat(timespec='seconds'),
            )
            yield Request(
                url=self.start_urls[1].format(vid=raw_item['VID']),
                callback=self.parse_vessel_attributes,
                meta={'raw_item': raw_item},
            )

    def parse_vessel_attributes(self, response):
        table = response.xpath('//table[@id="tabDetalleServicios_tmpl0_DetailsView1"]')
        attributes = table.xpath('.//tr')
        for attr in attributes:
            attr = [may_strip(each) for each in attr.xpath('.//td/text()').extract()]
            _iter = iter(attr)
            response.meta['raw_item'].update(dict(zip(_iter, _iter)))

        return normalize.process_item(response.meta['raw_item'])
