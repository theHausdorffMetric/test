from scrapy import Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.puerto_moin import normalize


class PuertoMoinSpider(PortAuthoritySpider, Spider):
    name = 'PuertoMoin'
    provider = 'JAPDEVA'
    version = '0.2.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://servicios.japdeva.go.cr/siopj/programasemanal/consulta']

    def parse(self, response):
        """Parse port activity pages.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        # memoise reported date so that we don't need to call it repeatedly below
        reported_date = response.xpath('//strong/parent::*/text()').extract()[1]

        header = response.xpath('//table//th/text()').extract()
        # first cell of table row will always be empty as displayed on the website
        # hence, we append an "irrelevant" column as the first element of the header
        header.insert(0, 'irrelevant')
        for row in response.xpath('//table/tbody/tr'):
            raw_item = row_to_dict(row, header)
            # contextualise raw item with metadata
            raw_item.update(
                port_name='Puerto Moin', provider_name=self.provider, reported_date=reported_date
            )
            yield normalize.process_item(raw_item)
