import json

from scrapy import Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.teesside import normalize


class TeessideSpider(PortAuthoritySpider, Spider):
    name = 'Teesside'
    provider = 'PDPorts'
    version = '1.1.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel]

    start_urls = ['https://www.pdports.co.uk/marine-information/shipping-reports/']

    def parse(self, response):
        """Parse table data from website.

        Website uses Vue to format table, so the actual data is contained within a hidden tag.
        This is the data structure after deserializing:

        {
            'shipping_movements_next_7': List[Dict[str, str],
            'shipping_movements_next_24': List[Dict[str, str]],
            'ships_in_port': List[Dict[str, str]],
            'ships_at_anchor': List[Dict[str, str]],
            'ships_entering_last_24': List[Dict[str, str]],
            'ships_entering_last_3': List[Dict[str, str]],
            'ships_entering_last_7': List[Dict[str, str]],
            'ships_departing_last_24': List[Dict[str, str]],
            'ships_departing_last_3': List[Dict[str, str]],
            'ships_departing_last_7': List[Dict[str, str]],
            '0': Dict[str, int] (contains reported date in unix time)
        }

        Note that the reported date is contained within the '0' key.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        data = json.loads(response.xpath('//shipping-report/@*').extract_first())
        for report_type in data:
            # we only want shipping schedule for the next 7 days, according to analysts
            if report_type != 'shipping_movements_next_7':
                continue

            # reported date is contained within the '0' key
            reported_date = data['0'][f'date_{report_type}']
            for raw_item in data[report_type]['ships']:
                # contextualise raw item with meta info
                raw_item.update(
                    port_name=self.name, provider_name=self.provider, reported_date=reported_date
                )
                yield normalize.process_item(raw_item)
