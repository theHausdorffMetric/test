import datetime as dt
import json

from scrapy import Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.saint_john import normalize


class SaintJohnSpider(PortAuthoritySpider, Spider):
    name = 'SaintJohn'
    provider = 'Saint John'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        # TODO we don't scrape in-port vessels for now since they don't bring much value ...
        # 'https://www.sjport.com/current_vessels.php',
        'https://www.sjport.com/expected_vessels.php'
    ]

    port_name = 'Saint John'

    reported_date = (
        dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
    )

    def parse(self, response):
        """Entry point for Saint John spider.

        Examples of a json response:

         -  current_vessels
            {
                "u'BERTH'": "u'ANCH'",
                "u'SHIP_LINE'": "u'Charter'",
                "u'DATE_OF_ARRIVAL'": "u'2017-01-08'",
                "u'AGENT'": "u'Furncan Marine-Agency Division'",
                "u'VESSEL_NAME'": "u'British Cormorant'",
                "u'CARGO_ACTIVITY'": "u'Crude Oil from Foreign Ports'"
            }

         -  expected_vessels
            {
                "u'BERTH'": "u'Pier 12A'",
                "u'SHIP_LINE'": "u'Charter'",
                "u'AGENT'": "u'Protos Shipping Limited'",
                "u'VESSEL_NAME'": "u'Ginga Tiger'",
                "u'ETA'": "u'2017-01-12'",
                "u'CARGO_ACTIVITY'": "u'Molasses for Foreign Ports'"
            }

        Args:
            scrapy.Response:

        Returns:
            Dict[str, Any]:

        """
        json_response = json.loads(response.body_as_unicode())
        for raw_item in json_response:
            # contextualise raw item with meta info
            raw_item.update(self.meta_field)
            yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {
            'port_name': self.port_name,
            'provider_name': self.provider,
            'reported_date': self.reported_date,
        }
