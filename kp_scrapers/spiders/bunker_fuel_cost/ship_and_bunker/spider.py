import datetime as dt

from six.moves import zip

from kp_scrapers.models.bunker_fuel_cost import BunkerFuelCost
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.bunker_fuel_cost import BunkerFuelCostSpider


# fuels we are interested in at the moment
BUNKER_TYPES = ['IFO380', 'MGO']
# zone we are interested in at the moment
DESIRED_AREA = 'Global 4 Ports Average'


class ShipAndBunkerSpider(BunkerFuelCostSpider):
    """Scrape bunker fuel oil prices.

    It is used by the voyage-calculator to estimate the total fuel cost during a
    voyage.  There are two types of fuel, IF0380 and MGO. Due to regulation, vessels
    have to use one or the other, depending on the water they sails.  We use the
    price of "Global 4 Ports Average" as an average of the world bunker price.
    """

    name = 'BunkerFuelCost'
    provider = 'Ship and Bunker'
    version = '1.1.0'
    produces = [DataTypes.BunkerFuelCost]

    start_urls = ['http://shipandbunker.com/prices']

    @validate_item(BunkerFuelCost, normalize=True, strict=True, log_level='error')
    def parse(self, response):
        reported_date = dt.datetime.utcnow().isoformat()

        for bunker_type in BUNKER_TYPES:
            zone = response.xpath(
                f'//*[@id="_{bunker_type}"]/table/tbody//tr//th//text()'
            ).extract()
            price = response.xpath(
                f'//*[@id="_{bunker_type}"]/table/tbody//tr//td[1]//text()'
            ).extract()

            price_by_zone = dict(list(zip(zone, price)))

            raw_item = {
                'fuel_type': bunker_type,
                'price': price_by_zone.get(DESIRED_AREA),
                'zone': DESIRED_AREA,
                'reported_date': reported_date,
                'provider_name': self.provider,
            }

            yield raw_item
