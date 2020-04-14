from scrapy import Spider

from kp_scrapers.spiders.bases.markers import KplerMixin


class BunkerFuelCostSpider(KplerMixin, Spider):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:bunker_fuel_cost']}

    @classmethod
    def category(cls):
        return 'bunker_fuel_cost'
