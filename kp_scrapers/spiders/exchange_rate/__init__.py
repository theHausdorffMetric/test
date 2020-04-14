from scrapy import Spider

from kp_scrapers.spiders.bases.markers import KplerMixin


class ExchangeRateSpider(KplerMixin, Spider):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:exchange_rate']}

    @classmethod
    def category(cls):
        return 'exchange_rate'
