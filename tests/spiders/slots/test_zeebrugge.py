import unittest

from scrapy.http import HtmlResponse

from kp_scrapers.spiders.slots.zeebrugge import ZeebruggeSpider
from tests._helpers.mocks import fixtures_path


def _trim_item(item, *fields_to_keep):
    return {k: v for k, v in item.items() if k in fields_to_keep}


class TestZeebruggeSpider(unittest.TestCase):
    def setUp(self):
        self.spider = ZeebruggeSpider()
        self.start_url = self.spider.start_urls[0]

    def test_zeebrugge_spider(self):
        # given
        with open(fixtures_path('slot', 'zeebrugge.html')) as source:
            response = HtmlResponse(
                url=self.start_url, status=200, body=source.read(), encoding='utf-8'
            )

        # when
        results = [
            _trim_item(item, 'date', 'installation_id', 'seller')
            for item in self.spider.parse(response)
        ]

        # then
        self.assertCountEqual(
            results, [{'date': '2019-10-07', 'installation_id': 3430, 'seller': 'FLUXYSLNG'}]
        )
