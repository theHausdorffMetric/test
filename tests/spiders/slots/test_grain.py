import nose.tools as nt
from scrapy.http import HtmlResponse

from kp_scrapers.spiders.slots.grain import SlotGrainSpider
from tests._helpers.mocks import fixtures_path


BASE_URL = SlotGrainSpider.start_urls[0]


def test_slot_grain():
    fixture_path = fixtures_path('slot', 'grain.html')
    spider = SlotGrainSpider()
    with open(fixture_path) as f:
        response = HtmlResponse(BASE_URL, status=200, body=f.read(), encoding='utf-8')
    results = [
        {'date': '2018-07-21T00:00:00', 'installation_id': 3513, 'seller': ''},
        {'date': '2018-07-22T00:00:00', 'installation_id': 3513, 'seller': ''},
        {'date': '2018-07-24T00:00:00', 'installation_id': 3513, 'seller': ''},
        {'date': '2018-07-27T00:00:00', 'installation_id': 3513, 'seller': ''},
        {'date': '2018-07-28T00:00:00', 'installation_id': 3513, 'seller': ''},
        {'date': '2018-07-29T00:00:00', 'installation_id': 3513, 'seller': ''},
    ]

    computed = list(spider.parse(response))
    for k, e in enumerate(results):
        for rk, rv in e.items():
            nt.assert_equal(rv, computed[k][rk])
