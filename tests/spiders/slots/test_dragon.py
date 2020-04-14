import nose.tools as nt
from scrapy.http import HtmlResponse

from kp_scrapers.spiders.slots.dragon import SlotDragonSpider
from tests._helpers.mocks import fixtures_path


RESULTS_TABLE = [
    {'date': '2015-08-07', 'installation_id': 3512, 'seller': 'BG'},
    {'date': '2015-08-10', 'installation_id': 3512, 'seller': 'BG'},
    {'date': '2015-08-14', 'installation_id': 3512, 'seller': 'Petronas'},
]


def test_slot_dragon():
    fixtures = fixtures_path('slot', 'dragonlng.html')
    spider = SlotDragonSpider()
    with open(fixtures) as f:
        response = HtmlResponse(
            SlotDragonSpider.start_urls[0], status=200, body=f.read(), encoding='utf-8'
        )
    computed = list(spider.parse(response))

    for i, expected in enumerate(RESULTS_TABLE):
        for k in expected.keys():
            nt.assert_equal(expected[k], computed[i][k])
