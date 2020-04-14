# -*- coding: utf-8; -*-

from __future__ import absolute_import, print_function, unicode_literals
import datetime as dt


# import nose.tools as nt
# from scrapy.http import HtmlResponse
# import six

# from kp_scrapers.spiders.slots.gate import SlotGateSpider
# from tests._helpers.mocks import fixtures_path


def _result_row(month, day):
    return {'date': str(dt.datetime(2016, month, day, 0, 0)), 'installation_id': '68', 'seller': ''}


# def test_slot_gate():
#     fixture_path = fixtures_path('slot', 'gate.html')
#     spider = SlotGateSpider()
#     with open(fixture_path) as f:
#         response = HtmlResponse(
#             'http://gate.nl/en/commercial/services-gate.html',
#             status=200,
#             body=f.read(),
#             encoding='utf-8',
#         )
#     results = [_result_row(1, i) for i in [3, 5, 7, 9, 11, 13, 15, 17, 22, 24, 26, 28]]
#     computed = list(spider.parse(response))
#     print(computed)
#     for k, e in enumerate(results):
#         for rk, rv in six.iteritems(e):
#             # print rv, computed[k][rk]
#             nt.assert_equal(rv, computed[k][rk])


# def test_slot_gate2():
#     fixture_path = fixtures_path('slot', 'gate2.html')
#     spider = SlotGateSpider()
#     with open(fixture_path) as f:
#         response = HtmlResponse(
#             'http://gate.nl/en/commercial/services-gate.html',
#             status=200,
#             body=f.read(),
#             encoding='utf-8',
#         )
#     results = [_result_row(1, i) for i in [1, 2, 3, 13, 15, 19, 24, 28]] + [
#         _result_row(4, i) for i in [4, 10, 11, 12]
#     ]
#     computed = list(spider.parse(response))
#     print(computed)
#     for k, e in enumerate(results):
#         for rk, rv in six.iteritems(e):
#             # print rv, computed[k][rk]
#             nt.assert_equal(rv, computed[k][rk])
