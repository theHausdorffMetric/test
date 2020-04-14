# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from unittest import TestCase

from scrapy.http import HtmlResponse, Request
from scrapy.selector import Selector

from kp_scrapers.models.items import EtaEvent
from kp_scrapers.spiders.port_authorities.rio import RioSpider
from tests._helpers.mocks import VESSEL_LIST


LPG_LINE = u"""
<header>
<meta charset="UTF-8">
</header>
<body>
<tr class="even">
<td>Rio</td>
<td>11/08/2017</td>
<td>05229/2017</td>
<td>MOYRA</td>
<td>BERGESEN DO BRASIL PARTICIPAÇÕES LTDA</td>
<td>TANKER</td>
</tr>
</body>"""

OIL_LINE = u"""
<header>
<meta charset="UTF-8">
</header>
<tr class="even">
<td>Rio</td>
<td>11/08/2017</td>
<td>05229/2017</td>
<td>MOYRA</td>
<td>BERGESEN DO BRASIL PARTICIPAÇÕES LTDA</td>
<td>PETROLEIRO</td>
</tr>
</body>"""


class RioTestCase(TestCase):
    def test_lpg_line_returns_lpg_event(self):
        meta = {
            "vessels": [
                {'name': vessel['name'], 'commos': vessel['_markets']}
                for vessel in VESSEL_LIST
                if vessel['name'] == 'moyra'
            ]
        }
        request = Request(url='http://www.portosrio.gov.br/prognav/navios_esperados', meta=meta)
        response = HtmlResponse(request.url, request=request, body=LPG_LINE, encoding='utf8')

        events = RioSpider().extract_events(response, Selector(response))
        events = list(events)

        self.assertTrue(all(type(event) is EtaEvent for event in events))

        self.assertTrue('lng' not in [event.get('cargo', {}).get('commodity') for event in events])
        self.assertTrue('lpg' in [event.get('cargo', {}).get('commodity') for event in events])
        self.assertTrue('oil' not in [event.get('cargo', {}).get('commodity') for event in events])

    def test_oil_line_returns_oil_event_even_if_lpg_vessel_matches(self):
        meta = {
            "vessels": [
                {'name': vessel['name'], 'commos': vessel['_markets']}
                for vessel in VESSEL_LIST
                if vessel['name'] == 'moyra'
            ]
        }
        request = Request(url='http://www.portosrio.gov.br/prognav/navios_esperados', meta=meta)
        response = HtmlResponse(request.url, request=request, body=OIL_LINE, encoding='utf8')

        events = RioSpider().extract_events(response, Selector(response))
        events = list(events)

        self.assertTrue('lng' not in [event.get('cargo', {}).get('commodity') for event in events])
        self.assertTrue('lpg' not in [event.get('cargo', {}).get('commodity') for event in events])
        self.assertTrue('oil' in [event.get('cargo', {}).get('commodity') for event in events])
