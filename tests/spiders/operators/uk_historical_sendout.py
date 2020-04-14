# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from unittest import skip
from unittest.mock import Mock, patch

import nose.tools as nt

from kp_scrapers.spiders.operators.uk_historical_sendout import UKOperatorHistoricalSendOutSpider
from tests._helpers.mocks import fixtures_path


RESULTS_TABLE = [
    {'date': '2015-08-26 00:00:00', 'city': 'Grain', 'output_o': 79.54},
    {'date': '2015-08-26 00:00:00', 'city': 'SouthHook', 'output_o': 39.29},
    {'date': '2015-08-26 00:00:00', 'city': 'Dragon', 'output_o': 0.00},
    {'date': '2015-08-25 00:00:00', 'city': 'Grain', 'output_o': 79.54},
    {'date': '2015-08-25 00:00:00', 'city': 'SouthHook', 'output_o': 39.29},
    {'date': '2015-08-25 00:00:00', 'city': 'Dragon', 'output_o': 0.00},
    {'date': '2015-08-24 00:00:00', 'city': 'Grain', 'output_o': 79.54},
    {'date': '2015-08-24 00:00:00', 'city': 'SouthHook', 'output_o': 39.27},
    {'date': '2015-08-24 00:00:00', 'city': 'Dragon', 'output_o': 0.00},
]


@skip('Fixture file "response.csv" does not exist.')
def test_uk_port_operator_sentout():
    fixture_path = fixtures_path('operator', 'uk_sentout', 'response.csv')
    mock_data_path = Mock(return_value="test.json")
    with patch('kp_scrapers.spiders.bases.persist_data_manager.data_path', new=mock_data_path):
        spider = UKOperatorHistoricalSendOutSpider()

    with open(fixture_path) as f:
        body = f.read()
    grouped_results = {}
    for e in RESULTS_TABLE:
        grouped_results.setdefault(e['date'], {}).setdefault(e['city'], e['output_o'])

    spider.parse_csv(body)

    computed = list(spider.parse_csv(body))
    nt.assert_equal(len(computed), len(RESULTS_TABLE))
    for e in computed:
        nt.assert_equal(float(e['output_o']), grouped_results[e['date']][e['city']])
