from unittest import TestCase

from kp_scrapers.spiders.charters.pf_gdansk.normalize_charters import (
    _get_lay_can_year,
    search_cargo_details,
    search_charterer,
    search_eta,
    search_etb,
    search_etd,
    search_vessel_name,
)


class GdanskCharterTestCase(TestCase):
    """
        raw_item
        {
            'raw_string': 'm/t “ anavatos ii “ - under discharging - jetty no.“ p “|approx. 100,000 mts of rebco from primorsk|e t c 02nd march at 15:00 hrs , e t s at 21:00 hrs|out : ballast ust-luga|a / c : petraco',  # noqa
            'provider_name': 'Polfract',
            'reported_date': '02 Mar 2020',
            'port_name': 'Gdansk',
            'cargo_movement': 'discharge',
        }
        processed_item
        {
            'provider_name': 'Polfract',
            'arrival_zone': ['Gdansk'],
            'cargo': {
                'movement': 'load',
                'product': 'rebco',
                'volume': '100000',
                'volume_unit': 'tons',
                'buyer': None,
                'seller': None,
            },
            'charterer': None,
            'departure_zone': 'Primorsk',
            'lay_can_end': '2020-02-29T06:21:00',
            'lay_can_start': '2020-02-27T06:21:00',
            'rate_raw_value': None,
            'rate_value': None,
            'reported_date': '02 Mar 2020',
            'seller': None,
            'status': None,
            'vessel': {
                'beam': None,
                'build_year': None,
                'call_sign': None,
                'dead_weight': None,
                'dwt': None,
                'flag_code': None,
                'flag_name': None,
                'gross_tonnage': None,
                'imo': None,
                'length': None,
                'mmsi': None,
                'name': 'anavatos ii',
                'type': None,
                'vessel_type': None,
            },
        }
    """

    def test_search_vessel_name(self):
        self.assertEqual(search_vessel_name('m/t " seaoath " - '), {'name': 'seaoath'})

    def test_search_charterer(self):
        self.assertEqual(
            search_charterer('a / c : pkn “ orlen “ s.a.|cargo receiver : plock refiner'),
            'pkn orlen s a',
        )

    def test_search_cargo_details(self):
        self.assertEqual(
            search_cargo_details('approx. 120,000 mts of arabian light crude oil from sidi kerir|'),
            {
                'product': 'arabian light crude oil',
                'volume': '120000',
                'volume_unit': 'tons',
                'movement': 'load',
            },
        )

    def test_search_eta(self):
        self.assertEqual(
            search_eta(' - e t a gdansk or butinge 06th march 2020|', '2020-03-10T00:00:00'),
            '2020-03-06T00:00:00',
        )

    def test_search_etb(self):
        self.assertEqual(
            search_etb('e t b 29th february 01:00 hrs', '2020-03-10T00:00:00'),
            '2020-02-29T01:00:00',
        )

    def test_search_etd(self):
        self.assertEqual(
            search_etd('e t s 24th february at 21:00 hrs after bunkering', '2020-03-10T00:00:00'),
            '2020-02-24T21:00:00',
        )

    def test__get_lay_can_year(self):
        self.assertEqual(_get_lay_can_year('12', '10 Jan 2020'), 2019)
