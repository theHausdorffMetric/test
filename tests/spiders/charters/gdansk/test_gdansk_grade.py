from unittest import TestCase

from kp_scrapers.spiders.charters.pf_gdansk.normalize_grades import (
    _get_lay_can_year,
    process_item,
    search_cargo_details,
    search_eta,
    search_etb,
    search_etd,
    search_vessel_name,
)


class GdanskGradeTestCase(TestCase):
    def test_process_item(self):
        raw_item = {
            'raw_string': 'm/t “ fulmar “ - e t a gdansk 06th march 2020 at 16:00 hrs wsnp|approx. 120,000 mts of arabian light crude oil from sidi kerir|a / c : pkn “ orlen “ s.a.',  # noqa
            'provider_name': 'Polfract',
            'reported_date': '02 Mar 2020',
            'port_name': 'Gdansk',
            'cargo_movement': 'discharge',
        }
        item = process_item(raw_item)
        for col in (
            'kp_package_version',
            'kp_source_version',
            'kp_uuid',
            'sh_item_time',
            'sh_spider_name',
            'sh_job_id',
            'sh_job_time',
            'kp_source_provider',
            '_type',
        ):
            item.pop(col, None)
        self.assertEqual(
            {
                'provider_name': 'Polfract',
                'arrival': None,
                'berthed': None,
                'berth': None,
                'cargoes': None,
                'departure': None,
                'eta': '2020-03-06T16:00:00',
                'installation': None,
                'port_name': 'Gdansk',
                'next_zone': None,
                'reported_date': '2020-03-02T00:00:00',
                'shipping_agent': None,
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
                    'name': 'fulmar',
                    'type': None,
                    'vessel_type': None,
                },
                'cargo': {
                    'movement': 'discharge',
                    'product': 'arabian light crude oil',
                    'volume': '120000',
                    'volume_unit': 'tons',
                    'buyer': None,
                    'seller': None,
                },
            },
            item,
        )

    def test_search_vessel_name(self):
        self.assertEqual(search_vessel_name('m/t \' seaoath \' - '), {'name': 'seaoath'})

    def test_search_cargo_details(self):
        self.assertEqual(
            search_cargo_details(
                'approx. 120,000 mts of arabian light crude oil from sidi kerir|', 'load'
            ),
            {
                'product': 'arabian light crude oil',
                'volume': '120000',
                'volume_unit': 'tons',
                'movement': 'load',
            },
            'load',
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
