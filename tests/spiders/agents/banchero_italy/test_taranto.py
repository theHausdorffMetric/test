import datetime as dt
from unittest import TestCase

from kp_scrapers.spiders.agents.banchero_italy.normalize_taranto import (
    normalize_date_time,
    normalize_time,
    process_item,
    split_cargo_volume,
)


class BancheroItalyTarantoTestCase(TestCase):
    def test_process_item(self):
        raw = {
            'VESSEL': 'SEAEMPRESS',
            'LAST PORT': 'AUGUSTA',
            'EXPECTED PIER': 'ENI',
            'ETA': '28/1200',
            'ETB': 'TBC',
            'ETC/ETS': 'TBC',
            'OPS': 'LOADING',
            'CARGO': 'LSFO',
            'QUANTITY': '30.000',
            'NEXT PORT': 'TBN',
            'AGENT': 'NAVALSUD',
            'reported_date': '2020-02-28T12:21:35',
            'provider_name': 'Banchero',
            'port_name': 'taranto',
        }

        item = list(process_item(raw))[0]
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
                'provider_name': 'Banchero',
                'arrival': None,
                'berthed': None,
                'berth': None,
                'cargoes': None,
                'departure': None,
                'eta': '2020-02-28T12:00:00',
                'installation': None,
                'port_name': 'taranto',
                'next_zone': None,
                'reported_date': '2020-02-28T12:21:35',
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
                    'name': 'SEAEMPRESS',
                    'type': None,
                    'vessel_type': None,
                },
                'cargo': {
                    'movement': 'load',
                    'product': 'LSFO',
                    'volume': '30000',
                    'volume_unit': 'tons',
                    'buyer': None,
                    'seller': None,
                },
            },
            item,
        )

    def test_normalize_date_time(self):
        '''Dates do not contain the year, for example 10700(dhhmm).The reported
        date is required to retrieve or guess the year
        '''
        # handle normal cases
        self.assertEqual(normalize_date_time('10700', '2019-07-03T10:01:00'), '2019-07-01T07:00:00')
        self.assertEqual(
            normalize_date_time('110700', '2019-07-03T10:01:00'), '2019-07-11T07:00:00'
        )
        # handle rollover months or years
        self.assertEqual(normalize_date_time('10700', '2019-12-30T10:01:00'), '2020-01-01T07:00:00')
        self.assertEqual(
            normalize_date_time('310700', '2019-01-01T10:01:00'), '2019-12-31T07:00:00'
        )
        self.assertEqual(
            normalize_date_time('310700', '2019-02-01T10:01:00'), '2019-01-31T07:00:00'
        )
        self.assertEqual(
            normalize_date_time('310700', '2019-02-01T10:01:00'), '2019-01-31T07:00:00'
        )
        # handle incorrect cases
        self.assertEqual(normalize_date_time('abc', '2019-04-01T10:01:00'), None)

    def test_normalize_time(self):
        '''sub function of test_normalize_date_time to obtain datetime object for time portion'''
        self.assertEqual(normalize_time('0407'), dt.time(4, 7))
        self.assertEqual(normalize_time(None), dt.time(0, 0))

    def test_split_cargo_volume(self):
        self.assertEqual(
            split_cargo_volume('GASOIL/UNL', '18000'), [('GASOIL', '9000.0'), ('UNL', '9000.0')]
        )
        self.assertEqual(
            split_cargo_volume('GASOIL+UNL', '18000'), [('GASOIL', '9000.0'), ('UNL', '9000.0')]
        )
