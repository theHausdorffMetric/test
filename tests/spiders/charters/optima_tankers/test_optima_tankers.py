from unittest import TestCase

from kp_scrapers.spiders.charters.optima_tankers.normalize import (
    normalize_charterer_status,
    normalize_vessel_status,
    normalize_voyage,
    process_item,
)


input_raw_items = [
    {
        'vessel_status': 'JAG PRAKASH',
        'cargo_volume': '35',
        'cargo_product': 'CPP',
        'lay_can': '03/MAR',
        'voyage': 'SIKKA/HAZIRA-JNPT',
        'rate_value': 'USD255K',
        'charterer': 'RELIANCE',
        'provider_name': 'Optima Tankers',
        'reported_date': '28 Feb 2020',
        'mail_title': 'FW: OPTIMA TANKERS-CLN FIXTURE REPORT (REF:20078IW)',
    },
    {
        'vessel': 'ENERGY CHAMPION',
        'cargo_volume': '50',
        'lay_can': '1/03',
        'voyage': 'ECMEX/USG',
        'rate_value': 'WS195',
        'charterer_status': 'HUNT',
        'provider_name': 'Optima Tankers',
        'reported_date': '28 Feb 2020',
        'mail_title': 'Fwd: FW: OPTIMA TANKERS-DPP FIXTURE REPORT (REF:2007983)',
    },
]
output_raw_items = [
    {
        'provider_name': 'Optima Tankers',
        'arrival_zone': ['HAZIRA', 'JNPT'],
        'cargo': {
            'movement': 'load',
            'product': 'CPP',
            'volume': '35',
            'volume_unit': 'kilotons',
            'buyer': None,
            'seller': None,
        },
        'charterer': 'RELIANCE',
        'departure_zone': 'SIKKA',
        'lay_can_end': '2020-03-03T00:00:00',
        'lay_can_start': '2020-03-03T00:00:00',
        'rate_raw_value': None,
        'rate_value': 'USD255K',
        'reported_date': '28 Feb 2020',
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
            'name': 'JAG PRAKASH',
            'type': None,
            'vessel_type': None,
        },
    },
    {
        'provider_name': 'Optima Tankers',
        'arrival_zone': ['USG'],
        'cargo': {
            'movement': 'load',
            'product': 'Dirty',
            'volume': '50',
            'volume_unit': 'kilotons',
            'buyer': None,
            'seller': None,
        },
        'charterer': 'HUNT',
        'departure_zone': 'ECMEX',
        'lay_can_end': '2020-03-01T00:00:00',
        'lay_can_start': '2020-03-01T00:00:00',
        'rate_raw_value': None,
        'rate_value': 'WS195',
        'reported_date': '28 Feb 2020',
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
            'name': 'ENERGY CHAMPION',
            'type': None,
            'vessel_type': None,
        },
    },
]


class OptimaTankersTestCase(TestCase):
    def test_process_item(self):
        for idx, raw_item in enumerate(input_raw_items):
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
            self.assertEqual(output_raw_items[idx], item)

    def test_normalize_vessel_status(self):
        self.assertEqual(normalize_vessel_status('SEALING(FLD)'), ('SEALING', 'Failed'))
        self.assertEqual(normalize_vessel_status('SEALING'), ('SEALING', None))
        self.assertEqual(normalize_vessel_status('ENI TBN'), (None, None))

    def test_normalize_charterer_status(self):
        self.assertEqual(normalize_charterer_status('PETCO - UPDATE'), ('PETCO', None))
        self.assertEqual(normalize_charterer_status('PETCO - FAILED'), ('PETCO', 'Failed'))
        self.assertEqual(normalize_charterer_status('ENI TBN'), ('ENI TBN', None))

    def test_normalize_voyage(self):
        self.assertEqual(
            normalize_voyage('AIN SUKHNA/MED – SPORE'), ('AIN SUKHNA', ['MED ', ' SPORE'])
        )
        self.assertEqual(normalize_voyage('AIN SUKHNA/OPTS'), ('AIN SUKHNA', None))
