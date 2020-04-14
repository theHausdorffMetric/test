from unittest import TestCase

from kp_scrapers.spiders.agents.banchero_italy.normalize_milazzo import (
    normalize_date_time,
    process_item,
    split_cargo_volume,
)


class BancheroItalyMilazzoTestCase(TestCase):
    def test_process_item(self):
        raw = {
            'vessel': 'DIMITRIS P.',
            'eta': '05/03/2020 18:00',
            'etb': 'N/A',
            'ets': 'N/A',
            'berth': '',
            'movement': 'D',
            'product': ' CRUDEOIL',
            'reported_date': '2020-02-28T12:21:35',
            'provider_name': 'Banchero',
            'port_name': 'Milazzo',
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
                'berth': '',
                'cargoes': None,
                'departure': None,
                'eta': '2020-03-05T18:00:00',
                'installation': None,
                'port_name': 'Milazzo',
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
                    'name': 'DIMITRIS P.',
                    'type': None,
                    'vessel_type': None,
                },
                'cargo': {
                    'movement': 'discharge',
                    'product': ' CRUDEOIL',
                    'volume': None,
                    'volume_unit': None,
                    'buyer': None,
                    'seller': None,
                },
            },
            item,
        )

    def test_normalize_date_time(self):
        self.assertEqual(normalize_date_time('21/11/2019 03:00'), '2019-11-21T03:00:00')
        self.assertEqual(normalize_date_time('N/A'), None)

    def test_split_cargo_volume(self):
        self.assertEqual(split_cargo_volume('GASOIL/UNL'), ['GASOIL', 'UNL'])
