from unittest import TestCase

from kp_scrapers.spiders.agents.banchero_italy.normalize_savona_multedo import (
    extract_status_date,
    process_item,
)


# this normalization script is for 2 attachments with 2 different ports
raw_item_list_input = [
    {
        'TERMINAL': 'BOE SARPOM',
        'STATUS': 'DEPARTURED',
        'VESSEL': 'NS CONSUL',
        'IMO NUMBER': '9341093',
        'PREVIOUS PORT': 'Altri Porti Canada -Atlantico',
        'NEXT PORT': 'ORDERS',
        'LOAD/DISCHARGE': 'DISCHARGE',
        'GRADE GROUP': 'CRUDE OIL',
        'GRADE DETAIL': 'PRIMEROSE',
        'QUANTITY': '90000',
        'BBL/MT': 'MT',
        'ETA': '24/02/2020 10.50.00',
        'reported_date': '2020-02-28T12:21:35',
        'provider_name': 'Banchero',
        'port_name': 'Vado Ligure (Savona)',
    },
    {
        'TERMINAL': 'MULTEDO OIL TERMINAL',
        'STATUS': 'ETA 01/03/2020',
        'VESSEL': 'THEMSESTERN',
        'IMO NUMBER': '9183843',
        'PREVIOUS PORT': 'ARKHANGELSK',
        'NEXT PORT': 'N/A',
        'LOAD/DISCHARGE': 'DISCHARGE',
        'GRADE DETAIL': 'KEROSENE',
        'QUANTITY': '19.000',
        'BBL/MT': 'MT',
        'CHARTERER': 'N.A.',
        'SUPPLIER': 'ENI',
        'COMMENTS': 'NIL',
        'reported_date': '2020-02-28T12:21:35',
        'provider_name': 'Banchero',
        'port_name': 'Genoa',
    },
]
raw_item_list_output = [
    {
        'provider_name': 'Banchero',
        'arrival': None,
        'berthed': None,
        'berth': None,
        'cargoes': None,
        'departure': None,
        'eta': '2020-02-24T10:50:00',
        'installation': 'BOE SARPOM',
        'port_name': 'Vado Ligure (Savona)',
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
            'imo': '9341093',
            'length': None,
            'mmsi': None,
            'name': 'NS CONSUL',
            'type': None,
            'vessel_type': None,
        },
        'cargo': {
            'movement': 'discharge',
            'product': 'PRIMEROSE',
            'volume': '90000',
            'volume_unit': 'tons',
            'buyer': None,
            'seller': None,
        },
    },
    {
        'provider_name': 'Banchero',
        'arrival': '2020-03-01T00:00:00',
        'berthed': None,
        'berth': None,
        'cargoes': None,
        'departure': None,
        'eta': None,
        'installation': 'MULTEDO OIL TERMINAL',
        'port_name': 'Genoa',
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
            'imo': '9183843',
            'length': None,
            'mmsi': None,
            'name': 'THEMSESTERN',
            'type': None,
            'vessel_type': None,
        },
        'cargo': {
            'movement': 'discharge',
            'product': 'KEROSENE',
            'volume': '19000',
            'volume_unit': 'tons',
            'buyer': {
                'address': None,
                'date_of_effect': None,
                'imo': None,
                'name': 'ENI',
                'role': None,
            },
            'seller': None,
        },
    },
]


class BancheroItalySavonaMultedoGenoaTestCase(TestCase):
    def test_process_item(self):
        for idx, raw_item in enumerate(raw_item_list_input):
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
            print(raw_item_list_output[idx])
            print(item)
            self.assertEqual(raw_item_list_output[idx], item)

    def test_extract_status_date(self):
        """to determine what kind of date it represents in kpler terms and format date string,"""
        self.assertEqual(
            extract_status_date('SAILED 20/02/2020'), ('departure', '2020-02-20T00:00:00')
        )
