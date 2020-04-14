from copy import deepcopy
import unittest

from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item
from tests._helpers import TestIO, with_test_cases


def _validate(raw_item, model, normalize, strict):
    @validate_item(model, normalize, strict)
    def _inner(raw_item):
        return raw_item

    return _inner(raw_item)


# TODO use the function from `_helpers` once tailored to `BaseMetaItem`
def _strip_meta_fields(item):
    fields_to_strip = [
        '__version__',
        '_type',
        '_uuid',
        'kp_package_version',
        'kp_source_provider',
        'kp_source_version',
        'kp_uuid',
        'sh_item_time',
        'sh_job_time',
        'sh_job_id',
        'sh_spider_name',
    ]
    return {k: v for k, v in item.items() if k not in fields_to_strip} if item else None


class ItemFactory(object):
    _optional = {
        'cargoes': [
            {
                'movement': 'load',
                'product': 'foobar',
                'volume': None,
                'volume_unit': None,
                'buyer': {
                    'name': 'National Green Tribunal',
                    'imo': None,
                    'address': 'India',
                    'date_of_effect': None,
                    'role': None,
                },
                'seller': {
                    'name': 'A.C.',
                    'imo': None,
                    'address': 'India',
                    'date_of_effect': None,
                    'role': None,
                },
            },
            {
                'movement': 'discharge',
                'product': 'bazqux',
                'volume': '30000',
                'volume_unit': Unit.tons,
                'buyer': {
                    'name': 'IPLOM',
                    'imo': None,
                    'address': 'Malaysia',
                    'date_of_effect': None,
                    'role': None,
                },
                'seller': {
                    'name': 'ENI',
                    'imo': None,
                    'address': 'Singapore',
                    'date_of_effect': None,
                    'role': None,
                },
            },
        ],
        'installation': 'IA',
        'next_zone': 'Indonesia',
        'berth': '18C',
        'shipping_agent': 'Agent',
    }
    _required = {
        'arrival': None,
        'berthed': None,
        'departure': None,
        'eta': '2018-04-05T00:00:00',
        'port_name': 'Oolacile',
        'provider_name': 'DS',
        'reported_date': '2018-03-06T00:00:00',
        'vessel': {
            'build_year': 2016,
            'call_sign': 'FNJI',
            'mmsi': None,
            'dead_weight': 61301,
            'dwt': 61301,
            'gross_tonnage': 125977,
            'length': None,
            'imo': '9774135',
            'name': 'ANOR LONDO',
            'beam': None,
            'length': None,
            'flag_name': None,
            'flag_code': None,
            'type': None,
            'vessel_type': None,
        },
    }

    @classmethod
    def filled_item(cls):
        x = deepcopy(cls._required)
        x.update(cls._optional)
        return x

    @classmethod
    def incomplete_item(cls):
        x = deepcopy(cls._required)
        x.pop('eta')
        return x

    @classmethod
    def invalid_item_vessel(cls):
        x = deepcopy(cls._required)
        for required in ('name', 'imo'):
            x['vessel'][required] = None
        return x

    @classmethod
    def invalid_item_cargoes(cls):
        x = deepcopy(cls._required)
        x['cargoes'] = {'product': 'foobar'}
        return x


class PortCallModelTestCase(unittest.TestCase):
    @with_test_cases(
        TestIO(
            'all mandatory fields present',
            given=ItemFactory.filled_item(),
            then=ItemFactory.filled_item(),
        )
    )
    def test_model_all_mandatory_fields(self, given):
        return _strip_meta_fields(_validate(given, PortCall, True, True))

    @with_test_cases(
        TestIO('mandatory `eta` field missing', given=ItemFactory.incomplete_item(), then=None)
    )
    def test_model_missing_mandatory_fields(self, given):
        return _strip_meta_fields(_validate(given, PortCall, True, True))

    @with_test_cases(
        TestIO('invalid `vessel` item', given=ItemFactory.invalid_item_vessel(), then=None)
    )
    def test_model_invalid_vessel(self, given):
        return _strip_meta_fields(_validate(given, PortCall, True, True))

    @with_test_cases(
        TestIO('invalid `cargoes` schema', given=ItemFactory.invalid_item_cargoes(), then=None)
    )
    def test_model_invalid_cargoes(self, given):
        return _strip_meta_fields(_validate(given, PortCall, True, True))
