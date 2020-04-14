# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from copy import deepcopy
import unittest

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.utils import validate_item
from tests._helpers import TestIO, with_test_cases


def _validate_spot_charter(raw_item, normalize, strict):
    @validate_item(SpotCharter, normalize, strict)
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
        'arrival_zone': ['FOO Port', 'BAR Port'],
        'cargo': None,
        'charterer': 'Zee Cargo Co.',
        'lay_can_end': '2018-06-06T21:00:00',
        'rate_raw_value': None,
        'rate_value': None,
        'seller': None,
        'status': None,
    }
    _required = {
        'departure_zone': 'Zee Port',
        'lay_can_start': '2018-06-01T21:00:00',
        'provider_name': 'Zee Shipping',
        'reported_date': '04 Jun 2018',
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
        x.pop('departure_zone')
        return x

    @classmethod
    def invalid_item_arrival_zone(cls):
        x = deepcopy(cls._required)
        x['arrival_zone'] = 'FOO Port'
        return x

    @classmethod
    def invalid_item_reported_date(cls):
        x = deepcopy(cls._required)
        x['reported_date'] = to_isoformat(x['reported_date'])
        return x

    @classmethod
    def invalid_item_laycan(cls):
        x = deepcopy(cls._required)
        x.update(cls._optional)
        _laycan = x['lay_can_start']
        x['lay_can_start'] = x['lay_can_end']
        x['lay_can_end'] = _laycan
        return x


class SpotCharterModelTestCase(unittest.TestCase):
    @with_test_cases(
        TestIO(
            'all mandatory fields present',
            given=ItemFactory.filled_item(),
            then=ItemFactory.filled_item(),
        )
    )
    def test_model_all_mandatory_fields(self, given):
        return _strip_meta_fields(_validate_spot_charter(given, True, True))

    @with_test_cases(
        TestIO(
            'mandatory `departure_zone` field missing',
            given=ItemFactory.incomplete_item(),
            then=None,
        )
    )
    def test_model_missing_mandatory_fields(self, given):
        return _strip_meta_fields(_validate_spot_charter(given, True, True))

    @with_test_cases(
        TestIO(
            'invalid `arrival_zone` type', given=ItemFactory.invalid_item_arrival_zone(), then=None
        )
    )
    def test_model_invalid_arrival_zone(self, given):
        return _strip_meta_fields(_validate_spot_charter(given, True, True))

    @with_test_cases(
        TestIO(
            'unsupported `reported_date` date format',
            given=ItemFactory.invalid_item_reported_date(),
            then=None,
        )
    )
    def test_model_invalid_reported_date(self, given):
        return _strip_meta_fields(_validate_spot_charter(given, True, True))

    @with_test_cases(
        TestIO('invalid `lay_can` dates', given=ItemFactory.invalid_item_laycan(), then=None)
    )
    def test_model_invalid_laycan(self, given):
        return _strip_meta_fields(_validate_spot_charter(given, True, True))
