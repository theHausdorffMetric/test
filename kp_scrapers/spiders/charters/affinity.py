# -*- coding: utf-8 -*-
from __future__ import absolute_import

from dateutil import parser as date_parser

from kp_scrapers.lib import parser
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.items import as_item, Cargo, SpotCharter, Vessel
from kp_scrapers.spiders.bases.gdrive import GDriveXlsSpider
from kp_scrapers.spiders.bases.markers import CoalMarker, CppMarker, LngMarker, LpgMarker, OilMarker
from kp_scrapers.spiders.charters import CharterSpider


def to_isoformat(date_str):
    return date_parser.parse(date_str, dayfirst=True).isoformat()


SPORT_CHARTER_MAPPER = {
    'lay can from': ('lay_can_start', to_isoformat),
    'lay can to': ('lay_can_end', to_isoformat),
    'from zone code': ('departure_zone_code', parser.may_strip),
    'to zone code': ('arrival_zone_code', parser.may_strip),
    'charterer': ('charterer', parser.may_strip),
    'from port': ('departure_zone', parser.may_strip),
    'to port': ('arrival_zone', lambda x: [parser.may_strip(x)]),
    'reported date': ('reported_date', to_isoformat),
}

VESSEL_MAPPER = {'imonumber': ('imo', parser.may_strip)}

CARGO_MAPPER = {'cargo type name': ('product', parser.may_strip)}


class AffinityXlsSpider(
    CharterSpider, GDriveXlsSpider, CoalMarker, CppMarker, LngMarker, LpgMarker, OilMarker
):
    name = 'AffinityXls'
    version = '1.0.0'
    provider = 'Affinity'

    def __init__(self, *args, **kwargs):
        # first_title is the name of the first element of the title row.
        super(AffinityXlsSpider, self).__init__(first_title='imonumber', *args, **kwargs)

    def process_item(self, item):
        item.update({'vessel': self.parse_vessel(item), 'cargo': self.parse_cargo(item)})
        yield self.parse_spot_charter(item)

    @as_item(SpotCharter)
    def parse_spot_charter(self, item):
        spot_charter = map_keys(item, SPORT_CHARTER_MAPPER, skip_missing=False)

        return spot_charter

    @as_item(Vessel)
    def parse_vessel(self, item):
        return map_keys(item, VESSEL_MAPPER, skip_missing=False)

    @as_item(Cargo)
    def parse_cargo(self, item):
        return map_keys(item, CARGO_MAPPER, skip_missing=False)
