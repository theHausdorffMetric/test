import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_apply, may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

UNIT_MAPPING = {
    'm3': Unit.cubic_meter,
    'cubic_meter': Unit.cubic_meter,
    'kt': Unit.kilotons,
    'mt': Unit.tons,
    't': Unit.tons,
    'tons': Unit.tons,
}

STATUS_MAPPING = {
    'on_subs': SpotCharterStatus.on_subs,
    'fully_fixed': SpotCharterStatus.fully_fixed,
    'failed': SpotCharterStatus.failed,
}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict(str, str):

    """
    item = map_keys(raw_item, charter_mapping())

    item['vessel'] = {
        'name': item.pop('vessel_name', None),
        'imo': item.pop('vessel_imo', None),
        'dead_weight': item.pop('vessel_dwt', None),
        'length': item.pop('vessel_length', None),
    }
    # discard unknown vessels
    if 'TBA' in item['vessel']['name'] or not item['vessel']['name']:
        return

    seller = item.pop('cargo_seller', None)
    buyer = item.pop('cargo_buyer', None)
    if item['cargo_product']:
        # build Cargo sub-model
        item['cargo'] = {
            'product': may_strip(item.pop('cargo_product', None)),
            'movement': item.pop('cargo_movement', None),
            'volume': item.pop('cargo_volume', None),
            'volume_unit': item.pop('cargo_unit', None),
            'buyer': {'name': buyer} if buyer else None,
            'seller': {'name': seller} if seller else None,
        }

    return item


def charter_mapping():
    return {
        'vessel_name': ('vessel_name', may_strip),
        'vessel_imo': ('vessel_imo', lambda x: may_apply(x, float, int, str)),
        'vessel_length': ('vessel_length', lambda x: may_apply(x, float, int)),
        'vessel_dwt': ('vessel_dwt', lambda x: may_apply(x, float, int)),
        'charterer': ('charterer', may_strip),
        'status': ('status', lambda x: STATUS_MAPPING.get(x.lower(), x) if x else None),
        'lay_can_start': (
            'lay_can_start',
            lambda x: to_isoformat(x, dayfirst=False, yearfirst=True),
        ),
        'lay_can_end': ('lay_can_end', lambda x: to_isoformat(x, dayfirst=False, yearfirst=True)),
        'rate_value': ('rate_value', may_strip),
        'rate_raw_value': ('rate_raw_value', may_strip),
        'departure_zone': ('departure_zone', may_strip),
        'arrival_zone': ('arrival_zone', lambda x: may_strip(x).split('-') if x else None),
        'cargo_product': ('cargo_product', may_strip),
        'cargo_movement': ('cargo_movement', None),
        'cargo_volume': ('cargo_volume', None),
        'cargo_unit': ('cargo_unit', lambda x: UNIT_MAPPING.get(x.lower(), x) if x else None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: parse_date(x).strftime('%d %b %Y')),
    }
