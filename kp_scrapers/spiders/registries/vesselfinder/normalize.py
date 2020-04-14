import logging

from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys


logger = logging.getLogger(__name__)


RELEVANT_VESSEL_TYPES = [
    'Bulk Carrier',
    'Chemical Tanker',
    'Chemical/Oil Products Tanker',
    'Crude Oil Tanker',
    'Liquefied Gas',
    'LNG Tanker',
    'LPG Tanker',
    'Oil Products Tanker',
    'Self Discharging Bulk Carrier',
]


def process_item(raw_item):
    """Normalize raw item.

    Args:
        raw_item:

    Returns:

    """
    item = map_keys(raw_item, field_mapping())

    if item.get('type') not in RELEVANT_VESSEL_TYPES:
        logger.info(f'Irrelevant vessel type, discarding: {item["name"]} ({item.get("type")})')
        return

    if not item.get('imo'):
        logger.warning(f'No IMO found for vessel: {item.get("name")}')
        return

    # only warn if MMSI is missing
    if not item.get('mmsi'):
        logger.warning(f'No MMSI found for vessel: {item.get("name")}')

    return item


def field_mapping():
    return {
        'AIS Type': ignore_key('vessel AIS type'),
        'Flag': ('flag_name', None),
        'Destination': ignore_key('destination'),
        'ETA': ignore_key('eta'),
        'IMO / MMSI': ('mmsi', lambda x: x.partition('/')[2].strip() if '/' in x else None),
        'Callsign': ('call_sign', None),
        'Length / Beam': ignore_key('length and beam, extracted from following'),
        'Current draught': ignore_key('current draught'),
        'Course / Speed': ignore_key('course / speed'),
        'Coordinates': ignore_key('coordinates'),
        'Last report': ignore_key('last report'),
        'IMO number': ('imo', None),
        'Vessel Name': ('name', None),
        'Ship type': ('type', None),
        'Homeport': ignore_key('homeport'),
        'Gross Tonnage': ('gross_tonnage', lambda x: try_apply(x, int)),
        'Summer Deadweight (t)': ('dead_weight', lambda x: try_apply(x, int)),
        'Length Overall (m)': ('length', lambda x: try_apply(x, _handle_empty_value, int)),
        'Beam (m)': ('beam', lambda x: try_apply(x, _handle_empty_value, int)),
        'Draught (m)': ignore_key('draught, no access'),
        'Year of Built': ('build_year', lambda x: try_apply(x, int)),
        'Builder': ignore_key('builder, no access'),
        'Place of Built': ignore_key('place of built, no access'),
        'Yard': ignore_key('yard, no access'),
        'TEU': ignore_key('teu'),
        'Crude': ignore_key('crude'),
        'Grain': ignore_key('grain'),
        'Bale': ignore_key('bale'),
        'Registered Owner': ignore_key('registered owner, no access'),
        'Manager': ignore_key('manager, no access'),
        'provider_name': ('provider_name', None),
    }


def _handle_empty_value(raw_value):
    """Handle empty value.

    Args:
        raw_value (str):

    Returns:
        None | str:

    """
    return raw_value if raw_value != '-' else None
