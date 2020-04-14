import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


IRRELEVANT_VESSEL_TYPES = ['CONT', 'PASS', 'NNES', 'SAIL', 'TNES', 'TUG']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Process item.

    Args:
        raw_item:

    Returns:
        Dict:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # discard irrelevant vessels
    vessel_type = item.pop('vessel_type')
    if vessel_type in IRRELEVANT_VESSEL_TYPES:
        logger.info(
            'Vessel %s (%s) has irrelevant type: %s',
            item['vessel_name'],
            item['vessel_imo'],
            vessel_type,
        )
        return

    # build vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'imo': item.pop('vessel_imo', None),
        'dead_weight': item.pop('vessel_dwt'),
        'gross_tonnage': item.pop('vessel_gt', None),
        'length': item.pop('vessel_loa', None),
        'beam': item.pop('vessel_beam', None),
        'flag_name': item.pop('vessel_flag', None),
    }

    # build proper eta/arrival/departure date
    item[item.pop('event_type')] = item.pop('matching_date')

    return item


def portcall_mapping():
    return {
        # main page
        'Arrived Date': ('matching_date', to_isoformat),
        'Schedule Date': ('matching_date', to_isoformat),
        'Berth': ('berth', None),
        'Vessel Name': ('vessel_name', None),
        'Vessel Type': ignore_key('alternate field for vessel type'),
        'Origin / Destination': ignore_key('FIXME not processed due to limitations on the ETL'),
        'Origin': ignore_key('ignore origin zone'),
        'Destination': ignore_key('FIXME not processed due to limitations on the ETL'),
        # vessel page
        'Type :': ('vessel_type', None),
        'Flag :': ('vessel_flag', None),
        'LOA :': ('vessel_loa', lambda x: try_apply(x, float, int)),
        'Bow to Bridge :': ignore_key('bow to bridge'),
        'Beam :': ('vessel_beam', lambda x: try_apply(x, float, int)),
        'Summer DWT :': ('vessel_dwt', lambda x: try_apply(x, int) if x != '0' else None),
        'Gross Tonnage :': ('vessel_gt', lambda x: try_apply(x, int)),
        'IMO :': ('vessel_imo', None),
        'Thrust Bow :': ignore_key('ignore'),
        'Thrust Stern :': ignore_key('ignore'),
        'Summer Draft :': ignore_key('ignore'),
        'Ramp Mid :': ignore_key('ignore'),
        'Ramp Stern :': ignore_key('ignore'),
        'Cargo Gear :': ignore_key('ignore'),
        'Inert Gas :': ignore_key('ignore'),
        'Crude Oil Wash :': ignore_key('ignore'),
        # meta info
        'event_type': ('event_type', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', to_isoformat),
    }
