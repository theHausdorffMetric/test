import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_PRODUCTS = ['-', 'cars', 'containers']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, portcall_mapping())
    # discard vessel movements with irrelevant cargoes
    if not item['cargoes']:
        logger.info(f'Vessel {raw_item["Vessel Name"]} has irrelevant cargo {raw_item["Cargo"]}')
        return

    return item


def portcall_mapping():
    return {
        'Agent': ignore_key('shipping agent'),
        'Arrival': ('eta', to_isoformat),
        'Berth': ignore_key('berth'),
        'Berthed': ('berthed', to_isoformat),
        'Cargo': (
            'cargoes',
            lambda x: None if x.lower() in IRRELEVANT_PRODUCTS else [{'product': x}],
        ),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'Qty(MT)': ignore_key('cargo volume; useless without flow direction'),
        'reported_date': ('reported_date', None),
        'Sailing': ignore_key('deprture date; irrelevant for ETA'),
        'SNo.': ignore_key('serial number'),
        'Vessel Name': ('vessel', lambda x: {'name': clean_vessel_name(x)}),
    }


def clean_vessel_name(vessel_name):
    """Remove common vessel prefixes from a raw vessel name.

    Examples:
        >>> clean_vessel_name('LNG C Maran Gas Troy')
        'Maran Gas Troy'
        >>> clean_vessel_name('MT Celsius Monaco')
        'Celsius Monaco'
        >>> clean_vessel_name('MTA Monaco')
        'MTA Monaco'
        >>> clean_vessel_name('BW Boss')
        'BW Boss'

    """
    for prefix in ('MT ', 'MV ', 'LPG C ', 'LNG C '):
        if vessel_name.startswith(prefix):
            return vessel_name.replace(prefix, '')

    return vessel_name
