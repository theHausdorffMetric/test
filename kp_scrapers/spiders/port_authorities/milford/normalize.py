import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


VESSEL_TYPE_TO_PRODUCT_MAPPING = {
    'Chemical Tanker': 'Liquids',
    'Chemical/Oil Products Tanker': 'Liquids',
    'Chemical/Products Tanker': 'Liquids',
    'Crude Oil Tanker': 'Liquids',
    'Crude/Oil Products Tanker': 'Liquids',
    'LNG Tanker': 'LNG',
    'LPG Tanker': 'LPG',
    'Oil Products Tanker': 'Liquids',
    'Products Tanker': 'Liquids',
    'VLCC': 'Liquids',
}

# see https://bit.ly/2qxQgEJ
BERTH_TO_INSTALLATION_MAPPING = {
    'Dragon': 'Dragon',
    'Puma': 'Milford Haven II',
    'South Hook': 'South Hook',
    'Valero': 'Valero Pembroke',
    'VPOT': 'Milford Haven I',
}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # discard portcall if vessel is of an irrelevant type
    vessel_type = item.pop('vessel_type', None)
    if vessel_type not in VESSEL_TYPE_TO_PRODUCT_MAPPING:
        logger.info(f'Vessel {item["vessel_name"]} has irrelevant type: {vessel_type}')
        return

    # build Cargo sub-model
    item['cargoes'] = [{'product': VESSEL_TYPE_TO_PRODUCT_MAPPING[vessel_type]}]

    # build Vessel sub model
    item['vessel'] = {'name': item.pop('vessel_name'), 'gross_tonnage': item.pop('gross_tonnage')}

    return item


def portcall_mapping():
    return {
        'Agent': ('shipping_agent', None),
        'Berth Name': ('installation', map_berth_to_installation),
        'Berthed Time': ('berthed', lambda x: to_isoformat(x, dayfirst=False)),
        'ETA': ('eta', lambda x: to_isoformat(x, dayfirst=False)),
        'From': ignore_key('previous port of call'),
        'GT': ('gross_tonnage', lambda x: try_apply(x, float, int)),
        'Next Movement Time': ('departure', lambda x: to_isoformat(x, dayfirst=False)),
        'Ship': ('vessel_name', None),
        'Ship Type': ('vessel_type', None),
        'To': ('installation', map_berth_to_installation),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def map_berth_to_installation(raw_berth):
    """Clean, normalize, and map a raw berth name to a known installation.

    Args:
        raw_berth (str):

    Returns:
        str: known installation

    Examples:
        >>> map_berth_to_installation('Valero 8')
        'Valero Pembroke'
        >>> map_berth_to_installation('Dragon No1')
        'Dragon'
        >>> map_berth_to_installation('Milford Dock')

    """
    for known_berth in BERTH_TO_INSTALLATION_MAPPING:
        if known_berth in raw_berth:
            return BERTH_TO_INSTALLATION_MAPPING[known_berth]

    logger.info(f'Unknown berth: {raw_berth}')
    return None
