import logging
import re

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


ZONE_MAPPING = {
    # These ports are not be on our platform as they cater
    # to smaller vessels VAN GIA, BINH TRI PORT
    'CAN LAN PORT': 'CAM PHA',
    'CAN LAN': 'CAM PHA',
    'CAM PHA': 'CAM PHA',
    'CAM PHA PORT': 'CAM PHA',
    'CUA LO': 'VUNG TAU',
    'CUA LO PORT': 'VUNG TAU',
    'DUYEN HAI EVN': 'DUYEN HAI',
    'DUYEN HAI TRA VINH': 'DUYEN HAI',
    'DUYEN HAI': 'DUYEN HAI',
    'DUYEN HAI 3': 'DUYEN HAI',
    'GO DA': 'VUNG TAU',
    'GO DA ANCHORAGE': 'VUNG TAU',
    'GO DAU': 'VUNG TAU',
    'HOA PHAT DUNG QUAT': 'CAM PHA',
    'HOA PHAT DUNG QUAT PORT': 'CAM PHA',
    'HO CHI MINH': 'HO CHI MINH',
    'HCM': 'HO CHI MINH',
    'HCM PORT': 'HO CHI MINH',
    'LONG THUAN PORT': 'HO CHI MINH',
    'NGHI SON': 'THANH HOA',
    'PHU MY': 'VUNG TAU',
    'PTSC': 'VUNG TAU',
    'QUANG NINH': 'CAM PHA',
    'SON DUONG': 'HA TINH',
    'THI VAI': 'VUNG TAU',
    'VUNG TAU ANCHORAGE': 'VUNG TAU',
    'VINH TAN 2': 'BING THUAN',
    'VINH TAN 4': 'BING THUAN',
}


@validate_item(CargoMovement, normalize=True, strict=True)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # vessel item
    if not item['vessel']['name']:
        return

    item['cargo'] = {
        'product': None,
        'movement': None,
        'volume': item['cargo_volume'],
        'volume_unit': Unit.tons,
        'buyer': {'name': item['cargo_buyer']} if item.get('cargo_buyer') else None,
        'seller': {'name': item['cargo_seller']} if item.get('cargo_seller') else None,
    }
    item['port_name'] = item['arrival_zone']

    for col in ['cargo_seller', 'cargo_buyer', 'cargo_volume', 'departure_zone', 'arrival_zone']:
        item.pop(col, None)

    return item


def field_mapping():
    return {
        'date': ('berthed', None),
        'vessel\'s name': ('vessel', lambda x: {'name': normalize_vessel(x) if x else None}),
        'seller': ('cargo_seller', None),
        'importer': ('cargo_buyer', None),
        'coal (ton)': ('cargo_volume', None),
        'p.o.l': ('departure_zone', None),
        'p.o.d': ('arrival_zone', lambda x: ZONE_MAPPING.get(may_strip(x.upper()), x)),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_vessel(raw_vessel_name):
    """Normalize vessel.

    Args:
        vessel_name (str):

    Examples:
        >>> normalize_vessel('mv. mbc')
        'mbc'
        >>> normalize_vessel('mv mbc')
        'mbc'
        >>> normalize_vessel('mv. mbc')
        'mbc'
        >>> normalize_vessel('mv.mbc')
        'mbc'
        >>> normalize_vessel('mbc')
        'mbc'
        >>> normalize_vessel('m/v mbc')
        'mbc'

    Returns:
        str:

    """
    _v_match = re.match(r'(?:mv\s|m/v\s|mv.\s?)?(.*)', raw_vessel_name.lower())
    if _v_match:
        return may_strip(_v_match.group(1))

    return may_strip(raw_vessel_name)
