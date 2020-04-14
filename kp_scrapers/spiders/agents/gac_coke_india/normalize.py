import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


PORT_MAPPING = {'KOLKATA (EX CALCUTTA)': 'KOLKATA', 'VOC PORT(EX.TUTICORIN) ': 'TUTICORIN'}


MOVEMENT_MAPPING = {'L': 'Load', 'D': 'discharge'}


@validate_item(CargoMovement, normalize=True, strict=True, log_level='error')
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]: normalized cargo movement item

    """
    item = map_keys(raw_item, field_mapping())

    # build proper Cargo model
    buyer = item.pop('cargo_buyer', None)
    volume = item.pop('cargo_volume', None)
    units = Unit.tons if volume else None
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': item.pop('cargo_movement', None),
        'volume': volume,
        'volume_unit': units,
        'buyer': {'name': buyer} if buyer else None,
    }
    if not item['cargo'].get('buyer') or not item['cargo'].get('buyer').get('name'):
        item['cargo'].pop('buyer')

    return item


def field_mapping():
    return {
        'position': ignore_key('irrelavant'),
        'jetty': ('berth', lambda x: x.replace('.0', '')),
        'name of vessel': ('vessel', lambda x: {'name': normalize_vessel(x)}),
        'eta/ arrived': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'etb/ berthed': ('berthed', lambda x: to_isoformat(x, dayfirst=True)),
        'etd': ('departure', lambda x: to_isoformat(x, dayfirst=True)),
        'cargo': ('cargo_product', None),
        'qty(mt)': ('cargo_volume', None),
        'receiver': ('cargo_buyer', None),
        'l/ d': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x, None)),
        'load port': ignore_key('irrelavant'),
        'port_name': ('port_name', lambda x: PORT_MAPPING.get(x, x)),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: extract_reported_date(x)),
    }


def normalize_vessel(raw_vessel_name):
    """Normalize vessel.

    Args:
        vessel_name (str):

    Examples:
        >>> normalize_vessel('mv. abc')
        'abc'
        >>> normalize_vessel('mv abc')
        'abc'
        >>> normalize_vessel('mv. mbc')
        'mbc'

    Returns:
        str:

    """
    _v_match = re.match(r'(?:mv\s|mv.\s)?(.*)', raw_vessel_name.lower())
    if _v_match:
        return may_strip(_v_match.group(1))

    return raw_vessel_name


def extract_reported_date(raw_reported_date):
    """raw_reported_date

    Args:
        vessel_name (str):

    Returns:
        str:

    """
    _match = re.match(r'.*\s(\d{1,2}\/\d{1,2}\/\d{2,4})', raw_reported_date)
    if _match:
        return to_isoformat(_match.group(1), dayfirst=True)
