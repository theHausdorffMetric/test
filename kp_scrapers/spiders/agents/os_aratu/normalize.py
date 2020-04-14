import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # discard cargo movement if payload is not confirmed
    if not item.get('cargo_product'):
        return

    # build cargo sub model
    item['cargo'] = {
        'product': item['cargo_product'],
        'movement': normalize_movement(item['load_cargo_movement'], item['dis_cargo_movement']),
        'volume': item.get('cargo_quantity'),
        'volume_unit': Unit.tons,
    }

    # remove rogue fields
    for _col in ['cargo_product', 'cargo_quantity', 'load_cargo_movement', 'dis_cargo_movement']:
        item.pop(_col, None)

    return item


def field_mapping():
    return {
        'VESSEL': ('vessel', lambda x: {'name': may_strip(x)}),
        'ETA': ('eta', normalize_date),
        'ETB': ('berthed', normalize_date),
        'ETS': ('departure', normalize_date),
        'GRADE': ('cargo_product', lambda x: None if 'TBI' in x else x),
        'QTTY': ('cargo_quantity', lambda x: try_apply(x.replace('.', '', 1), int)),
        'TTL  QTTY': ignore_key('redundant'),
        'LD PORT': ('load_cargo_movement', None),
        'DISC PORT': ('dis_cargo_movement', None),
        'SH / REC': ignore_key('redundant'),
        'RMK': ignore_key('redundant'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_date(raw_date):
    """Normalize date

    Date might appear be a range, pick the later date.
        - 01/08/13 - 01:01
        - 01/08/13

    Examples:
        >>> normalize_date('13/12/19')
        '2019-12-13T00:00:00'
        >>> normalize_date('13/12/19 - 01:01')
        '2019-12-13T01:01:00'
        >>> normalize_date('TBI')

    Args:
        raw_date (str):

    Returns:
        str: date
    """
    match_date = re.match(r'(\d+\/\d+\/\d+)\s?\-?\s?(\d+)?\:?(\d+)?', may_strip(raw_date))

    if match_date:
        if '-' in raw_date:
            _date, _hour, _min = match_date.groups()

            date_time = parse_date(_date, dayfirst=True).replace(hour=int(_hour), minute=int(_min))
            return date_time.isoformat()

        return to_isoformat(match_date.group(1), dayfirst=True)


def normalize_movement(load_raw_movement, dis_raw_movement):
    """Normalize movement

    Examples:
        >>> normalize_movement('china', 'aratu')
        'discharge'
        >>> normalize_movement('aratu', 'china')
        'load'

    Args:
        load_raw_movement (str):
        dis_raw_movement (str):

    Returns:
        str: movement
    """
    for _movement in [load_raw_movement, dis_raw_movement]:
        if 'aratu' in _movement.lower() and _movement == load_raw_movement:
            return 'load'

        if 'aratu' in _movement.lower() and _movement == dis_raw_movement:
            return 'discharge'
