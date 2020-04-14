import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MOVEMENT_MAPPING = {'(D)': 'discharge', '(L)': 'load'}


UNIT_MAPPING = {'MTS': Unit.tons, 'MT': Unit.tons}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict(str, str):

    """
    item = map_keys(raw_item, grades_mapping())
    # discard unknown vessels
    if 'TBA' in item['vessel']['name'] or not item['vessel']['name']:
        return

    if item['cargo_product']:
        # separate conjoined cells
        c_movement, c_vol, c_units = normalize_movement_vol_unit(
            item.pop('cargo_volume_movement', None)
        )

        # build Cargo sub-model
        item['cargoes'] = [
            {
                'product': may_strip(item.pop('cargo_product', None)),
                'movement': c_movement,
                'volume': c_vol,
                'volume_unit': c_units,
            }
        ]

    return item


def grades_mapping():
    return {
        'SHIPS NAME': ('vessel', lambda x: {'name': may_strip(x.replace('*', ''))}),
        'PRODUCTS': ('cargo_product', None),
        'QUANTITY': ('cargo_volume_movement', None),
        'TERMINAL': ('installation', None),
        'PORT': ('port_name', None),
        'ARRIVAL': ('arrival', normalize_date),
        'BERTHING': ('berthed', normalize_date),
        'ETD': ('departure', normalize_date),
        'COMMENTS': ignore_key('irrelevant'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_date(raw_date):
    """Normalize raw port-call date

    Args:
        raw_date (str): raw date

    Returns:
        str: port-call date as an ISO8601 date

    Examples:
        >>> normalize_date('25-12-19')
        '2019-12-25T00:00:00'

    """
    _match = re.match(r'(\d{1,2}\-\d{1,2}\-\d{1,4})', raw_date)
    if _match:
        return to_isoformat(_match.group(1), dayfirst=True)

    return None


def normalize_movement_vol_unit(movement_eta):
    """Normalize movement and eta.

    Examples:
        >>> normalize_movement_vol_unit('(L) 55,000 MTS')
        ('load', '55000', 'tons')

    Args:
        movement_eta:

    Returns:

    """
    if len(movement_eta.split()) == 3:
        _mov, _vol, _units = movement_eta.split()

        movement = MOVEMENT_MAPPING.get(_mov, None)
        volume = _vol.replace(',', '')
        units = UNIT_MAPPING.get(_units, None)

        return movement, volume, units

    return None, None, None
