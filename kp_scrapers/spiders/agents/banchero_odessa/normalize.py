import logging
import re

from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]: normalized cargo movement item

    """
    item = map_keys(raw_item, field_mapping())

    # remove unnecessary items
    if not item['vessel_name_length'] or 'name' in item['vessel_name_length'].lower():
        return

    if not item['cargo_product']:
        return

    # build proper Vessel model
    item['vessel_name'], item['vessel_length'] = split_vessel_length(
        item.pop('vessel_name_length', None)
    )

    item['vessel'] = {
        'name': item.pop('vessel_name', None),
        'length': item.pop('vessel_length', None),
    }

    # build proper Cargo model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': None,
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.tons,
    }

    item['port_name'] = 'Odessa'

    return item


def field_mapping():
    # declarative mapping for ease of developement/maintenance
    return {
        'Vessel': ('vessel_name_length', None),
        'approach': ('arrival', None),
        'berthing': ('berthed', None),
        'tons': ('cargo_volume', lambda x: try_apply(x, float, int, str)),
        'commencement': ignore_key('not needed'),
        'completion': ignore_key('not needed'),
        'total': ignore_key('not needed'),
        'date': ('departure', None),
        'Q-ty': ('cargo_volume', lambda x: try_apply(x, float, int, str)),
        'Cargo': ('cargo_product', None),
        'Time': ('eta', None),
        'Shipper': ignore_key('cannot be used for now'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def split_vessel_length(raw_vessel_length):
    """split vessel name and length

    Examples:
        >>> split_vessel_length('NORD HIGHLANDER L=182,55')
        ('NORD HIGHLANDER', '182')

    Args:
        raw_vessel_length (str):

    Returns:
        vessel (str):
        length (str):

    """
    _match = re.match(r'(.*)\sL=(.*)', raw_vessel_length)
    if _match:
        _vessel_name, _vessel_length = _match.groups()
        _vessel_length = _vessel_length.replace(',', '.')
        return _vessel_name, try_apply(_vessel_length, float, int, str)

    return raw_vessel_length, None
