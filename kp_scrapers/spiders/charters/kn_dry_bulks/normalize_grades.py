import logging
import re

from kp_scrapers.lib.date import is_isoformat, to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


UNIT_MAPPING = {'MT': Unit.tons, 'KB': Unit.barrel}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, grades_mapping(), skip_missing=True)

    # build vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name')}

    item['cargo_volume'], item['cargo_units'] = normalize_volume_unit(item['cargo_volume_unit'])

    # build cargo sub-model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'discharge',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': item.pop('cargo_units', None),
    }

    item.pop('cargo_volume_unit', None)

    return item


def grades_mapping():
    return {
        'PORT': (ignore_key('irrelevant')),
        'VSL NAME': ('vessel_name', lambda x: normalize_vessel_name(x)),
        'CARGO': ('cargo_product', None),
        'QNTY': ('cargo_volume_unit', None),
        'OWNERS ': (ignore_key('irrelevant')),
        'DIS PORT': ('port_name', None),
        'LOAD PORT': (ignore_key('irrelevant')),
        'SHIPPERS': (ignore_key('irrelevant')),
        'SHIPPER': (ignore_key('irrelevant')),
        'RECEIVERS': (ignore_key('irrelevant')),
        'RECEIVER': (ignore_key('irrelevant')),
        'CHARTS': (ignore_key('irrelevant')),
        'ARRIVED': ('arrival', lambda x: normalize_date(x)),
        'BERTHED': ('berthed', lambda x: normalize_date(x)),
        'SAILED': ('departure', lambda x: normalize_date(x)),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
    }


def normalize_date(raw_date):
    """Normalize laycan

    Examples:
        - '02.02.19'

    Examples:
        >>> normalize_date('02.02.19')
        '2019-02-02T00:00:00'

    Args:
        raw_laycan (str):

    Returns:
        str:
    """
    return raw_date if is_isoformat(raw_date) else to_isoformat(raw_date, dayfirst=True)


def normalize_vessel_name(raw_vessel_name):
    """Normalize vessel names, remove unnessary letters.

    Examples:
        - MV ORARLE
        - MV.ORACLE

    Examples:
        >>> normalize_vessel_name('MV ORACLE')
        'ORACLE'
        >>> normalize_vessel_name('ORACLE')
        'ORACLE'

    Args:
        raw_vessel_name (str):

    Returns:
        str:

    """
    _match = re.match(r'^MV(?:[. ])?(.*)', raw_vessel_name)
    if _match:
        return _match.group(1)

    return raw_vessel_name


def normalize_volume_unit(raw_volume_unit):
    """Normalize volume unit strings

    Examples:
        - 10000MT
        - 10000
        - 10000 MT

    Examples:
        >>> normalize_volume_unit('10000MT')
        ('10000', 'tons')
        >>> normalize_volume_unit('10000')
        ('10000', 'tons')

    Args:
        raw_volume_unit (str):

    Returns:
        str, str:

    """
    _match = re.match(r'([0-9.,]+)([A-z ]+)?', raw_volume_unit)
    if _match:
        volume, units = _match.groups()
        volume = volume.replace(',', '')
        units = UNIT_MAPPING.get(may_strip(units), may_strip(units)) if units else Unit.tons

        return volume, units
