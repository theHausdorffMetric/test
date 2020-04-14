import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to SpotCharter model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, field_mapping())
    # discarded unwanted vessels
    if not item['vessel']['name'] or any(
        blacklist in item['vessel']['name'] for blacklist in ('NIL', 'NO.')
    ):
        return

    # ignore items with no dates at all
    if not item['eta'] and not item['departure'] and not item['berthed']:
        return

    if item['port_name'].lower() == 'tanjung mangkok':
        product = 'Iron Ore'
    else:
        product = 'Thermal Coal'

    # build cargo
    item['cargo'] = {
        # Product request by coal analyst, malay trivedi
        'product': product,
        'movement': 'load',
        'volume': item['cargo_quantity'] if item['cargo_quantity'].isdigit() else None,
        'volume_unit': Unit.tons,
        'seller': {'name': item.pop('cargo_seller', None)},
    }

    item.pop('cargo_quantity')

    return item


def field_mapping():
    return {
        'FILLER': ignore_key('irrelevant'),
        'NO.': ignore_key('irrelevant'),
        'VESSELS NAME': ('vessel', lambda x: {'name': x}),
        'QUANTITY': ('cargo_quantity', lambda x: x.replace(',', '')),
        'ETA': ('eta', lambda x: normalize_dates(x)),
        'LOAD': ('berthed', lambda x: normalize_dates(x)),
        'ETC / D': ('departure', lambda x: normalize_dates(x)),
        'DESTINATION': ignore_key('irrelevant'),
        'SHIPPER': ('cargo_seller', None),
        'provider_name': ('provider_name', None),
        'port_name': ('port_name', lambda x: normalize_port_name(x)),
        'reported_date': ('reported_date', lambda x: to_isoformat(x)),
    }


def normalize_dates(raw_dates):
    """ Normalize raw dates
    Args:
        raw_lay_can (str):

    Returns:
        str:

    Examples:
        >>> normalize_dates('12/08/19 - 19.40')
        '2019-08-12T19:40:00'
        >>> normalize_dates('16-17/08/19 - RVTG')
        '2019-08-17T00:00:00'
        >>> normalize_dates('17/08/19 - RVTG')
        '2019-08-17T00:00:00'
        >>> normalize_dates('19/08/19 - PM')
        '2019-08-19T12:00:00'
        >>> normalize_dates('19/08/19')
        '2019-08-19T00:00:00'
        >>> normalize_dates('19/08/19 -')
        '2019-08-19T00:00:00'
    """
    if '-' not in raw_dates:
        try:
            return to_isoformat(raw_dates, dayfirst=True)
        except Exception:
            return None

    if len(raw_dates.split('-')) == 2:
        date, _, time = raw_dates.partition('-')
        _hour, _min = normalize_time(time)
        return to_isoformat(f'{date} {_hour}:{_min}', dayfirst=True)

    if len(raw_dates.split('-')) == 3:
        _, _, date_time = raw_dates.partition('-')
        date, _, time = date_time.partition('-')
        _hour, _min = normalize_time(time)
        return to_isoformat(f'{date} {_hour}:{_min}', dayfirst=True)


def normalize_time(raw_time):
    """ Normalize time

    Args:
        raw_time (str):

    Returns:
        Tuple[str, str]:

    Examples:
        >>> normalize_time('19.40')
        ('19', '40')
        >>> normalize_time('RVTG')
        ('00', '00')
        >>> normalize_time('')
        ('00', '00')
    """
    if '.' in raw_time:
        n_hour, _, n_min = raw_time.partition('.')
        return n_hour, n_min
    elif 'PM' in raw_time:
        return '12', '00'
    elif 'AM' in raw_time:
        return '09', '00'

    return '00', '00'


def normalize_port_name(raw_port_name):
    """ Normalize port names

    Args:
        raw_port_name (str):

    Returns:
        str:

    Examples:
        >>> normalize_port_name('A. ABC')
        'ABC'
        >>> normalize_port_name('ABC')
        'ABC'
        >>> normalize_port_name('AA. ABC')
        'AA. ABC'
    """
    _match = re.match(r'^(?:[A-z]{1}\.)?(.*)$', may_strip(raw_port_name))
    if _match:
        return may_strip(_match.group(1))

    return raw_port_name
