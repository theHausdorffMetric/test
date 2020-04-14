import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


DATETIME_PAIRINGS = [
    ('eta_date', 'eta_time', 'eta'),
    ('berthed_date', 'berthed_time', 'berthed'),
    ('arrival_date', 'arrival_time', 'arrival'),
]


@validate_item(CargoMovement, normalize=True, strict=True, log_level='error')
def process_item(raw_item):
    """Transform raw item to Portcall model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, field_mapping())

    # remove vessels not named
    if 'NIL' in item['vessel_name']:
        return

    item['vessel'] = {'name': item.pop('vessel_name', None), 'length': item.pop('vessel_loa', None)}

    for col in DATETIME_PAIRINGS:
        if item.get(col[0]):
            item[col[2]] = combine_date_time(
                normalize_date(item.pop(col[0], None), item['reported_date']),
                normalize_time(item.pop(col[1], None)),
            )

    cargo_list = split_cargo_volume(item.pop('cargo_product', None), item.pop('cargo_volume', None))

    for item_cargo in cargo_list:
        item['cargo'] = {
            'product': item_cargo[0],
            'movement': None,
            'volume': item_cargo[1],
            'volume_unit': Unit.tons,
        }
        yield item


def field_mapping():
    return {
        'VESSEL\'S NAME': ('vessel_name', may_strip),
        'LOA': ('vessel_loa', None),
        'DRAFT': ignore_key('vessel draught'),
        'CARGO': ('cargo_product', None),
        'QTY': ('cargo_volume', None),
        'E.T.A': ('eta_date', None),
        'E.T.A_time': ('eta_time', None),
        'E.T.B': ('berthed_date', None),
        'E.T.B_time': ('berthed_time', None),
        'ARRIVED': ('arrival_date', None),
        'ARRIVED_time': ('arrival_time', None),
        'QTY': ('cargo_volume', None),
        'B. NO.': ('berth', None),
        'REMARKS': ignore_key('REMARKS'),
        'reported_date': ('reported_date', None),
        'provider_name': ('provider_name', None),
        'port_name': ('port_name', None),
    }


def normalize_date(raw_date, raw_reported_date):
    """dates do not contain the year

    Format:
    1. dd/mm

    Args:
        raw_date (str):
        raw_reported_date (str):

    Examples:
        >>> normalize_date('04/07', '2019-04-03T10:01:00')
        datetime.datetime(2019, 7, 4, 0, 0)
        >>> normalize_date('30/12', '2019-12-30T10:01:00')
        datetime.datetime(2019, 12, 30, 0, 0)
        >>> normalize_date('30/12', '2020-01-01T10:01:00')
        datetime.datetime(2019, 12, 30, 0, 0)
        >>> normalize_date('03/01', '2019-12-30T10:01:00')
        datetime.datetime(2020, 1, 3, 0, 0)

    Returns:
        str:
    """
    year = parse_date(raw_reported_date).year
    _match = re.match(r'(\d{2})\/(\d{2})', raw_date)

    if _match:
        _day, _month = _match.groups()

        f_date = dt.datetime(year=int(year), month=int(_month), day=int(_day))

        # to accomodate end of year parsing, prevent dates too old or far into
        # the future. 100 days was chosen as a gauge
        if (f_date - parse_date(raw_reported_date)).days < -100:
            f_date = dt.datetime(year=int(year) + 1, month=int(_month), day=int(_day))

        if (f_date - parse_date(raw_reported_date)).days > 100:
            f_date = dt.datetime(year=int(year) - 1, month=int(_month), day=int(_day))

        return f_date

    logger.error('unable to parse %s', raw_date)

    return None


def normalize_time(raw_time_string):
    """times are in a seperate column

    Format:
    1. hhss

    Args:
        raw_time_string (str):

    Examples:
        >>> normalize_time('0407')
        datetime.time(4, 7)

    Returns:
        Tuple[str, str]:
    """
    if raw_time_string and raw_time_string.isdigit():
        return dt.time(int(raw_time_string[:2]), int(raw_time_string[2:]))

    return dt.time(hour=0)


def combine_date_time(date_object, time_object):
    """combine date and time objects

    Args:
        date_string (str):
        time_string (str):

    Returns:
        str:
    """
    if date_object and type(date_object) is dt.datetime:
        return dt.datetime.combine(date_object, time_object).isoformat()

    return None


def split_cargo_volume(raw_cargo, raw_vol):
    """times are in a seperate column

    Args:
        raw_cargo (str):
        raw_vol (str):

    Examples:
        >>> split_cargo_volume('DIESEL + PETROL', '1000 + 1000')
        [('DIESEL', '1000'), ('PETROL', '1000')]
        >>> split_cargo_volume('DIESEL & PETROL', '1000 + 1000')
        [('DIESEL', '1000'), ('PETROL', '1000')]
        >>> split_cargo_volume('DIESEL & PETROL', '1000')
        [('DIESEL', '500'), ('PETROL', '500')]
        >>> split_cargo_volume('CORN & SOYA BEAN', '38748.76')
        [('CORN', '19374'), ('SOYA BEAN', '19374')]

    Returns:
        Tuple[str, str]:
    """
    product_list = [may_strip(prod) for prod in re.split(r'[\&\+]', raw_cargo)]
    vol_list = [may_strip(vol) for vol in re.split(r'[\&\+]', raw_vol)]

    if len(product_list) == len(vol_list):
        return list(zip(product_list, vol_list))

    if len(vol_list) == 1 and len(product_list) > 1:
        f_list = []
        for item_product in product_list:
            f_list.append(
                (item_product, try_apply(try_apply(vol_list[0], float), lambda x: x / 2, int, str))
            )

        return f_list

    return raw_cargo, raw_vol
