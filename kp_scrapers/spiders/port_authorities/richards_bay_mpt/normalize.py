from itertools import zip_longest
import logging
from typing import List, Tuple

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import is_number, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

PRODUCT_BLACKLIST = [
    'passengers',
    'cleaning',
    'jan',
    'feb',
    'mar',
    'apr',
    'may',
    'jun',
    'jul',
    'aug',
    'sep',
    'oct',
    'nov',
    'dec',
    ';',  # ignore summary product rows
    ',',  # ignore summary product rows
    '*',  # ignore summary volume rows
    'maintenance',
    'preship',
    'slot',
    'pre-assembling cargo',
    'pre-assembling',
    'assembling',
    'pre-ship',
    'route',
]

STRING_CLEANING = [
    'export',
    'import',
    'discharge',
    'load',
    'exp/imp',
    'imp',
    'exp',
]


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    # filter out passenger vessels
    if '=' not in item['vessel_name']:
        return

    # cargo volume loaded/unloaded will be fused with vessel name
    vessel_name, potenaial_cargo_volume = item['vessel_name'].split('=')

    # build vessel sub-model
    item['vessel'] = {
        'name': may_strip(vessel_name),
        'length': item.pop('vessel_loa', None),
    }

    item['berthed'], item['eta'] = normalize_date(
        item['eta'],
        item['berthed_day'],
        item['berthed_month'],
        item['berthed_time'],
        item['reported_date'],
    )

    cargo_list = normalize_cargo(item['cargo_list'], potenaial_cargo_volume)
    item['cargoes'] = [
        {
            'product': cargo[0],
            'movement': None,
            'volume': cargo[1] if cargo[1] and cargo[1] != '0' else None,
            'volume_unit': Unit.tons if cargo[1] else None,
        }
        for cargo in cargo_list
    ]
    for col in ('vessel_name', 'berthed_day', 'berthed_month', 'berthed_time', 'cargo_list'):
        item.pop(col)

    return item


def field_mapping():
    return {
        'vessel_name': ('vessel_name', None),
        'vessel_loa': ('vessel_loa', None),
        'eta': ('eta', None),
        'berthed_day': ('berthed_day', None),
        'berthed_month': ('berthed_month', None),
        'berthed_time': ('berthed_time', None),
        'cargo_list': ('cargo_list', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_cargo(raw_cargo_list: str, raw_potential_volume: str) -> List[Tuple[str, str]]:
    """Normalize raw cargo string into a proper Cargo object.

    Examples:
        >>> normalize_cargo(['DISCHARGE SALT', 'DISCHARGE SALT', 'CLEANING'], '7000')
        [('salt', '7000')]
        >>> normalize_cargo(['PIG IRON=7000', 'MANG=8000', 'CLEANING'], '15000')
        [('pig iron', '7000'), ('mang', '8000')]
    """
    product_list = []
    volume_list = []
    for cargo_item in raw_cargo_list:
        _prod, _, _vol = cargo_item.partition('=')
        _prod = clean_product_string(_prod)
        if any(sub in _prod.lower() for sub in PRODUCT_BLACKLIST):
            continue
        if _prod:
            product_list.append(may_strip(_prod))
        if _vol:
            volume_list.append(may_strip(_vol.replace(' ', '')))

    if len(list(set(product_list))) == 1:
        return list(zip_longest(set(product_list), [raw_potential_volume.replace(' ', '')]))

    return list(zip_longest(product_list, volume_list))


def clean_product_string(raw_product_string: str) -> str:
    """clean up product strings

    Examples:
        >>> clean_product_string('iron discharge')
        'iron'
        >>> clean_product_string('ferro chrome')
        'ferro chrome'
    """
    clean_string_list = [
        may_strip(_token)
        for _token in raw_product_string.lower().split(' ')
        if _token not in STRING_CLEANING
    ]

    return ' '.join(clean_string_list)


def normalize_date(
    raw_eta: str,
    raw_berthed_day: str,
    raw_berthed_month: str,
    raw_berthed_time: str,
    reported_date: str,
) -> Tuple[str, str]:
    """Normalize eta and berthing dates

    Some examples of possible input combinations:
        - 'raw_eta': '01-Feb'
        - 'raw_berthed_day': '01-Mar-20'
        - 'raw_berthed_day': '01'
        - 'raw_berthed_time': '16:00'

    Examples:
        >>> normalize_date('01-Feb', '01', None, '17:00', '2020-02-01T00:00:00')
        ('2020-02-01T17:00:00', '2020-02-01T00:00:00')
        >>> normalize_date('01-Feb', '01', None, '17:00', '2020-02-01T00:00:00')
        ('2020-02-01T17:00:00', '2020-02-01T00:00:00')
        >>> normalize_date('01-Feb', '01-Mar-20', '01-Mar-20', '17:00', '2020-02-01T00:00:00')
        ('2020-03-01T17:00:00', '2020-02-01T00:00:00')
        >>> normalize_date('01-Feb', '01', '01-Mar-20', '17:00', '2020-02-01T00:00:00')
        ('2020-03-01T17:00:00', '2020-02-01T00:00:00')
    """
    # check if berthed month contains a month else discard
    if len(str(raw_berthed_month).split('-')) == 3:
        _day, _month, _year = raw_berthed_month.split('-')
        _year = f'20{_year}'
    else:
        _month = parse_date(reported_date).month
        _year = parse_date(reported_date).year

    if is_number(raw_berthed_day):
        _day = raw_berthed_day

    return (
        parse_date(f'{_year}-{_month}-{_day} {raw_berthed_time}:00').isoformat(),
        parse_date(f'{raw_eta}-{_year}', dayfirst=True).isoformat(),
    )
