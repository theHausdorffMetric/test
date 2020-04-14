from datetime import datetime
from itertools import zip_longest
import logging
import re

from kp_scrapers.lib.date import is_isoformat, to_isoformat
from kp_scrapers.lib.parser import may_remove_substring, may_strip, split_by_delimiters, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

STRING_BLACKLIST = ['N/A', 'TBA', 'TBC']
MOVEMENT_MAPPING = {
    'DISCHARGING': 'discharge',
    'TO DISCHARGE': 'discharge',
    'DISCHARGED': 'discharge',
    'LOADING': 'load',
    'TO LOAD': 'load',
    'LOADED': 'load',
}
LETTERS = {
    'ñ': 'n',
}
ABBREVIATIONS = ["a.m", "am", "p.m", "pm"]


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.
    Args:
        raw_item (Dict[str, str]):
    Yields:
        Dict[str, str]:
    """
    item = map_keys(raw_item, grades_mapping())

    # discard items without vessel names
    vessel_name = item.pop('vessel_name')
    if not vessel_name:
        logger.warning(f'Item has no vessel name, discarding:\n{item}')
        return

    # discard items without valid dates
    if not (item.get('arrival') and item.get('berthed') and item.get('departure')):
        logger.warning(f'Item has no valid portcall dates, discarding:\n{item}')
        return

    # check if years of etc, eta and etb are similar to the reported year
    item = normalize_check_dates(item, ['arrival', 'berthed', 'departure'])

    # build Vessel sub-model
    item['vessel'] = {'name': vessel_name, 'imo': item.pop('vessel_imo', None)}

    # build Cargo sub-model
    products = item.pop('cargo_product', [])
    movement = item.pop('cargo_movement', None)
    # associate volumes to their respective products
    volumes = item.pop('cargo_volume', None)
    cargo_type = item.pop('cargo_type', None)

    # take into account barrel unit
    if cargo_type == 'CRUDE':
        volume_unit = Unit.barrel
    else:
        volume_unit = Unit.tons

    for product, volume in zip_longest(products, volumes):
        item['cargo'] = {
            'product': product,
            'movement': movement,
            'volume': try_apply(volume, float, int),
            'volume_unit': volume_unit,
        }

        yield item


def grades_mapping():
    return {
        'port_name': ('port_name', normalize_port_name),
        'VESSEL': ('vessel_name', None),
        'DATE OF ARRIVAL': ('arrival', normalize_pc_date),
        'ETA': ('arrival', normalize_pc_date),
        'ETB': ('berthed', normalize_pc_date),
        'PIER': ('berth', normalize_berth_name),
        'ETC': ('departure', normalize_pc_date),
        'TERMINAL': ('installation', lambda x: x if x else None),
        'STATUS': ignore_key('irrelevant'),
        'AGENCY': ignore_key('shipping agent is not required'),
        'CHARTERER': ignore_key('charterer is not required'),
        'SHIPOWNER': ignore_key('not required'),
        'OPERATION': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x)),
        'TYPE': ('cargo_type', None),
        'PRODUCT': ('cargo_product', split_cargoes),
        'MT BY PRODUCT': ('cargo_volume', split_volumes),
        'TOTAL MT': ignore_key('we already have mt by product'),
        'DISCH LAST 24 HRS': ignore_key('not required'),
        'DISCHARGED': ignore_key('not required for PortCall for now'),
        'ROB/LOAD': ignore_key('not required'),
        'PORT LOAD/DISCH': ignore_key('not required'),
        'provider_name': ('provider_name', None),
        'region_name': ignore_key('not required'),
        'reported_date': ('reported_date', normalize_reported_date),
    }


def split_cargoes(raw_cargo):
    """Split a raw cargo into multiple cargoes.
    Args:
        raw_cargo (str):
    Returns:
        List[str]:
    Examples:
        >>> split_cargoes('N/A')
        []
        >>> split_cargoes('MDO/ ULSD')
        ['MDO', 'ULSD']
        >>> split_cargoes('VGO')
        ['VGO']
    """
    if not raw_cargo or raw_cargo in STRING_BLACKLIST:
        return []

    return split_by_delimiters(raw_cargo, '/', '+', '\n')


def split_volumes(raw_volume):
    """Split a raw cargo into multiple cargoes.
    Args:
        raw_volume (str):
    Returns:
        List[str]:
    Examples:
        >>> split_volumes('TBC')
        []
        >>> split_volumes('112/ 116')
        ['112', '116']
        >>> split_volumes('111')
        ['111']
    """
    if not raw_volume or raw_volume in STRING_BLACKLIST:
        return []
    raw_volume = may_remove_substring(raw_volume, [',', 'BLS'])
    return split_by_delimiters(raw_volume, '/', '+', '\n')


def normalize_port_name(port_str):
    """Cleanup port name.

        Args:
            port_str (str):

        Returns:
            str | None: string without undesired letters
    """
    if not port_str:
        return None

    for letter in LETTERS.keys():
        if re.compile(letter).search(port_str):
            port_str = port_str.replace(letter, LETTERS[letter])
    return port_str


def normalize_berth_name(berth_str):
    """Cleanup port name.

        Args:
            berth_str (str):

        Returns:
            str | None: string without undesired letters
    """
    if not may_strip(berth_str) or any(sub in berth_str for sub in STRING_BLACKLIST):
        return None

    berth_str = may_strip(berth_str)
    if not re.compile('/').search(berth_str):
        berth_str = may_remove_substring(berth_str, ['.', '0'])
    return berth_str


def normalize_pc_date(date_str):
    """Cleanup portcall-related date.

    Args:
        date_str (str):

    Returns:
        str | None: date string without "am" or 'pm' features
    """

    if not may_strip(date_str) or any(sub in date_str for sub in STRING_BLACKLIST):
        return None

    # remove 'am' and 'pm' abbreviations in portcall-dates
    if not is_isoformat(date_str):
        for abbreviation in ABBREVIATIONS:
            if re.compile(abbreviation).search(date_str):
                date_str = date_str.replace(abbreviation, '')
                # assume hours of arrival for 'am' and 'pm' equal to 06:00 and 18:00
                if abbreviation == 'a.m' or abbreviation == 'am':
                    date_str = date_str + '06:00'
                else:
                    date_str = date_str + '18:00'
        # parse raw date and format it
        date_str = may_remove_substring(date_str, ["'", '.', ' '])
        date_str = datetime.strptime(date_str, '%d-%b%H:%M')

    return date_str


def normalize_reported_date(raw_date):
    """Normalize raw reported date into an ISO8601-formatted string.
    This function WILL discard timezone data.
    Args:
        raw_date (str):
    Returns:
        str: ISO8601-formatted date string
    Examples:
        >>> normalize_reported_date('Fri, 5 Jul 2018 17:15:23 +0200')
        '2018-07-05T17:15:23'
        >>> normalize_reported_date('BMS Crude & Fuel')
    """
    date_match = re.match(r'.*:\d+(?!\+)', raw_date)
    return to_isoformat(date_match.group(0)) if date_match else None


def normalize_check_dates(item, dates):
    """Check if the year of each date is similar to the reported year.
        Args:
            item (Dict[str, str])
            dates : List of str

        Returns:
            Dict[str, str]:  item
        Examples:
            >>> normalize_check_dates({'arrival': datetime(1990, 5, 23, 12, 5),\
             'reported_date': '2020-07-05T17:15'}, ['arrival'])
            {'arrival': datetime.datetime(2020, 5, 23, 12, 5), 'reported_date': '2020-07-05T17:15'}
            >>> normalize_check_dates({'arrival': datetime(2020, 5, 23, 12, 5),\
             'reported_date': '2020-07-05T17:15'}, ['arrival'])
            {'arrival': datetime.datetime(2020, 5, 23, 12, 5), 'reported_date': '2020-07-05T17:15'}
        """
    reported_year = int(re.match(r'(\d+)', item.get('reported_date')).group(0))
    for key in dates:
        if item.get(key) is not None:
            date = item.get(key)
            if isinstance(date, datetime):
                # assume absolute difference of 1 year
                # example : reporting in December for vessel arrivals in next January
                if abs(date.year - reported_year) > 1:
                    date = date.replace(year=reported_year)
                    item[key] = date
    return item
