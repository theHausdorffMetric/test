import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import get_first_day_of_next_month, to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_DATE = ['ATT BERTH', 'ORDERS', 'TBC']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Process item of ships schedule pdf.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, portcall_mapping())
    # discard items without cargo info
    if not item['cargoes'][0]:
        logger.info(f'Vessel {raw_item["SHIP NAME"]} has irrelevant cargo {raw_item["OPERATIONS"]}')
        return

    # build vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_length', None)}

    # build a proper ETA date
    item['eta'] = normalize_eta_and_departure(item['eta'], item['reported_date'])
    if not item['eta']:
        logger.info(f'Vessel {raw_item["SHIP NAME"]} has no portcall date')
        return

    return item


def portcall_mapping():
    return {
        'ARRIVAL DATE': ('eta', None),
        'SHIP NAME': ('vessel_name', None),
        'VOY': ignore_key('voyage'),
        'LOA': ('vessel_length', None),
        'DRAFT': ignore_key('draught'),
        'FLAG': ignore_key('flag'),
        'AGENT': ignore_key('agent'),
        'LINE': ignore_key('line'),
        'SJTP': ignore_key('berth'),
        'BERTH': ignore_key('berth'),
        'OPERATIONS': ('cargoes', normalize_cargoes),
        'CALL': ignore_key('call'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
    }


def normalize_eta_and_departure(date_str, reported):
    """Normalize eta with reference of reported date.

    As eta and departure only contains day and time, we need to get month and year from reported
    date. The format:
        1) dd/hhmm: 11/2100
        2) dd/AM: 12/AM
        3) dd/PM: 14/PM
        4) ORDERS
        5) ATT BERTH

    Tested pattern ref: https://regex101.com/r/tUA1MZ/1/

    Here's the strategy for dealing with month and year:
    1. If dd is bigger or equal to reported day, it represents the same month as reported date;
    2. Else, it is next month of dd. One thing should be noted: if reported month is December,
    next month would be January of next year
    3. There might be the eta is not the above format, we should be able to detect and report it.

    Examples:
        >>> normalize_eta_and_departure('11/2100', '2018-10-11T00:00:00')
        '2018-10-11T21:00:00'
        >>> normalize_eta_and_departure('03/1800', '2018-10-11T00:00:00')
        '2018-11-03T18:00:00'
        >>> normalize_eta_and_departure('24/AM', '2018-10-11T00:00:00')
        '2018-10-24T00:00:00'
        >>> normalize_eta_and_departure('03/1800', '2018-12-28T00:00:00')
        '2019-01-03T18:00:00'
        >>> normalize_eta_and_departure('23AM', '2018-10-11T00:00:00')
        '2018-10-23T00:00:00'

    Args:
        date_str (str):
        reported (str): ISO 8601 format

    Returns:
        str | None: ISO 8601 format

    """
    if date_str in IRRELEVANT_DATE or not date_str:
        return None

    _match = re.match(r'(\d{1,2})/?(\d{4})?', date_str)
    if not _match:
        logger.exception(f'Unknown date pattern: {date_str}')
        return None

    day, time = _match.groups()
    _ref = parse_date(reported)

    # current day is less than reported day, it's next month
    if int(day) < _ref.day:
        _ref = get_first_day_of_next_month(_ref)

    return to_isoformat(f'{day} {_ref.month} {_ref.year} {time if time else ""}', dayfirst=True)


def normalize_arrival(arrival, reported):
    """Normalize arrival with reference of reported date.

    Arrival date pattern:
        1) 20/09 AT 1720
        2) 03/03/15 AT 1200
        3) 23/11 /15 AT 1300

    Tested pattern ref: https://regex101.com/r/tUA1MZ/3

    Examples:
        >>> normalize_arrival('20/09 AT 1720', '2018-10-11T00:00:00')
        '2018-09-20T17:20:00'
        >>> normalize_arrival('03/03/15 AT 1200', '2018-10-11T00:00:00')
        '2015-03-03T12:00:00'

    Args:
        arrival (str):
        reported (str): ISO 8601 format

    Returns:
        str: ISO 8601 format

    """
    _match = re.match(r'(\d{1,2})\D*(\d{1,2})\D*?(\d{1,2})?\D*(\d{4})', arrival)
    if not _match:
        logger.exception(f'Unknown date pattern: {arrival}')
        return None

    day, month, year, time = _match.groups()
    year = year if year else parse_date(reported).year

    return to_isoformat(f'{day} {month} {year} {time}', dayfirst=True)


def normalize_cargoes(cargo_strs):
    """Normalize cargo information for each vessel.

    Strings like down below are where we can extract cargo info
    D <number> MT/MTS <cargo>
    <number> MT/MTS <cargo>

    Args:
        cargo_strs (str):

    Returns:
        List[Cargo]: cargo item list

    """
    return [_extract_cargo(c) for c in may_strip(cargo_strs).split('+')]


def _extract_cargo(cargo_str):
    """Assemble cargo.

    Args:
        cargo_str (str):

    Returns:
        Cargo:

    """
    units = [' MT ', ' MTS ']

    for unit in units:
        if unit in cargo_str:
            volume, product = cargo_str.split(unit)
            movement = 'load'
            if 'D' in volume:
                movement = 'discharge'
            return {
                'volume': _normalize_volume(volume),
                'volume_unit': 'tons',
                'product': product.replace('OF ', ''),
                'movement': movement,
            }


def _normalize_volume(volume_str):
    """Extract volume information.

    Format:
    1. D 6 888
    2. 7 980

    D is for discharge. There are blanks between volume numbers, and D should be removed.

    Examples:
        >>> _normalize_volume('D 6 888')
        '6888'
        >>> _normalize_volume('7 980')
        '7980'

    Args:
        volume_str (str):

    Returns:
        float: volume

    """
    return volume_str.replace(' ', '').replace('D', '')
