import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import is_isoformat, to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # remove tbn vessels or no names
    if not item['vessel']['name'] or 'TBN' in item['vessel']['name']:
        return

    # get lay can dates and normalize patterns
    item['lay_can_start'] = normalize_lay_can(
        item.get('lay_can_start_1') if item.get('lay_can_start_1') else item.get('lay_can_start_2'),
        item['reported_date'],
    )

    # build proper Cargo model
    item['cargo'] = {
        'product': 'Petcoke',
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.tons,
        'seller': {'name': item.pop('cargo_seller', None)},
    }

    for col in ('lay_can_start_1', 'lay_can_start_2'):
        item.pop(col, None)

    return item


def field_mapping():
    return {
        'Vessel Name': ('vessel', lambda x: {'name': may_strip(x) if x else None}),
        'Laycan': ignore_key('redundant dates'),
        'Charterer': ('charterer', None),
        'Owner / Operator': ignore_key('owner and operator field'),
        'Supplier': ('cargo_seller', None),
        'Cargo Type': ignore_key('cargo type'),
        'Cargo (MT)': ('cargo_volume', lambda x: normalize_cargo_volume(x)),
        'ETA Anchorage': ('lay_can_start_1', None),
        'ETA Berth': ('lay_can_start_2', None),
        'Location': ignore_key('location'),
        'Description': ignore_key('description'),
        'ETS': ignore_key('ets'),
        'Next Port': ('arrival_zone', lambda x: [x] if x else None),
        'port_name': ('departure_zone', lambda x: normalize_departure_zone(x)),
        'provider_name': ('provider_name', may_strip),
        'reported_date': ('reported_date', lambda x: parse_date(x).strftime('%d %b %Y')),
    }


def normalize_departure_zone(raw_port_name):
    """Extract departure zone from email title

    Examples:
        >>> normalize_departure_zone('Houston Petcoke Linup')
        'Houston'

    Args:
        raw_port_name (str):

    Returns:
        str:
    """
    return raw_port_name.partition(' ')[0]


def normalize_lay_can(date_item, rpt_date):
    """Transform non isoformat dates to isoformat

    Examples:
        >>> normalize_lay_can('8/14 PM', '14 Aug 2019')
        '2019-08-14T00:00:00'
        >>> normalize_lay_can('2018-02-04T00:00:00', '14 Aug 2019')
        '2018-02-04T00:00:00'
        >>> normalize_lay_can('12/31 PM', '29 Dec 2019')
        '2019-12-31T00:00:00'
        >>> normalize_lay_can('12/31 PM', '01 Jan 2020')
        '2019-12-31T00:00:00'
        >>> normalize_lay_can('01/01 PM', '31 Dec 2019')
        '2020-01-01T00:00:00'
        >>> normalize_lay_can('7/06/2015 1750', '31 Dec 2019')
        '2015-07-06T17:50:00'

    Args:
        date_item (str):

    Returns:
        str:
    """
    if is_isoformat(date_item):
        return date_item

    if not is_isoformat(date_item):
        year = parse_date(rpt_date, dayfirst=True).year
        _date = date_item.partition(' ')[0]
        if len(_date.split('/')) == 2:
            _month, _day = _date.split('/')
            if 'Dec' in rpt_date and (str(_month) == '1' or str(_month) == '01'):
                year += 1
            if 'Jan' in rpt_date and str(_month) == '12':
                year -= 1

            return to_isoformat(f'{_day} {_month} {year}', dayfirst=True)

        if len(_date.split('/')) == 3:
            _time = date_item.partition(' ')[2]
            _hour = _time.replace(' ', '')[:2] if _time.replace(' ', '')[:2].isdigit() else '00'
            _min = _time.replace(' ', '')[2:] if _time.replace(' ', '')[2:].isdigit() else '00'
            try:
                return to_isoformat(f'{_date} {_hour}:{_min}', dayfirst=False)
            except Exception:
                logger.error('Skipping date row: %s', date_item)
                return date_item

    logger.error('Skipping date row: %s', date_item)
    return date_item


def normalize_cargo_volume(raw_vol):
    """Transform non isoformat dates to isoformat

    Examples:
        >>> normalize_cargo_volume('30,000 MT')
        '30000'

    Args:
        raw_vol (str):

    Returns:
        str:
    """
    if raw_vol:
        return raw_vol.replace(',', '').partition(' ')[0]

    return None
