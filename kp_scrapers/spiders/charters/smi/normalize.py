import calendar
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import get_last_day_of_current_month, to_isoformat
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


VAGUE_DAY_MAPPING = {'ELY': ('1', '7'), 'MID': ('14', '21')}

EMPTY_LAY_CAN_DATE = {'--', 'DNR', 'PPT'}

ARRIVAL_ZONE_SEPARATOR = ['-', '+']

IRRELEVANT_CHARTERER = ['FLD']

IRRELEVANT_VESSEL = ['TBN']

logger = logging.getLogger(__name__)


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        SpotCharter | None

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    if not item['vessel'] or not item['charterer']:
        return

    item['departure_zone'], item['arrival_zone'] = normalize_departure_arrival_zone(
        item.pop('departure_arrival_zone')
    )
    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    # build cargo sub-model, product might be empty
    if item['product']:
        item['cargo'] = {
            'product': item['product'],
            'movement': 'load',
            'volume': item['volume'],
            'volume_unit': Unit.kilotons,
        }
    item.pop('product')
    item.pop('volume')

    return item


def field_mapping():
    return {
        'Vessel': (
            'vessel',
            lambda x: {'name': normalize_vessel_name(x)} if normalize_vessel_name(x) else None,
        ),
        'Size': ('volume', None),
        'Cargo': ('product', None),
        'Voyage': ('departure_arrival_zone', None),
        'Laycan': ('lay_can', None),
        'Rate': ('rate_value', None),
        'Charterer': ('charterer', normalize_charterer),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can start date with the year of reported date as reference.

    Lay can start date format:
        1. 21/SEP, 21/SEPT (normal case)
        2. END/AUG, ELY/SEP (vague case)
        3. --/SEP, DNR, PPT (invalid case)

    Examples:
        >>> normalize_lay_can('--/SEP', '27 Aug 2018')
        (None, None)
        >>> normalize_lay_can('DNR', '27 Aug 2018')
        (None, None)
        >>> normalize_lay_can('PPT', '27 Aug 2018')
        (None, None)
        >>> normalize_lay_can('END/AUG', '27 Aug 2018')
        ('2018-08-25T00:00:00', '2018-08-31T00:00:00')
        >>> normalize_lay_can('ELY/SEP', '27 Aug 2018')
        ('2018-09-01T00:00:00', '2018-09-07T00:00:00')
        >>> normalize_lay_can('21/SEPT', '27 Aug 2018')
        ('2018-09-21T00:00:00', '2018-09-21T00:00:00')
        >>> normalize_lay_can('END/Dec', '1 Jan 2019')
        ('2018-12-25T00:00:00', '2018-12-31T00:00:00')
        >>> normalize_lay_can('ELY/Jan', '28 Dec 2018')
        ('2019-01-01T00:00:00', '2019-01-07T00:00:00')
        >>> normalize_lay_can('END/Feb', '20 Feb 2019')
        ('2019-02-22T00:00:00', '2019-02-28T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        str:

    """
    # check empty date
    if any(pattern in raw_lay_can for pattern in EMPTY_LAY_CAN_DATE):
        return None, None

    _normal_match = re.match(r'(\d{1,2}).([A-Za-z]{3,4})', raw_lay_can)
    if _normal_match:
        day, month = _normal_match.groups()
        year = _get_lay_can_year(month, reported)
        lay_can_date = to_isoformat(f'{day} {month} {year}', dayfirst=True)
        return lay_can_date, lay_can_date

    _vague_match = re.match(r'(ELY|MID|END).([A-Za-z]{3,4})', raw_lay_can)
    if _vague_match:
        vague_day, month = _vague_match.groups()
        year = _get_lay_can_year(month, reported)
        start_day, end_day = VAGUE_DAY_MAPPING.get(vague_day, (None, None))
        if not start_day:
            end_day = get_last_day_of_current_month(month, year, '%b', '%Y')
            start_day = end_day - 6

        lay_can_start = to_isoformat(f'{start_day} {month} {year}', dayfirst=True)
        lay_can_end = to_isoformat(f'{end_day} {month} {year}', dayfirst=True)
        return lay_can_start, lay_can_end

    logger.error(f'Unknown lay can date format: {raw_lay_can}')
    return None, None


def _get_lay_can_year(month, reported):
    """Get lay can month with reference of reported year.

    Args:
        month (str):
        reported (str):

    Returns:

    """
    year = parse_date(reported).year
    month = month.upper()
    if 'DEC' in month and 'Jan' in reported:
        year -= 1
    # Year shift Jan case
    if 'JAN' in month and 'Dec' in reported:
        year += 1

    return year


def _day_mapping(flag, month, year):
    """Extract the start day and end day.

    Args:
        flag (str): ELY | MID | END
        month (str):
        year (int):

    Returns:

    """
    if flag in VAGUE_DAY_MAPPING:
        return VAGUE_DAY_MAPPING[flag]

    last_day = calendar.monthrange(year, parse_date(month).month)[1]
    return str(last_day - 6), str(last_day)


def normalize_departure_arrival_zone(raw_voyage):
    """Normalize departure zone and arrival zones.

    Examples:
        >>> normalize_departure_arrival_zone('USG/UKC-MED')
        ('USG', ['UKC', 'MED'])
        >>> normalize_departure_arrival_zone('ST. LUCIA/STS ARUBA')
        ('ST. LUCIA', ['STS ARUBA'])
        >>> normalize_departure_arrival_zone('MAP TA PHUT')
        ('MAP TA PHUT', None)
        >>> normalize_departure_arrival_zone('ALGERIA/VADINAR+MUMBAI')
        ('ALGERIA', ['VADINAR', 'MUMBAI'])

    Args:
        raw_voyage (str):

    Returns:
        Tuple[str, List[str]]

    """
    departure_zone, _, arrival_zone = raw_voyage.partition('/')

    if not arrival_zone:
        return departure_zone, None

    for separator in ARRIVAL_ZONE_SEPARATOR:
        if separator in arrival_zone:
            return departure_zone, arrival_zone.split(separator)

    return departure_zone, [arrival_zone]


def normalize_vessel_name(raw_vessel):
    """Filter vessel and remove irrelevant letters.

    Examples:
        >>> normalize_vessel_name('HEIDMAR TBN')
        >>> normalize_vessel_name('BTTBN')
        'BTTBN'
        >>> normalize_vessel_name('TATAKI O/O')
        'TATAKI'

    Args:
        raw_vessel (str):

    Returns:
        str:

    """
    for each in IRRELEVANT_VESSEL:
        if each in raw_vessel.split():
            return

    # remove characters
    match = re.search(r'(?:O/O|P/C|C/C|S/S)', raw_vessel)
    return raw_vessel[: match.span()[0]].strip() if match else raw_vessel


def normalize_charterer(raw_charterer):
    """Filter charterer and remove irrelevant letters.

    Examples:
        >>> normalize_charterer('MERCURIA-FLD')
        >>> normalize_charterer('SK ENERGY-RPLC')
        'SK ENERGY'

    Args:
        raw_charterer (str):

    Returns:
        str:

    """
    charterer, _, flag = raw_charterer.partition('-')
    return charterer if flag not in IRRELEVANT_CHARTERER else None
