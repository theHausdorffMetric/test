import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import get_last_day_of_current_month
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


VAGUE_DAY_MAPPING = {'ELY': (1, 7), 'EARLY': (1, 7), 'MID': (14, 21)}

MONTH_MAPPING = {
    "JANUARY": "JAN",
    "FEBRUARY": "FEB",
    "MARCH": "MAR",
    "APRIL": "APR",
    "MAY": "MAY",
    "JUNE": "JUN",
    "JULY": "JUL",
    "AUGUST": "AUG",
    "SEPTEMBER ": "SEP",
    "OCTOBER": "OCT",
    "NOVEMBER": "NOV",
    "DECEMBER ": "DEC",
}

logger = logging.getLogger(__name__)


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    if not item['vessel']:
        return

    # build cargo sub-model
    item['cargo'] = {
        'product': item.pop('product'),
        'volume': item.pop('volume'),
        'volume_unit': Unit.cubic_meter,
        'movement': 'load',
    }

    item['departure_zone'], item['arrival_zone'] = normalize_voyage(item.pop('voyage'))
    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    return item


def field_mapping():
    return {
        '0': ('vessel', lambda x: {'name': x} if 'TBN' not in x.split() else None),
        '1': ('volume', lambda x: ''.join(re.findall(r'\d*', x))),
        '2': ('product', lambda x: x.split()[-1]),
        '3': ('voyage', None),
        '4': ('lay_can', None),
        '5': ('rate_value', None),
        '6': ('charterer', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_voyage(raw_voyage):
    """Normalize departure zone and arrival zone from voyage field.

    Args:
        raw_voyage (str):

    Returns:
        Tuple[str, List[str]]:

    """
    departure, _, arrival = raw_voyage.partition('/')
    return departure.strip(), [arr.strip() for arr in arrival.split('+')]


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Lay can date pattern spotted so far:
        - Normal case:
            25/26 NOV
            2 / 3 NOV
            25 OCT-27 OCT
            17/4
            15-20 APR
            29 June - 01 July
        - Vague case: END OCT
        - Rollover case: 31 OCT - 02 NOV

    Note that spaces would appear anywhere in lay can date.We have three date patterns after
    removing all the blanks:
        1. 25OCT-27OCT,  31OCT-02NOV
        2. 25/26NOV, 25NOV
        3. ENDOCT
        4. 17/4

    Examples:
        >>> normalize_lay_can('25/26 NOV', '18 Oct 2018')
        ('2018-11-25T00:00:00', '2018-11-26T00:00:00')
        >>> normalize_lay_can('END OCT', '18 Oct 2018')
        ('2018-10-24T00:00:00', '2018-10-31T00:00:00')
        >>> normalize_lay_can('31 OCT - 02 NOV', '18 Oct 2018')
        ('2018-10-31T00:00:00', '2018-11-02T00:00:00')
        >>> normalize_lay_can(' 28/30 OCT', '18 Oct 2018')
        ('2018-10-28T00:00:00', '2018-10-30T00:00:00')
        >>> normalize_lay_can('28 DEC - 3 JAN', '18 Dec 2018')
        ('2018-12-28T00:00:00', '2019-01-03T00:00:00')
        >>> normalize_lay_can('26-28 MAR', '22 Mar 2019')
        ('2019-03-26T00:00:00', '2019-03-28T00:00:00')
        >>> normalize_lay_can('17/4', '22 Mar 2019')
        ('2019-04-17T00:00:00', '2019-04-17T00:00:00')
        >>> normalize_lay_can('END APRIL', '22 Mar 2019')
        ('2019-04-23T00:00:00', '2019-04-30T00:00:00')


    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    # remove all the blanks as it would appear anywhere
    lay_can = raw_lay_can.replace(' ', '')

    # matches 25 OCT - 27 OCT
    _match_1 = re.match(r'(\d{1,2})([A-z]{3,5})-(\d{1,2})([A-z]+)', lay_can)
    if _match_1:
        start_day, start_month, end_day, end_month = _match_1.groups()
        start_month = MONTH_MAPPING.get(start_month, start_month)
        end_month = MONTH_MAPPING.get(end_month, end_month)
        start_year = _get_lay_can_year(start_month, reported)
        end_year = _get_lay_can_year(end_month, reported)

        lay_can_start = _build_lay_can_date(start_day, start_month, start_year)
        lay_can_end = _build_lay_can_date(end_day, end_month, end_year)

        return lay_can_start.isoformat(), lay_can_end.isoformat()

    # matches 25/26 NOV, 25-26 NOV, 25 NOV
    _match_2 = re.match(r'(\d{1,2})[\/\-]?(\d{1,2})?([A-z]+)', lay_can)
    if _match_2:
        start_day, end_day, month = _match_2.groups()
        month = MONTH_MAPPING.get(month, month)
        year = _get_lay_can_year(month, reported)

        lay_can_start = _build_lay_can_date(start_day, month, year)
        lay_can_end = _build_lay_can_date(end_day, month, year) if end_day else lay_can_start

        # skip month rollover case as we're not sure how they do this yet
        return lay_can_start.isoformat(), lay_can_end.isoformat()

    # matches 4/7
    _match_3 = re.match(r'(\d{1,2})[\/-]?(\d{1,2})', lay_can)
    if _match_3:
        start_day, month = _match_3.groups()
        end_day = None
        year = _get_lay_can_year(month, reported)

        lay_can_start = _build_lay_can_date(start_day, month, year)
        lay_can_end = _build_lay_can_date(end_day, month, year) if end_day else lay_can_start

        return lay_can_start.isoformat(), lay_can_end.isoformat()

    # matches END OCT etc.
    _match_4 = re.match(r'(ELY|EARLY|MID|END)([A-z]+)', lay_can)
    if _match_4:
        vague, month = _match_4.groups()
        month = MONTH_MAPPING.get(month, month)
        year = _get_lay_can_year(month, reported)
        start_day, end_day = VAGUE_DAY_MAPPING.get(vague, (None, None))
        if vague == 'END':
            end_day = get_last_day_of_current_month(month, year, '%b', '%Y')
            start_day = end_day - 7

        lay_can_start = _build_lay_can_date(start_day, month, year)
        lay_can_end = _build_lay_can_date(end_day, month, year)

        return lay_can_start.isoformat(), lay_can_end.isoformat()

    logger.error(f'Unknown lay can pattern: {raw_lay_can}, reported date: {reported}')
    return None, None


def _get_lay_can_year(month, reported):
    """Get the year of lay can date with reference of reported date.

    Args:
        month:
        reported:

    Returns:
        int:

    """
    year = parse_date(reported).year
    if 'DEC' == month and 'Jan' in reported:
        year -= 1
    if 'JAN' == month and 'Dec' in reported:
        year += 1
    return year


def _build_lay_can_date(day, month, year):
    """Build lay can date.

    Args:
        day:
        month:
        year:

    Returns:
        datetime.datetime:

    """
    return parse_date(f'{day} {month} {year}', dayfirst=True)
