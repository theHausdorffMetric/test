import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.date import get_last_day_of_current_month
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.charters.berge import parser


VAGUE_DAY_MAPPING = {'BEG': (1, 7), 'MID': (14, 21)}

UNIT_MAPPING = {'MT': Unit.tons}

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

    item['cargo'] = normalize_cargo(item['cargo'])
    if not item['cargo']:
        # collect missing rows so that it will be accessible to analysts in #data-notification
        parser.MISSING_ROWS.append(
            ' '.join(
                [
                    raw_item['VESSEL'],
                    raw_item['CARGO'],
                    raw_item['POL'],
                    raw_item['POD'],
                    raw_item['LAYCAN'],
                    raw_item['FREIGHT'],
                    raw_item['ACCOUNT'],
                ]
            )
        )
        return

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    return item


def field_mapping():
    return {
        'VESSEL': ('vessel', lambda x: {'name': None if 'TBN' in x.split() else x}),
        'CARGO': ('cargo', None),
        'POL': ('departure_zone', None),
        'POD': ('arrival_zone', lambda x: [alias.strip() for alias in x.split('+')]),
        'LAYCAN': ('lay_can', None),
        'FREIGHT': ('rate_value', None),
        'ACCOUNT': ('charterer', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_cargo(raw_cargo):
    """Normalize cargo.

    Examples:
        >>> normalize_cargo('6,600 MT ETHY')
        {'product': 'ETHY', 'volume': '6600', 'volume_unit': 'tons', 'movement': 'load'}
        >>> normalize_cargo('2.500 MT BUT')
        {'product': 'BUT', 'volume': '2500', 'volume_unit': 'tons', 'movement': 'load'}
        >>> normalize_cargo('F&C ETHY+BUT')

    Args:
        raw_cargo:

    Returns:
        Dict[str, str]:

    """
    _match = re.match(r'([\d]+)\s+(MT)\s+(.+)', raw_cargo.replace(',', '').replace('.', ''))

    if _match:
        volume, unit, product = _match.groups()
        return {
            'product': product,
            'volume': volume,
            'volume_unit': UNIT_MAPPING.get(unit),
            'movement': 'load',
        }

    else:
        logger.warning(f'Unknown cargo pattern: {raw_cargo}')


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Lay can date pattern:
        1. Normal case: OCT 10/14
        2. Vague case: BEG NOV, END NOV
        3. Rollover case: APR 30/2 (30 April to 2 May)
        4. Normal case 2: 18/20 DEC


    Examples:
        >>> normalize_lay_can('BEG OCT', '15 Oct 2018')
        ('2018-10-01T00:00:00', '2018-10-07T00:00:00')
        >>> normalize_lay_can('END OCT', '15 NOV 2018')
        ('2018-10-24T00:00:00', '2018-10-31T00:00:00')
        >>> normalize_lay_can('OCT 10/14', '15 Oct 2018')
        ('2018-10-10T00:00:00', '2018-10-14T00:00:00')
        >>> normalize_lay_can('DEC 30/2', '15 Dec 2018')
        ('2018-12-30T00:00:00', '2019-01-02T00:00:00')
        >>> normalize_lay_can('JAN 2/4', '15 Dec 2018')
        ('2019-01-02T00:00:00', '2019-01-04T00:00:00')
        >>> normalize_lay_can('18/20 DEC', '18 Dec 2018')
        ('2018-12-18T00:00:00', '2018-12-20T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    if not raw_lay_can or raw_lay_can == 'DNR':
        return None, None

    _normal_match = re.match(r'([A-Za-z]{3,4}).(\d{1,2}).(\d{1,2})?', raw_lay_can)
    if _normal_match:
        month, start_day, end_day = _normal_match.groups()
        year = _get_lay_can_year(month, reported)

        lay_can_start = _build_lay_can_date(start_day, month, year)
        lay_can_end = _build_lay_can_date(end_day, month, year) if end_day else lay_can_start

        # detect if it's rollover
        if lay_can_start > lay_can_end:
            lay_can_end = lay_can_end + relativedelta(months=1)

        return lay_can_start.isoformat(), lay_can_end.isoformat()

    _normal_match_2 = re.match(r'(\d{1,2}).(\d{1,2})?.([A-Za-z]{3,4})', raw_lay_can)
    if _normal_match_2:
        start_day, end_day, month = _normal_match_2.groups()
        year = _get_lay_can_year(month, reported)

        lay_can_start = _build_lay_can_date(start_day, month, year)
        lay_can_end = _build_lay_can_date(end_day, month, year) if end_day else lay_can_start

        # detect if it's rollover
        if lay_can_start > lay_can_end:
            lay_can_end = lay_can_end + relativedelta(months=1)

        return lay_can_start.isoformat(), lay_can_end.isoformat()

    _vague_match = re.match(r'(BEG|MID|END).([A-Za-z]{3,4})', raw_lay_can.upper())
    if _vague_match:
        vague, month = _vague_match.groups()
        year = _get_lay_can_year(month, reported)
        start_day, end_day = VAGUE_DAY_MAPPING.get(vague, (None, None))
        if vague == 'END':
            end_day = get_last_day_of_current_month(month, year, '%b', '%Y')
            start_day = end_day - 7

        lay_can_start = _build_lay_can_date(start_day, month, year)
        lay_can_end = _build_lay_can_date(end_day, month, year)

        return lay_can_start.isoformat(), lay_can_end.isoformat()

    logger.error(f'New lay can pattern spotted: {raw_lay_can}, reported date: {reported}')
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
