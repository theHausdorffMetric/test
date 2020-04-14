from datetime import datetime, timedelta
import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


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

    # build Vessel sub-model
    if not item['vessel_name']:
        return

    item['vessel'] = {'name': item.pop('vessel_name'), 'build_year': item.pop('build_year')}

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    return item


def field_mapping():
    return {
        'BUILT': ('build_year', normalize_build_year),
        'CHTRS': ('charterer', None),
        'DIS PORT': ('arrival_zone', lambda x: x.split('+')),
        'FREIGHT': ('rate_value', None),
        'L. PORT': ('departure_zone', None),
        'LAYCAN': ('lay_can', None),
        'LC': ('lay_can', None),
        'OWNERS': ignore_key('owner'),
        'provider_name': ('provider_name', None),
        'QTY.': ('cargo', normalize_cargo),
        'reported_date': ('reported_date', None),
        'VESSEL': ('vessel_name', lambda x: x if 'TBN' not in x.split() else None),
    }


def normalize_build_year(raw_value):
    """Normalize build year.

    This field might present in the following formats:
        - (17), two digits year representation with brackets at each side
        - '', empty string as the vessel is `TBN`

    Examples:
        >>> normalize_build_year('(09)')
        '2009'
        >>> normalize_build_year('')

    Args:
        raw_value (str):

    Returns:
        str:

    """
    return str(datetime.strptime(raw_value, '(%y)').year) if raw_value else None


def normalize_cargo(raw_value):
    """Normalize cargo.

    This field might be presented as:
        - 130, with only quantity info
        - 80FO, quantity and cargo product

    We'll extract and build cargo model for the second pattern.

    Examples:
        >>> normalize_cargo('80COND')
        {'product': 'COND', 'movement': 'load', 'volume': '80', 'volume_unit': 'kilotons'}
        >>> normalize_cargo('80 FO')
        {'product': 'FO', 'movement': 'load', 'volume': '80', 'volume_unit': 'kilotons'}
        >>> normalize_cargo('130')

    Args:
        raw_value (str):

    Returns:
        Dict[str, str]: cargo dict

    """
    _match_cargo = re.match(r'(\d+)\s*([A-Z]+)', raw_value.upper())
    if _match_cargo:
        volume, product = _match_cargo.groups()
        return {
            'product': product,
            'movement': 'load',
            'volume': volume,
            'volume_unit': Unit.kilotons,
        }


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Based on current report, lay can date comes with the following pattern:
        - 25/12,

    Examples:
        >>> normalize_lay_can('28/12', '19 Dec 2018')
        ('2018-12-28T00:00:00', '2018-12-28T00:00:00')
        >>> normalize_lay_can('04/01', '19 Dec 2018')
        ('2019-01-04T00:00:00', '2019-01-04T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        str | datetime.datetime | None: lay can start date
        str | datetime.datetime | None: lay can end date

    """
    if not raw_lay_can:
        return None, None

    reported = parse_date(reported)
    _match_day = re.match(r'(\d{1,2})\/(\d{1,2})', raw_lay_can)
    if _match_day:
        day, month = _match_day.groups()

        lay_can_start = _build_lay_can_date(day, month, reported.year)
        if lay_can_start - reported < timedelta(days=-7):
            lay_can_start = lay_can_start + relativedelta(years=1)

        return lay_can_start.isoformat(), lay_can_start.isoformat()

    logger.error(f'New lay can date pattern: {raw_lay_can}, at reported date: {reported}')
    return None, None


def _build_lay_can_date(day, month, year):
    """Build lay can date.

    Args:
        day (int | str):
        month (str):
        year (int):

    Returns:
        datetime.datetime: time in ISO 8601 format

    """
    return parse_date(f'{day} {month} {year}', dayfirst=True)
