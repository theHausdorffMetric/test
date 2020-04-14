import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

MOVEMENT = 'load'
ARRIVAL_ZONE = ['South-East Asia']


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
    item['vessel'] = {'name': item.pop('vessel')}

    # build Cargo sub-model
    item['cargo'] = {
        'product': item.pop('product', None),
        'movement': MOVEMENT,
        'volume': item.pop('volume', None),
        'volume_unit': Unit.kilotons,
    }

    # the arrival zone value is hard-coded since the report is for a specific zone
    # as per discussion with PO.
    item['arrival_zone'] = ARRIVAL_ZONE

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    return item


def field_mapping():
    return {
        '0': ignore_key('arrival_time_or_status'),
        '1': ('vessel', None),
        '2': ignore_key('volume_product_combined'),
        '3': ('lay_can', None),
        '4': ('departure_zone', None),
        '5': ('rate_value', None),
        '6': ('charterer', lambda x: x.replace('?', '')),
        '7': ('volume', None),
        '8': ('product', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Lay can date pattern:
        - Normal pattern: 03/12, 10-12/12
        - Rollover pattern: 30-01/12

    Examples:
        >>> normalize_lay_can('03/12', '26 Nov 2018')
        ('2018-12-03T00:00:00', '2018-12-03T00:00:00')
        >>> normalize_lay_can('10-12/12', '26 Nov 2018')
        ('2018-12-10T00:00:00', '2018-12-12T00:00:00')
        >>> normalize_lay_can('30-01/12', '26 Nov 2018')
        ('2018-11-30T00:00:00', '2018-12-01T00:00:00')
        >>> normalize_lay_can('28-01/01', '27 Dec 2018')
        ('2018-12-28T00:00:00', '2019-01-01T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    _match = re.match(r'(\d{1,2})-?(\d{1,2})?/(\d{1,2})', raw_lay_can)
    if _match:
        start_day, end_day, month = _match.groups()
        year = _get_lay_can_year(month, reported)
        # to handle roll over case before converting them to date type
        start_month = (
            _get_previous_month(month) if end_day and int(start_day) > int(end_day) else month
        )
        # this block is to safeguard from handling dates like '29/02'
        try:
            lay_can_start = _build_lay_can_date(start_day, start_month, year)
            lay_can_end = _build_lay_can_date(end_day, month, year) if end_day else lay_can_start
        except (ValueError):
            logger.warning(f'Invalid date entry in the report: {raw_lay_can}')
            return None, None

        # if it's rollover case, this is to rewind the year if the dates are between two years
        if lay_can_start > lay_can_end:
            lay_can_start = lay_can_start + relativedelta(years=-1)

        return lay_can_start.isoformat(), lay_can_end.isoformat()

    logger.error(f'Invalid or new lay can date pattern: {raw_lay_can}')


def _get_lay_can_year(month, reported):
    """Get lay can year.

    Args:
        month (str):
        reported (str):

    Returns:
        int:

    """
    year = parse_date(reported).year
    if ('12' == month or '11' == month) and 'Jan' in reported:
        year -= 1
    if ('01' == month or '1' == month) and 'Dec' in reported:
        year += 1
    return year


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


def _get_previous_month(month):
    return int(month) - 1 if int(month) != 1 else 12
