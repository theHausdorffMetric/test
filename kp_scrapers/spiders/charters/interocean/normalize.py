import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.utils import map_keys
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
    if not item['vessel']:
        return

    # build cargo model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.kilotons,
    }

    if not item['cargo']['product']:
        item.pop('cargo')

    item['departure_zone'], item['arrival_zone'] = normalize_voyage(item.pop('voyage'))
    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    return item


def field_mapping():
    return {
        '0': ('vessel', lambda x: {'name': x} if 'TBN' not in x.split() else None),
        '1': ('cargo_volume', None),
        '2': ('cargo_product', None),
        '3': ('voyage', None),
        '4': ('lay_can', None),
        '5': ('rate_value', None),
        '6': ('charterer', None),
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

        lay_can_start = _build_lay_can_date(start_day, month, year)
        lay_can_end = _build_lay_can_date(end_day, month, year) if end_day else lay_can_start

        # if it's rollover case
        if lay_can_start > lay_can_end:
            lay_can_start = lay_can_start + relativedelta(months=-1)

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
    if '12' == month and 'Jan' in reported:
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


def normalize_voyage(raw_voyage):
    """Normalize departure zone and arrival zone from voyage field.

    Args:
        raw_voyage (str):

    Returns:
        Tuple[str, List[str]]:

    """
    departure, _, arrival = raw_voyage.partition('/')
    return departure, arrival.split('-')
