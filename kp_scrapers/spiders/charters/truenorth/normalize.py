import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import get_last_day_of_current_month, to_isoformat
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


LAY_CAN_PATTERN = r'(\d{1,2}|[A-Z]{3,5})\D?(\d{1,2})?\D(\d{1,2})'

LAY_CAN_MAPPING = {'EARLY': (1, 7), 'ELY': (1, 7), 'MID': (14, 21)}

STATUS_MAPPING = {
    'FLD': SpotCharterStatus.failed,
    'RPTD': SpotCharterStatus.on_subs,
    'FFXD': SpotCharterStatus.fully_fixed,
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
    if item['product']:
        item['cargo'] = {
            'product': item.get('product'),
            'movement': 'load',
            'volume': item.get('volume'),
            'volume_unit': Unit.kilotons,
        }
    item.pop('product')
    item.pop('volume')

    item['charterer'], item['status'] = normalize_charterer(item.pop('charterer_status'))
    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    return item


def field_mapping():
    return {
        '0': ('vessel', lambda x: {'name': x} if 'TBN' not in x.split() else None),
        '1': ('volume', None),
        '2': ('product', None),
        '3': ('lay_can', None),
        '4': ('departure_zone', None),
        '5': ('arrival_zone', lambda x: x.replace('-', '-').split('-')),
        '6': ('rate_value', None),
        '7': ('charterer_status', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Existing pattern found:
    1. 26-27/9, 17/10
    2. END/9

    No cross month pattern found in current reports, so we don't handle it for now until we
    encountered one, which would fail in validation and we'll know.

    Examples:
        >>> normalize_lay_can('26-27/9', '13 Sep 2018')
        ('2018-09-26T00:00:00', '2018-09-27T00:00:00')
        >>> normalize_lay_can('17/10', '13 Sep 2018')
        ('2018-10-17T00:00:00', '2018-10-17T00:00:00')
        >>> normalize_lay_can('END/9', '13 Sep 2018')
        ('2018-09-23T00:00:00', '2018-09-30T00:00:00')
        >>> normalize_lay_can('29/12', '1 Jan 2019')
        ('2018-12-29T00:00:00', '2018-12-29T00:00:00')
        >>> normalize_lay_can('END/1', '31 Dec 2018')
        ('2019-01-24T00:00:00', '2019-01-31T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    match = re.match(LAY_CAN_PATTERN, raw_lay_can)
    if not match:
        logger.warning(f'Invalid lay can date: {raw_lay_can}')
        return None, None

    start_day, end_day, month = match.groups()

    # month rollover for lay can start
    year = parse_date(reported).year
    if '12' == month and 'Jan' in reported:
        year -= 1
    if month == '1' or month == '01' and 'Dec' in reported:
        year += 1

    # handle vague day cases
    if 'END' in start_day:
        end_day = get_last_day_of_current_month(month, str(year), '%m', '%Y')
        start_day = end_day - 7

    if start_day in LAY_CAN_MAPPING:
        start_day, end_day = LAY_CAN_MAPPING.get(start_day)

    start = to_isoformat(f'{start_day} {month} {year}', dayfirst=True)
    end = to_isoformat(f'{end_day} {month} {year}', dayfirst=True) if end_day else start

    return start, end


def normalize_charterer(charterer_fuzzy):
    """Normalize charterer and remove status info.

    Examples:
        >>> normalize_charterer('VITOL - RPTD')
        ('VITOL ', 'On Subs')
        >>> normalize_charterer('LITASCO - FFXD')
        ('LITASCO ', 'Fully Fixed')
        >>> normalize_charterer('SHELL')
        ('SHELL', None)

    Args:
        charterer_fuzzy: with possible status info inside.

    Returns:
        Tuple[str, str]:

    """
    charterer, _, status_fuzzy = charterer_fuzzy.partition('-')

    status = None
    for s in STATUS_MAPPING:
        if s in status_fuzzy:
            status = STATUS_MAPPING.get(s)

    return charterer, status
