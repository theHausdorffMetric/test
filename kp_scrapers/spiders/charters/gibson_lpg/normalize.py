import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.date import get_last_day_of_current_month
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


VAGUE_DAY_MAPPING = {'EARLY': (1, 7), 'ELY': (1, 7), 'MID': (14, 21)}

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

    # when vessel name and volume cbm are joint
    if item.get('vessel_name_volume_cbm'):
        vessel_name, volume_cbm = normalize_vessel_name_and_volume_cbm(
            item.pop('vessel_name_volume_cbm')
        )
    else:
        vessel_name, volume_cbm = item.pop('vessel_name'), item.pop('volume_cbm')

    # skip irrelevant vessel
    if not vessel_name:
        return

    # build vessel sub-model
    item['vessel'] = {'name': vessel_name}

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can', None), item['reported_date']
    )

    # build cargo sub-model
    item['cargo'] = {
        'product': item.pop('product', None),
        'volume': volume_cbm,
        'volume_unit': Unit.cubic_meter if volume_cbm else None,
        'movement': 'load',
    }

    return item


def field_mapping():
    return {
        'Vessel cbm': ('vessel_name_volume_cbm', None),
        'mts Cargo': ('product', normalize_product),
        'Vessel': ('vessel_name', lambda x: None if 'TBN' in x.split() else x),
        'cbm': ('volume_cbm', lambda x: x.replace(',', '')),
        'mts': ignore_key('ignore mtons, will take cbm as volume'),
        'Cargo': ('product', None),
        'Load': ('departure_zone', None),
        'Disch': (
            'arrival_zone',
            # if arrival zone is `Worldwide`, ignore
            lambda x: [may_strip(z) for z in x.split('/')] if not x.startswith('WW') else None,
        ),
        'Laycan': ('lay_can', None),
        'Rate': ('rate_value', None),
        'Charterer': ('charterer', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_product(mixed_volume_product):
    """Normalize volume and products.

    Examples:
        >>> normalize_product('12k + 8k C4 / C3')
        'C4/C3'
        >>> normalize_product('44,000 LPG')
        'LPG'
        >>> normalize_product('Up to F/C C3+')
        'C3+'

    Args:
        mixed_volume_product (str):

    Returns:
        Tuple[List[str], List[str]]: (product list, volume list)

    """
    product = re.sub(r'\s*/\s*', '/', mixed_volume_product).split()[-1]

    return product


def normalize_vessel_name_and_volume_cbm(mixed_name_volume):
    """Remove irrelevant string and normalize vessel name.

    Examples:
        >>> normalize_vessel_name_and_volume_cbm('Pacific Shanghai 84,000')
        ('Pacific Shanghai', 84000)
        >>> normalize_vessel_name_and_volume_cbm('Equinor TBN 3-5,000')
        (None, None)

    Args:
        mixed_name_volume:

    Returns:
        Tuple[str, int]: (vessel name, volume cbm)

    """
    _split = mixed_name_volume.split()
    vessel_name = None if 'TBN' in _split[:-1] else may_strip(' '.join(_split[:-1]))
    volume_cbm = try_apply(_split[-1].replace(',', ''), int)
    return vessel_name, volume_cbm


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Lay can pattern:
        1. Normal pattern: 20-21 Nov, 17-Nov
        2. Vague pattern: early Nov, mid Oct, end Oct
        3. Rollover pattern: 29 SEP TO 01 OCT
        4. Rollover pattern: 29-01 Oct

    Examples:
        >>> normalize_lay_can('Mid Nov', '19 Oct 2018')
        ('2018-11-14T00:00:00', '2018-11-21T00:00:00')
        >>> normalize_lay_can('20-25 July', '26 Jul 2019')
        ('2019-07-20T00:00:00', '2019-07-25T00:00:00')
        >>> normalize_lay_can('End July', '26 Jul 2019')
        ('2019-07-24T00:00:00', '2019-07-31T00:00:00')
        >>> normalize_lay_can('29-01 Oct', '19 Oct 2018')
        ('2018-10-29T00:00:00', '2018-11-01T00:00:00')
        >>> normalize_lay_can('29 DEC TO 01 JAN', '19 Oct 2018')
        ('2018-12-29T00:00:00', '2019-01-01T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    if not raw_lay_can or raw_lay_can == 'DNR':
        return None, None

    raw_lay_can = raw_lay_can.upper()
    _rollover_match = re.match(r'(\d{1,2}) ([A-Z]{3,4}) TO (\d{1,2}) ([A-Z]{3,4})', raw_lay_can)
    if _rollover_match:
        start_day, start_month, end_day, end_month = _rollover_match.groups()
        year = _get_lay_can_year(start_month, reported)

        lay_can_start = _build_lay_can_date(start_day, start_month, year)
        lay_can_end = _build_lay_can_date(end_day, end_month, year)

        if lay_can_start > lay_can_end:
            lay_can_end = lay_can_end + relativedelta(years=1)

        return lay_can_start.isoformat(), lay_can_end.isoformat()

    _vague_match = re.match(r'(EARLY|ELY|MID|END)\s([A-Z]{3,4})', raw_lay_can)
    if _vague_match:
        day, month = _vague_match.groups()
        year = _get_lay_can_year(month, reported)
        start_day, end_day = VAGUE_DAY_MAPPING.get(day, (None, None))
        if day == 'END':
            # account for cases where reports sometimes use long-form months instead
            # for months like July/Jul and June/Jun, due to there only being one extra char
            month = month[:3]
            end_day = get_last_day_of_current_month(month, year, '%b', '%Y')
            start_day = end_day - 7

        lay_can_start = _build_lay_can_date(start_day, month, year).isoformat()
        lay_can_end = _build_lay_can_date(end_day, month, year).isoformat()
        return lay_can_start, lay_can_end

    _normal_match = re.match(r'(\d{1,2}).(\d{1,2}.)?([A-Z]{3,4})', raw_lay_can)
    if _normal_match:
        start_day, end_day, month = _normal_match.groups()
        year = _get_lay_can_year(month, reported)

        lay_can_start = _build_lay_can_date(start_day, month, year)
        lay_can_end = _build_lay_can_date(end_day, month, year) if end_day else lay_can_start

        # month rollover
        if lay_can_start > lay_can_end:
            lay_can_end = lay_can_end.replace(month=lay_can_start.month + 1)

        return lay_can_start.isoformat(), lay_can_end.isoformat()

    logger.warning('New lay can pattern spotted: %s', raw_lay_can)
    return None, None


def _get_lay_can_year(month, reported):
    """Get lay can year.

    Args:
        month (str):
        reported (str):

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
        day (int | str):
        month (str):
        year (int):

    Returns:
        datetime.datetime: time in ISO 8601 format

    """
    return parse_date(f'{day} {month} {year}', dayfirst=True)
