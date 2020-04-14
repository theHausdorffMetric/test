import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


ITALIAN_MONTH_MAPPING = {
    'gen': 'jan',
    'feb': 'feb',
    'mar': 'mar',
    'apr': 'apr',
    'mag': 'may',
    'giu': 'jun',
    'lug': 'jul',
    'lu': 'jul',
    'ag': 'aug',
    'ago': 'aug',
    'set': 'sep',
    'ott': 'oct',
    'nov': 'nov',
    'dic': 'dec',
}


STATUS_MAPPING = {
    'subs': SpotCharterStatus.on_subs,
    'fld': SpotCharterStatus.failed,
    'reported': SpotCharterStatus.fully_fixed,
}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    if not item['vessel']['name'] or 'tbn' in item['vessel']['name'].lower():
        return

    # remove unnecessary items
    if not item['departure_zone']:
        return

    if any(sub in item['rate_value'] for sub in ('reported', 'quote')):
        return

    item['lay_can_start'] = normalize_lay_can(item.pop('lay_can', None), item['reported_date'])

    if item['cargo_prod_volume']:
        cargo_volume, cargo_product = normalize_cargo_vol(item.pop('cargo_prod_volume', None))
        if cargo_product:
            item['cargo'] = {
                'product': cargo_product,
                'volume': cargo_volume,
                'volume_unit': Unit.tons,
                'movement': 'load',
            }

    return item


def field_mapping():
    return {
        '0': ('vessel', lambda x: {'name': normalize_vessel(x)}),
        '1': ('cargo_prod_volume', None),
        '2': ('departure_zone', None),
        '3': ('arrival_zone', normalize_arrival),
        '4': ('lay_can', None),
        '5': ('rate_value', None),
        '6': ('charterer', None),
        '7': ('status', lambda x: STATUS_MAPPING.get(x, None)),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_vessel(raw_vessel):
    """Normalize cargo.

    Examples:
        >>> normalize_vessel('BUNGA KASTURI TIGA vl8/06')
        'BUNGA KASTURI TIGA'

    Args:
        raw_cargo:

    Returns:

    """
    return re.sub(r'\s[A-z0-9]+\/[A-z0-9]+', '', raw_vessel) if raw_vessel else None


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Lay can pattern:
        - Normal case: 6-Nov, 6Nov, 6Dic

    Month rollover case not spotted yet.

    Examples:
        >>> normalize_lay_can('6-Nov', '5 Nov 2018')
        '2018-11-06T00:00:00'
        >>> normalize_lay_can('6Nov', '5 Nov 2018')
        '2018-11-06T00:00:00'
        >>> normalize_lay_can('6gen', '5 Nov 2018')
        '2018-01-06T00:00:00'
        >>> normalize_lay_can('30Dec', '5 Jan 2019')
        '2018-12-30T00:00:00'
        >>> normalize_lay_can('01Jan', '5 Dec 2018')
        '2019-01-01T00:00:00'

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    raw_lay_can = raw_lay_can.lower()
    _normal_match = re.match(r'([0-9]+)\W?([A-z]+)', raw_lay_can)
    if _normal_match:
        start_day, month = _normal_match.groups()
        month = ITALIAN_MONTH_MAPPING.get(month, month)
        year = _get_lay_can_year(month, reported)

        try:
            _date = parse_date(f'{start_day} {month} {year}', dayfirst=True).isoformat()
        except Exception:
            logger.error(
                'Invalid laycan format spotted (reported_date=%(reported_date)s): %(laycan)s',
                {'laycan': repr(raw_lay_can), 'reported_date': reported},
            )
            return None

        return _date

    # suppress log message for cases where there is clearly no laycan provided
    if raw_lay_can:
        logger.error(
            'Unknown laycan (reported_date=%(reported_date)s): %(laycan)s',
            {'laycan': repr(raw_lay_can), 'reported_date': reported},
        )
    return None


def _get_lay_can_year(month, reported):
    """Get the year of lay can date with reference of reported date.

    Args:
        month:
        reported:

    Returns:
        int:

    """
    year = parse_date(reported).year
    if month in ['dec', 'dic'] and 'Jan' in reported:
        year -= 1
    if month in ['jan', 'gen'] and 'Dec' in reported:
        year += 1
    return year


def normalize_cargo_vol(raw_string):
    """Split cargo product and volume

    Examples:
        >>> normalize_cargo_vol('130 fo')
        ('130', 'fo')
        >>> normalize_cargo_vol('130')
        ('130', '')

    Args:
        month:
        reported:

    Returns:
        int:

    """
    _match = re.match(r'([0-9]+)(.*)', raw_string)
    if _match:
        vol, prod = _match.groups()
        return may_strip(vol), may_strip(prod)

    return None, None


def normalize_arrival(raw_arr):
    """Normalize arrival zones.

     Examples:
        >>> normalize_arrival('UKC-MED')
        ['UKC', 'MED']
        >>> normalize_arrival('ST. LUCIA')
        ['ST. LUCIA']

     Args:
        raw_voyage (str):

     Returns:
        List[str]
     """
    return raw_arr.split('-') if raw_arr else None
