import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MISSING_ROWS = []


STATUS_MAPPING = {
    'fld': SpotCharterStatus.failed,
    'fxd': SpotCharterStatus.fully_fixed,
    'subs': SpotCharterStatus.on_subs,
}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        SpotCharter | None

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # remove not nominated vessels
    if not item['vessel'] or 'TBN' in item['vessel']['name'].upper():
        return

    # remove filler rows with not laycans
    if not item['lay_can_start']:
        return

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item['lay_can_start'], item.pop('lay_can_to', None), item['reported_date'], item
    )

    # sanity check again, remove normalized rows without laycans
    if not item['lay_can_start']:
        return

    # build cargo sub-model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.kilotons,
    }

    return item


def field_mapping():
    return {
        'vessel': ('vessel', lambda x: {'name': x}),
        'status': ('status', lambda x: STATUS_MAPPING.get(x.lower(), None)),
        'qty': ('cargo_volume', None),
        'grade': ('cargo_product', None),
        'lay_can_from': ('lay_can_start', None),
        'lay_can_to': ('lay_can_to', None),
        'load': ('departure_zone', None),
        'discharge': ('arrival_zone', normalize_arrival),
        'rate': ('rate_value', None),
        'charterer': ('charterer', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can_from, raw_lay_can_to, reported, r_item):
    """Normalize lay can start date with the year of reported date as reference.

    Lay can start date format:
        1. 21/5 (normal case or cln attachment)
        2. 10-12/5 (dty attachment)

    Examples:

        >>> normalize_lay_can('10-12/5', None, '30 Dec 2018', 'vessel...')
        ('2018-05-10T00:00:00', '2018-05-12T00:00:00')
        >>> normalize_lay_can('30-1/1', None, '30 Dec 2018', 'vessel...')
        ('2018-12-30T00:00:00', '2019-01-01T00:00:00')
        >>> normalize_lay_can('12/5', None,'30 Dec 2018', 'vessel...')
        ('2018-05-12T00:00:00', None)
        >>> normalize_lay_can('30/12', None,'10 Jan 2019', 'vessel...')
        ('2018-12-30T00:00:00', None)
        >>> normalize_lay_can('1/1', None, '30 Dec 2018', 'vessel...')
        ('2019-01-01T00:00:00', None)

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        str:

    """
    # check if raw_lay_can_from contains double dates
    _match_double = re.match(r'(\d{1,2})\-(\d{1,2})\/(\d{1,2})', raw_lay_can_from)
    if _match_double:
        _day_start, _day_end, _month = _match_double.groups()
        _year = _get_lay_can_year(_month, reported)
        # in the event the rollover has february dates (30-01 FEB), first result would
        # be 30th february which does not exist, hence try and except is needed
        try:
            start = parse_date(f'{_day_start} {_month} {_year}', dayfirst=True)
        except Exception:
            start = parse_date(f'{_day_start} {int(_month) - 1} {_year}', dayfirst=True)

        end = parse_date(f'{_day_end} {_month} {_year}', dayfirst=True)

        # if it's rollover case
        if start > end:
            start = start + relativedelta(months=-1)

        return start.isoformat(), end.isoformat()

    if raw_lay_can_to:
        if len(raw_lay_can_from.split('/')) == 2 and len(raw_lay_can_to.split('/')) == 2:
            return (
                normalize_single_date(raw_lay_can_from, reported),
                normalize_single_date(raw_lay_can_to, reported),
            )

    else:
        if len(raw_lay_can_from.split('/')) == 2:
            return (normalize_single_date(raw_lay_can_from, reported), None)

    MISSING_ROWS.append(str(r_item))
    # suppress logging if it is a known invalid value
    if not (
        raw_lay_can_from in (None, 'dnr', 'DNR', 'Dnr')
        and raw_lay_can_to in (None, 'dnr', 'DNR', 'Dnr')
    ):
        logger.error(
            'Unknown laycan: %(laycan_start)s -> %(laycan_end)s',
            {'laycan_start': raw_lay_can_from, 'laycan_end': raw_lay_can_to},
        )
    return None, None


def normalize_single_date(raw_date, reported):
    """Normalize lay can start date if single

    Lay can start date format:
        1. 21/5 (normal case or cln attachment)

    Examples:

        >>> normalize_single_date('12/5', '30 Dec 2018')
        '2018-05-12T00:00:00'
        >>> normalize_single_date('30/12', '10 Jan 2019')
        '2018-12-30T00:00:00'
        >>> normalize_single_date('1/1', '30 Dec 2018')
        '2019-01-01T00:00:00'

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        str:

    """
    _match = re.match(r'(\d{1,2})\/(\d{1,2})', raw_date)
    if _match:
        s_day, s_month = _match.groups()
        s_year = _get_lay_can_year(s_month, reported)
        return to_isoformat(f'{s_day} {s_month} {s_year}', dayfirst=True)


def _get_lay_can_year(month, reported):
    """Get lay can month with reference of reported year.

    Args:
        month (str):
        reported (str):

    Returns:

    """
    year = parse_date(reported).year

    if '12' in month and 'Jan' in reported:
        year -= 1
    # Year shift Jan case
    if month in ['1', '01'] and 'Dec' in reported:
        year += 1

    return year


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
