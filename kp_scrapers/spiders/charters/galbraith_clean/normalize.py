import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

STATUS_MAPPING = {
    'SUBS': SpotCharterStatus.on_subs,
    'FXD': SpotCharterStatus.fully_fixed,
    'FLD': SpotCharterStatus.failed,
}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict(str, str):

    """
    item = map_keys(raw_item, charters_mapping())

    # discard failed charters
    if item['status'] == SpotCharterStatus.failed:
        return

    # enrich laycan dates with year
    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    return item


def charters_mapping():
    return {
        '0': ('status', lambda x: STATUS_MAPPING.get(x.partition(' ')[0])),
        '1': ('vessel', lambda x: {'name': x}),
        '2': ignore_key('unknown value, possibly cargo volume'),
        '3': ('cargo', lambda x: {'product': x}),
        '4': ('departure_zone', None),
        '5': ignore_key('slash /, irrelevant'),
        '6': ('arrival_zone', lambda x: x.split('-') if x else None),
        '7': ('lay_can', None),
        '8': ('rate_value', None),
        '9': ('charterer', None),
        '10': ignore_key('irrelevant'),
        '11': ignore_key('irrelevant'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported):
    """Normalize raw lay can date with reference of reported year.

    Raw laycan inputs can be of the following formats:
        1) single day: '08-SEP'
        2) duration days: '10-11 OCT'
        3) month cross: '30 OCT-2 NOV'
        4) month only: 'AUG'

    Examples:
        >>> normalize_lay_can('08-SEP','1 Sep 2018')
        ('2018-09-08T00:00:00', '2018-09-08T00:00:00')
        >>> normalize_lay_can('10-11 OCT', '10 Oct 2018')
        ('2018-10-10T00:00:00', '2018-10-11T00:00:00')
        >>> normalize_lay_can('30 OCT-2 NOV', '10 Oct 2018')
        ('2018-10-30T00:00:00', '2018-11-02T00:00:00')
        >>> normalize_lay_can('AUG','28 Aug 2018')
        (None, None)
        >>> normalize_lay_can('28 Dec-1 Jan', '25 Dec 2018')
        ('2018-12-28T00:00:00', '2019-01-01T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str): reported date

    Returns:
        Tuple[str, str]: tuple of lay can period, (lay can start, lay can end)

    """
    # format 1, 2
    _match = re.match(r'(^\d{1,2}).(\d{1,2}.)?([A-Za-z]{3,4}$)', raw_lay_can)
    if _match:
        start_day, end_day, month = _match.groups()
        year = _get_year(month, reported)
        start = to_isoformat(f'{start_day} {month} {year}', dayfirst=True)
        end = to_isoformat(f'{end_day} {month} {year}', dayfirst=True) if end_day else start

        return start, end

    # format 3
    _findall = re.findall(r'\d{1,2} [A-Za-z]{3,4}', raw_lay_can)
    if len(_findall) == 2:
        start_date, end_date = _findall
        start_year, end_year = _get_year(start_date, reported), _get_year(end_date, reported)
        start = to_isoformat(f'{start_date} {start_year}', dayfirst=True)
        end = to_isoformat(f'{end_date} {end_year}', dayfirst=True)

        return start, end

    # format 4
    _match = re.match(r'^[A-Za-z]{3,4}$', raw_lay_can)
    if _match:
        return None, None

    # unknown formats
    logger.exception(f'Invalid or unknown lay can format: {raw_lay_can}')
    return None, None


def _get_year(lay_can_str, reported):
    """Get lay can year with reference of reported date.

    Args:
        lay_can_str (str):
        reported (str):

    Returns:
        str:

    """
    year = parse_date(reported).year
    if 'Dec' in reported and 'JAN' in lay_can_str.upper():
        year += 1
    if 'Jan' in reported and 'DEC' in lay_can_str.upper():
        year -= 1

    return year
