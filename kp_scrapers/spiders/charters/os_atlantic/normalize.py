from datetime import timedelta
import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.date import get_last_day_of_current_month
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.utils import validate_item


STATUS_MAPPING = {
    'FLD': SpotCharterStatus.failed,
    'RPTD': SpotCharterStatus.fully_fixed,
    'RPLC': SpotCharterStatus.fully_fixed,
    'FFXD': SpotCharterStatus.fully_fixed,
    'OLD': SpotCharterStatus.fully_fixed,
}

ZONE_MAPPING = {'USG+ECM': 'Gulf of Mexico'}

VAGUE_DAY_MAPPING = {'ELY': ('1', '7'), 'MID': ('14', '21')}

RATE_MAPPING = {'0': 'RNR', '1': 'COA'}

LAY_CAN_PATTERN = r'(\d{1,2}|[A-Z]{3,5})\D?(\d{1,2})?\D(\d{1,2})'

logger = logging.getLogger(__name__)


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping())
    if not item['vessel']:
        logger.info(f'Discard irrelevant vessel: {raw_item["0"]}')
        return

    item['charterer'], item['status'] = normalize_charterer_status(item.pop('charterer_status'))
    item['departure_zone'], item['arrival_zone'] = normalize_voyage(item.pop('voyage'))
    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    return item


def field_mapping():
    return {
        '0': ('vessel', normalize_vessel),
        '1': ignore_key('size'),
        '2': ignore_key('cargo'),
        '3': ('lay_can', None),
        '4': ('voyage', None),
        '5': ('rate_value', lambda x: RATE_MAPPING.get(x, x)),
        '6': ('charterer_status', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_vessel(raw_vessel):
    """Normalize vessel and remove unwanted characters.

    Rules:
        1. If `TBN` in vessel name, return None;
        2. If vessel name contains O/P, S/P sort of string, remove them.

    Examples:
        >>> normalize_vessel('SUEZ GEORGE O/O ')
        {'name': 'SUEZ GEORGE'}
        >>> normalize_vessel('S’GOL CAZENGA ')
        {'name': 'S’GOL CAZENGA'}
        >>> normalize_vessel('DELTA TBN ')

    Args:
        raw_vessel (str):

    Returns:
        Dict[str]: vessel sub model

    """
    if 'TBN' in raw_vessel.split():
        return None

    return {'name': re.sub(r'O/O|P/C|S/S|C/C|N/B', '', raw_vessel).strip()}


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference

    Date pattern spotted for now:
        - single day: 26/09
        - range: 29-30/09
        - range with month rollover: 27-02/10 (27/09 - 02/10)
        - vague day: END/09, ELY/10
        - typo: 03/05/09
    Examples:
        >>> normalize_lay_can('26/09', '1 Sep 2018')
        ('2018-09-26T00:00:00', '2018-09-26T00:00:00')
        >>> normalize_lay_can('29-30/09', '1 Sep 2018')
        ('2018-09-29T00:00:00', '2018-09-30T00:00:00')
        >>> normalize_lay_can('27-02/10', '26 Sep 2018')
        ('2018-09-27T00:00:00', '2018-10-02T00:00:00')
        >>> normalize_lay_can('END/10', '26 Sep 2018')
        ('2018-10-24T00:00:00', '2018-10-31T00:00:00')
        >>> normalize_lay_can('ELY/10', '26 Sep 2018')
        ('2018-10-01T00:00:00', '2018-10-07T00:00:00')
        >>> normalize_lay_can('28-1/1', '1 Jan 2019')
        ('2018-12-28T00:00:00', '2019-01-01T00:00:00')
        >>> normalize_lay_can('03/05/09', '17 Aug 2018')
        ('2018-09-03T00:00:00', '2018-09-05T00:00:00')
        >>> normalize_lay_can('31-01/04', '17 Aug 2019')
        ('2019-03-31T00:00:00', '2019-04-01T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]: (lay_can_start, lay_can_ends)
    """
    _match = re.match(LAY_CAN_PATTERN, raw_lay_can)
    if not _match:
        logger.error(f'Unknown lay can pattern: {raw_lay_can}')

    start_day, end_day, month = _match.groups()
    start_day, end_day = VAGUE_DAY_MAPPING.get(start_day, (start_day, end_day))

    # get year with year rollover handled
    year = parse_date(reported).year
    if 'Jan' in reported and month == '12':
        year -= 1
    if 'Dec' in reported and (month == '1' or month == '01'):
        year += 1

    # handle END vague day
    if start_day == 'END':
        end_day = get_last_day_of_current_month(month, year, '%m', '%Y')
        start_day = end_day - 7

    # assemble date
    try:
        start = parse_date(f'{start_day} {month} {year}', dayfirst=True)
    except Exception:
        roll_month = int(month) - 1 if month != 1 else 12
        start = parse_date(f'{start_day} {roll_month} {year}', dayfirst=True)

    end = parse_date(f'{end_day} {month} {year}', dayfirst=True) if end_day else start

    # month rollover
    if start - end > timedelta(days=0):
        start = start - relativedelta(months=1)

    return start.isoformat(), end.isoformat()


def normalize_voyage(raw_voyage):
    """Normalize departure zone and arrival zone from voyage field.

    Voyage examples:
        - USG/EAST-OPTS
        - USG+ECM/JAPAN
        - SIDI K/CANAPORT

    Examples:
        >>> normalize_voyage('USG/EAST-OPTS')
        ('USG', ['EAST', 'OPTS'])
        >>> normalize_voyage('USG+ECM/JAPAN')
        ('Gulf of Mexico', ['JAPAN'])
        >>> normalize_voyage('SIDI K/CANAPORT')
        ('SIDI K', ['CANAPORT'])
        >>> normalize_voyage('STS USG/USG')
        ('USG', ['USG'])

    Args:
        raw_voyage (str):

    Returns:
        Tuple[str, List[str]]: (departure_zone, arrival_zone)

    """
    raw_voyage = raw_voyage.replace('STS', '')

    departure, _, arrival = raw_voyage.partition('/')
    departure = ZONE_MAPPING.get(departure, departure).strip()
    arrival = [ZONE_MAPPING.get(x, x) for x in arrival.split('-')]
    return departure, arrival


def normalize_charterer_status(raw_charterer_status):
    """Normalize charterer and extract status if any.

    If the string contains status info, it would be like:
        - CLEARLAKE=FFXD=
        - CSSA =RPTD=

    Usually, it's only charterer. Therefore, we separate them by `=` and normalize status info.

    Examples:
        >>> normalize_charterer_status('CLEARLAKE=FFXD=')
        ('CLEARLAKE', 'Fully Fixed')
        >>> normalize_charterer_status('CSSA =RPTD=')
        ('CSSA', 'Fully Fixed')
        >>> normalize_charterer_status('P66')
        ('P66', None)
        >>> normalize_charterer_status('PERTAMINA - 35k')
        ('PERTAMINA', None)
        >>> normalize_charterer_status('CNR')
        (None, None)

    Args:
        raw_charterer_status (str):

    Returns:
        Tuple[str, str]: (charterer, status)

    """
    charterer, _, vague_status = raw_charterer_status.partition('=')

    # normalize charterer
    charterer = charterer.split('-')[0].strip()
    charterer = None if charterer == 'CNR' else charterer

    # normalize status
    status = None
    for s in STATUS_MAPPING:
        if s in vague_status:
            status = STATUS_MAPPING.get(s)
            break

    return charterer, status
