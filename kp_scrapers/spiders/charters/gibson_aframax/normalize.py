import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import get_last_day_of_current_month, to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

VAGUE_DAY_MAPPING = {'ELY': '1-7', 'EARLY': '1-7', 'MID': '14-21'}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    if not item['vessel_name']:
        return

    item['charterer'], item['status'] = normalize_charterer_status(item.pop('charterer_status'))
    item['departure_zone'], item['arrival_zone'] = normalize_voyage(item.pop('voyage'))
    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    for name in item.pop('vessel_name'):
        if name:
            item['vessel'] = {'name': name}
            yield item


def field_mapping():
    return {
        '0': ('vessel_name', normalize_vessel),
        '1': ignore_key('size'),
        '2': ignore_key('cargo'),
        '3': ('lay_can', None),
        '4': ('voyage', None),
        '5': ('rate_value', None),
        '6': ('charterer_status', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_vessel(raw_vessel):
    """Normalize vessel and remove unwanted strings.

    If vessel name contains `TBN`, it's irrelevant vessel, discard it.
    There's a situation that the vessel name contains `o/o`, analyst wants to return two fixtures
    if o/o is in the middle of two vessels.

    Examples:
        >>> normalize_vessel('NORD O/O')
        ['NORD', '']
        >>> normalize_vessel('MONTEGO O/O AGRARI')
        ['MONTEGO', 'AGRARI']
        >>> normalize_vessel('OILMEDA TBN')

    Args:
        raw_vessel (str):

    Returns:
        List[str]:

    """
    if 'TBN' in raw_vessel.upper().split():
        logger.info(f'Discard irrelevant vessel: {raw_vessel}')
        return

    # remove unwanted string
    vessel = may_strip(re.sub(r'P/C|S/S|C/C|N/B', '', raw_vessel))

    # O/O: owner option, multiple vessel
    return [may_strip(each) for each in vessel.split('O/O')]


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    In this report, the lay can date can vary differently, however, we only extract below formats:
    - 2-3/10
    - 30/09
    - End/9

    No month rollover found yet, don't handle them first as we don't want to make false assumptions.

    Examples:
        >>> normalize_lay_can('2-3/10', '22 Sep 2018')
        ('2018-10-02T00:00:00', '2018-10-03T00:00:00')
        >>> normalize_lay_can('30/09', '22 Sep 2018')
        ('2018-09-30T00:00:00', '2018-09-30T00:00:00')
        >>> normalize_lay_can('30/12', '1 Jan 2019')
        ('2018-12-30T00:00:00', '2018-12-30T00:00:00')
        >>> normalize_lay_can('1-2/01', '28 Dec 2018')
        ('2019-01-01T00:00:00', '2019-01-02T00:00:00')
        >>> normalize_lay_can('END/9', '28 Dec 2018')
        ('2018-09-24T00:00:00', '2018-09-30T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    year = parse_date(reported).year
    day, _, month = raw_lay_can.partition('/')
    day = VAGUE_DAY_MAPPING.get(day, day)
    start_day, _, end_day = day.partition('-')

    # year rollover
    if month == '12' and 'Jan' in reported:
        year -= 1
    if (month == '1' or month == '01') and 'Dec' in reported:
        year += 1

    if start_day == 'END':
        end_day = get_last_day_of_current_month(month, str(year), '%m', '%Y')
        start_day = end_day - 6

    start = to_isoformat(f'{start_day} {month} {year}', dayfirst=True)
    end = to_isoformat(f'{end_day} {month} {year}', dayfirst=True) if end_day else start

    return start, end


def normalize_voyage(raw_voyage):
    """Normalize departure zone and arrival zone from voyage field.

    Examples:
        >>> normalize_voyage('TAMAN MED-MALTA')
        ('TAMAN', ['MED', 'MALTA'])
        >>> normalize_voyage('BALTIC UKC')
        ('BALTIC', ['UKC'])
        >>> normalize_voyage('ES SIDER SPAIN REPSOL')
        (None, None)
        >>> normalize_voyage('SIDI K SINES')
        (None, None)

    Notes:
        As there's no separation for departure zone and arrival zones, we'll only handle situation
        like below, in which the zone names are one word:
        - Taman Med-Malta
        - Baltic UKC

        If we encounter such cases listed below, we'll discagrd this, and analysts will fill them:
        - Es Sider Spain Repsol
        - Sidi K Sines

    Args:
        raw_voyage (str):

    Returns:
        Tuple[str, List[str]]: departure zone, arrival zones

    """
    _zones = raw_voyage.split()

    if len(_zones) != 2:
        return None, None

    return _zones[0], _zones[1].replace('/', '-').split('-')


def normalize_charterer_status(vague_charterer_status):
    """Normalize charterer and status.

    Usually, they are separated by dash (-), only detect failed status.

    Examples:
        >>> normalize_charterer_status('CORAL - HEATING FOR OWNER ACCOUNT')
        ('CORAL', None)
        >>> normalize_charterer_status('CLEARLAKE')
        ('CLEARLAKE', None)
        >>> normalize_charterer_status('CLEARLAKE - FLD')
        ('CLEARLAKE', 'Failed')
        >>> normalize_charterer_status('CLEARLAKE RPLCD UML')
        ('CLEARLAKE', None)
        >>> normalize_charterer_status('VITOL TCO STEM - old')
        ('VITOL', None)
        >>> normalize_charterer_status('CNR')
        ('', None)

    Args:
        vague_charterer_status:

    Returns:

    """
    charterer, _, status = re.sub(r'RPLCD|TCO|KPO', '-', vague_charterer_status).partition('-')
    charterer = '' if may_strip(charterer) == 'CNR' else charterer
    status = SpotCharterStatus.failed if 'FLD' in status.split() else None
    return may_strip(charterer), status
