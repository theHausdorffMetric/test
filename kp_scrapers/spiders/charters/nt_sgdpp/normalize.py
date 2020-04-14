import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


ZONE_MAPPING = {'RTAN': 'Ras Tanura', 'YOSU': 'Yeosu'}


ZONE_BLACKLIST = ['+1']


CHARTER_MAPPING = {
    'FLD': SpotCharterStatus.failed,
    'RPTD': SpotCharterStatus.fully_fixed,
    'RPLD': SpotCharterStatus.fully_fixed,
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

    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.kilotons,
    }

    vessel, item['status'] = normalize_vessel(item['vessel_status'])

    item['vessel'] = {'name': vessel}

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('laycan'), item['reported_date']
    )

    item.pop('vessel_status', None)

    return item


def field_mapping():
    return {
        '0': ('vessel_status', None),
        '1': ('cargo_volume', None),
        '2': ('cargo_product', None),
        '3': ('departure_zone', normalize_zone),
        '4': ('arrival_zone', lambda x: [normalize_zone(x)]),
        '5': ('laycan', None),
        '6': ('rate_value', None),
        '7': ('charterer', lambda x: None if x == '?' else x),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_zone(raw_zone):
    """Normalize zone, remove unwanted strings.

    Examples:
        >>> normalize_zone('Singapore')
        'Singapore'
        >>> normalize_zone('Singapore+1')
        'Singapore'

    Args:
        raw_zone (str):

    Returns:
        str:

    """
    for word in ZONE_BLACKLIST:
        raw_zone = may_strip(raw_zone.replace(word, ''))

    return ZONE_MAPPING.get(raw_zone, raw_zone)


def normalize_vessel(raw_vessel):
    """Normalize charter and rate, remove unwanted strings.

    Examples:
        >>> normalize_vessel('CHAFA (FLD)')
        ('CHAFA', 'Failed')
        >>> normalize_vessel('CHAFA')
        ('CHAFA', 'Fully Fixed')

    Args:
        raw_zone (str):

    Returns:
        str:

    """
    match_vessel = re.match(r'(.*)\((.*)\)', raw_vessel)
    if match_vessel:
        vessel_name, charter_status = match_vessel.groups()

        return may_strip(vessel_name), CHARTER_MAPPING.get(charter_status, charter_status)

    return raw_vessel, SpotCharterStatus.fully_fixed


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    In this report, the lay can date can vary differently, however, we only extract below formats:
    - 18-May

    No month rollover found yet, don't handle them first as we don't want to make false assumptions.

    Examples:
        >>> normalize_lay_can('18-May', '22 Mar 2018')
        ('2018-05-18T00:00:00', '2018-05-18T00:00:00')
        >>> normalize_lay_can('30-Dec', '22 Dec 2018')
        ('2018-12-30T00:00:00', '2018-12-30T00:00:00')
        >>> normalize_lay_can('30-Dec', '22 Jan 2019')
        ('2018-12-30T00:00:00', '2018-12-30T00:00:00')
        >>> normalize_lay_can('01-Jan', '30 Dec 2018')
        ('2019-01-01T00:00:00', '2019-01-01T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    rpt_year = parse_date(reported).year

    _match_single_date = re.match(r'(\d+)(?:[\-\/])([[A-z0-9]+)', raw_lay_can)
    if _match_single_date:
        start_day, month = _match_single_date.groups()

        # year rollover
        if month == 'Dec' and 'Jan' in reported:
            rpt_year -= 1
        if month == 'Jan' and 'Dec' in reported:
            rpt_year += 1

        start = to_isoformat(f'{start_day} {month} {rpt_year}', dayfirst=True)

        # return the same start date for lay_can_end is single date provided
        return start, start

    logger.error('Unable to normalize laycan: %s', raw_lay_can)

    return None, None
