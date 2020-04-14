from datetime import datetime
import logging

from kp_scrapers.lib.date import may_parse_date_str
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

STATUS_MAPPING = {
    'SUBS': SpotCharterStatus.on_subs,
    'CONF': SpotCharterStatus.fully_fixed,
    'RPTD': SpotCharterStatus.fully_fixed,
    'RPLC': SpotCharterStatus.fully_fixed,
    'FAIL': SpotCharterStatus.failed,
}

EMPTY_LAY_CAN_START = ['DNR']


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        SpotCharter:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    if item['status'] == SpotCharterStatus.failed:
        logger.info(f'Spot Charter has status failed: {item}')
        return

    item['lay_can_start'] = normalize_lay_can(item['lay_can_start'], item['reported_date'])
    return item


def field_mapping():
    return {
        'Name': ('vessel', lambda x: {'name': x}),
        'Size': ignore_key('size'),
        'Laycan': ('lay_can_start', None),
        'Load': ('departure_zone', None),
        'Discharge': ('arrival_zone', normalize_arrival_zone),
        'Rate': ('rate_value', None),
        'Charterer': ('charterer', None),
        'Status': ('status', lambda x: STATUS_MAPPING[x]),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(lay_can, reported_date):
    """Transform lay can start date to ISO 8601 format.

    We use the year of reported date as reference as there's no year info in lay can start date,
    however, we need to tackle End of Year issue:

    1. Dec case:
        lay can start: 2018.12.28
        reported date: 2019.1.1
    2. Jan case:
        lay can start: 2019.1.2
        reported date: 2018.12.28

    Examples:
        >>> normalize_lay_can('DNR', '21 Aug 2018')
        >>> normalize_lay_can('5-Sep', '21 Aug 2018')
        '2018-09-05T00:00:00'
        >>> normalize_lay_can('28-Dec', '21 Dec 2018')
        '2018-12-28T00:00:00'
        >>> normalize_lay_can('28-Dec', '1 Jan 2019')
        '2018-12-28T00:00:00'
        >>> normalize_lay_can('01-Jan', '28 Dec 2018')
        '2019-01-01T00:00:00'


    Args:
        lay_can (str):
        reported_date (str):

    Returns:

    """
    # QUESTION: using may_parse_date_str would cause duck typing problem
    if lay_can in EMPTY_LAY_CAN_START:
        return

    year = may_parse_date_str(reported_date, '%d %b %Y').year

    # Dec case
    if 'Dec' in lay_can and 'Jan' in reported_date:
        year = year - 1
    # Jan case
    if 'Jan' in lay_can and 'Dec' in reported_date:
        year = year + 1

    lay_can_str = lay_can + '-' + str(year)
    lay_can_start = datetime.strptime(lay_can_str, '%d-%b-%Y').isoformat()

    return lay_can_start


def normalize_arrival_zone(raw_data):
    """Turn it into list type, split multiple zones if any.

    Examples:
        >>> normalize_arrival_zone('MED-UKC')
        ['MED', 'UKC']
        >>> normalize_arrival_zone('SINGAPORE')
        ['SINGAPORE']

    Args:
        raw_data:

    Returns:

    """
    # QUESTION: could we use lambda instead as the function is quite short?
    return raw_data.strip().split('-')
