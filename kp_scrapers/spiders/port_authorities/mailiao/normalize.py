import datetime as dt
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


RELEVANT_EVENT = ['A']

IRRELEVANT_CARGO = ['3BD', 'GAA']

MOVEMENT_MAPPING = {'D': 'discharge', 'L': 'load'}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform a raw item into a normalized Dict.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str | Dict[str, str]]

    """
    item = map_keys(
        raw_item, field_mapping(reported_date=raw_item['reported_date']), skip_missing=True
    )

    # discard vessels with irrelevant cargoes or irrelevant port movements
    if not item['cargoes'] or not item['event_type']:
        return

    return {
        'vessel': {
            'name': item['vessel_name'],
            'gross_tonnage': item['vessel_gt_length'][0],
            'length': item['vessel_gt_length'][1],
        },
        'cargoes': item['cargoes'],
        'port_name': item['port_name'],
        'provider_name': item['provider_name'],
        'reported_date': item['reported_date'],
        'eta': item['eta'],
    }


def field_mapping(**kwargs):
    return {
        'POB': ignore_key('data is not being used'),
        'SHIP\'S NAME': ('vessel_name', None),
        'G/L': ('vessel_gt_length', normalize_gross_tonnage_and_length),
        'M': ('event_type', normalize_event_type),
        'CARGO': ('cargoes', lambda x: list(normalize_cargoes(x))),
        'ETA': ('eta', lambda x: normalize_date(x, **kwargs)),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_gross_tonnage_and_length(raw_gt_length):
    """Get gross tonnage from G/L field.

    Examples:
        >>> normalize_gross_tonnage_and_length('3125/ 98')
        ['3125', '98']
        >>> normalize_gross_tonnage_and_length('28736/ 181.5')
        ['28736', '181']

    Args:
        raw_gt_length (str):

    Returns:
        str:

    """
    _res = [each.strip() for each in raw_gt_length.split('/')]
    _res[1] = try_apply(_res[1], float, int, str)
    return _res


def normalize_event_type(event_type):
    """Return only arrival and departure event type.

    Examples:
        >>> normalize_event_type('A')
        'A'
        >>> normalize_event_type('D')

        >>> normalize_event_type('SI')

    Args:
        event_type (str):

    Returns:
        str | None:

    """
    return event_type if event_type in RELEVANT_EVENT else None


def normalize_cargoes(raw_cargo):
    """Extract cargoes from raw cargo string.

    Examples:
        >>> list(normalize_cargoes('D:CRUO T'))
        []
        >>> list(normalize_cargoes('D:METH 15000'))
        [{'product': 'METH', 'movement': 'discharge', 'volume': '15000', 'volume_unit': 'tons'}]
        >>> list(normalize_cargoes('O:OTHER'))
        []
        >>> list(normalize_cargoes('L:MEG 15000T;L:DEG 2000T'))  # doctest: +NORMALIZE_WHITESPACE
        [{'product': 'MEG', 'movement': 'load', 'volume': '15000', 'volume_unit': 'tons'},
        {'product': 'DEG', 'movement': 'load', 'volume': '2000', 'volume_unit': 'tons'}]

    Args:
        raw_cargo (str):

    Yields:
        Dict[str, str]

    """
    # keep only discharge or load cargoes, with numerical values
    cargo_list = re.findall(r'([L|D]):(\S+) (\d{3,7})', raw_cargo)

    for movement, cargo, volume in cargo_list:
        # check if the cargo is relevant
        if cargo in IRRELEVANT_CARGO:
            return

        # assemble cargo and yield
        yield {
            'product': cargo,
            'movement': MOVEMENT_MAPPING[movement],
            'volume': volume,
            'volume_unit': 'tons',
        }


def normalize_reported_date(date_str):
    """Transform reported date to ISO 8601 format.

    Args:
        date_str (str): date format: mm/dd hh:mm, no year information

    Returns:
        str:

    """
    date_format = '%Y/%m/%d %H:%M'

    today = dt.datetime.utcnow()

    # use the year of today as reported year, append it to the head of date_str
    date = str(today.year) + '/' + date_str.strip()

    # parse the date, reported date is utc, but source provided date is Taiwan time.
    reported_date = dt.datetime.strptime(date, date_format) - dt.timedelta(hours=8)

    # reported date should always be smaller than today
    # if not, we encounter the reported date is the end of last year, and
    # today is the next year
    if reported_date > today:
        date = str(today.year - 1) + '/' + date_str.strip()
        reported_date = dt.datetime.strptime(date, date_format)

    return reported_date.isoformat()


def normalize_date(date_str, reported_date):
    """Transform date to ISO 8601 format.

    Format:
    1. mm/dd
    2. mm/dd hhmm
    3. mm/dd AM
    4. mm/dd PM

    The year information should be extracted from reported.

                    ------------------------------------>
                    |(less than a month)
    ----------------.-----------.---------------------------------->
                    date        reported_date

    Examples:
        >>> normalize_date('08/04 0100', '2018-08-03T10:01:00')
        '2018-08-04T01:00:00'
        >>> normalize_date('08/15 0100', '2018-08-21T10:01:00')
        '2018-08-15T01:00:00'
        >>> normalize_date('08/13 ', '2018-08-03T10:01:00')
        '2018-08-13T00:00:00'
        >>> normalize_date('01/01 ', '2018-12-30T10:01:00')
        '2019-01-01T00:00:00'
        >>> normalize_date('12/30 ', '2020-01-01T09:18:00')
        '2019-12-30T00:00:00'
        >>> normalize_date('08/23 AM', '2018-12-30T10:01:00')
        '2019-08-23T00:00:00'
        >>> normalize_date('08/23 PM', '2018-12-30T10:01:00')
        '2019-08-23T00:00:00'

    Args:
        date_str (str):
        reported_date (str):

    Returns:
        str:

    """
    # get reported date of datetime type, we'll need the year
    reported_date = parse_date(reported_date)

    # extract month, day, hour and minute (if only) information from date string
    date_str = date_str.replace('AM', '')
    date_str = date_str.replace('PM', '')
    date_list = date_str.strip().split(' ')
    month, day = [int(each) for each in date_list[0].split('/')]
    hh, mm = 0, 0
    if len(date_list) == 2:
        hh, mm = int(date_list[1][:2]), int(date_list[1][2:])

    # try use the reported year
    normalized_date = dt.datetime(year=reported_date.year, month=month, day=day, hour=hh, minute=mm)

    # some date string doesn't provide hour and minute info, the eventual date would be no
    # more than a day earlier than reported date.
    # if not, we encounter the end of the year situation
    if (normalized_date - reported_date).days > 30:
        normalized_date = dt.datetime(
            year=reported_date.year - 1, month=month, day=day, hour=hh, minute=mm
        )
    elif (normalized_date - reported_date).days < -30:
        normalized_date = dt.datetime(
            year=reported_date.year + 1, month=month, day=day, hour=hh, minute=mm
        )

    return normalized_date.isoformat()
