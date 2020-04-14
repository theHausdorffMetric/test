from copy import deepcopy
import datetime as dt
import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import xldate_to_datetime
from kp_scrapers.lib.utils import ignore_key, map_keys, protect_against
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


# cell values indicating invalid fields
INVALID_FIELD_VALUES = ['rvtg', '']

# disambiguate destinations/zones
PORT_MAPPING = {'KOREA': 'SOUTH KOREA'}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Process raw item, and normalize them.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:
    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    item['cargoes'] = [{'product': 'Thermal Coal', 'seller': {'name': item.pop('shipper', None)}}]

    if not are_pc_dates_valid(item):
        logger.info(f'Portcall dates are invalid, discarding:\n{item}')
        return

    pc_dates = filter_expired_dates(
        item.get('eta'),
        item.get('berthed'),
        item.get('finished'),
        item.get('departure'),
        item['reported_date'],
    )

    # no valid dates, discard item
    if not pc_dates:
        logger.info(f'Portcall dates have expired, discarding:\n{item}')
        return

    remove_unwanted_fields(item, 'finished', 'sheet_mode')
    item.update(pc_dates)

    # yield current port call
    portcall = deepcopy(item)
    if portcall.pop('next_zone', None):
        yield portcall

    # yield foreign port call
    portcall = deepcopy(item)
    yield portcall


def field_mapping():
    return {
        '0': ignore_key('month; redundant'),
        '1': ('port_name', None),
        '2': ('vessel', lambda x: {'name': x}),
        '3': ignore_key('cargo_volume; irrelevant for now'),
        '4': ('eta', convert_xldate),
        '5': ignore_key('eta_time; redundant'),
        '6': ('berthed', convert_xldate),
        '7': ignore_key('berthed_time; redundant'),
        '8': ('finished', convert_xldate),
        '9': ignore_key('finished_time; redundant'),
        '10': ('departure', convert_xldate),
        '11': ignore_key('departure_time; redundant'),
        '12': (
            'next_zone',
            lambda x: PORT_MAPPING.get(x, x) if x not in INVALID_FIELD_VALUES else None,
        ),
        '13': ('shipper', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
        'sheet_mode': ('sheet_mode', None),
    }


@protect_against()
def convert_xldate(raw_date_float):
    """Convert an xldate cell into an ISO-8601 string depending on value of immediate cell.

    Args:
        raw_date_float (float):

    Returns:
        str: ISO-8601 string if no errors, else empty string
    """
    return xldate_to_datetime(raw_date_float, sheet_datemode=0).isoformat()


def filter_expired_dates(eta, berthed, finished, departure, reported_date):
    """Filter and keep only recent items through filtering date.

    We can filter from departure, complete, berthed to eta in reverse chronological order. Say,
    if berthed exists, eta must exist.

    Args:
        eta (str):
        berthed (str):
        finished (str):
        departure (str):
        reported_date (str):

    Returns:
        Dict[str, str] | None:
    """
    reported_date = parse_date(reported_date, dayfirst=False)
    if departure and parse_date(departure) >= reported_date:
        return {'departure': departure, 'berthed': berthed, 'arrival': eta}

    if finished and parse_date(finished) >= reported_date:
        return {'departure': finished, 'berthed': berthed, 'arrival': eta}

    if berthed and parse_date(berthed) >= reported_date:
        return {'berthed': berthed, 'arrival': eta}

    if eta and parse_date(eta) >= reported_date:
        return {'eta': eta}

    return None


def are_pc_dates_valid(item):
    """Check if portcall dates satisfy certain business requirements.

    Sanity check in case there are nonsensical dates that don't make sense
    or are too far into the future, for example.

    Validity rules:
        - eta/berthed < finished ≤ departure
        - date must not be more than 1 year in advance (source only provides 1 year of data)

    Args:
        item (Dict[str, str]):

    Returns:
        bool: True if dates are all valid

    """
    _satisfied = []

    # sanity check to make sure date order is satisfied:
    # eta/berthed < finished ≤ departure
    if item.get('eta'):
        if item.get('finished'):
            _satisfied.append(parse_date(item['eta']) < parse_date(item['finished']))
        if item.get('departure'):
            _satisfied.append(parse_date(item['eta']) < parse_date(item['departure']))

    if item.get('berthed'):
        if item.get('finished'):
            _satisfied.append(parse_date(item['berthed']) < parse_date(item['finished']))
        if item.get('departure'):
            _satisfied.append(parse_date(item['berthed']) < parse_date(item['departure']))

    if item.get('finished') and item.get('departure'):
        _satisfied.append(parse_date(item['finished']) <= parse_date(item['departure']))

    for field in ['eta', 'arrival', 'berthed', 'departure']:
        if item.get(field):
            _satisfied.append(
                parse_date(item[field]) - parse_date(item['reported_date']) < dt.timedelta(days=365)
            )

    return not (False in _satisfied)


def remove_unwanted_fields(item, *fields):
    """Remove fields from a dict.

    This function will modify a list in-place.
    TODO could be made generic

    Args:
        item (Dict[str, str]):
        fields (List[str]): list of fields to remove
    """
    for field in fields:
        item.pop(field, None)
