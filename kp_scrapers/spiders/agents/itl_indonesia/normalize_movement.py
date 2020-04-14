import datetime as dt
import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import xldate_to_datetime
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys, protect_against
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


# cell values indicating invalid fields
INVALID_FIELD_VALUES = ['rvtg', '']


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Process raw item, and normalize them.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:
    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # fallback unto finished time if departure is null (analyst request)
    if not item.get('departure'):
        item['departure'] = item.get('finished', None)

    if not (item.get('eta') or item.get('berthed') or item.get('departure')):
        return

    seller = item.pop('shipper', None)
    item['cargo'] = {
        # product requested by coal analyst (Malay Trivedi)
        'product': 'Thermal Coal',
        'movement': 'load',
        'volume': try_apply(item.pop('cargo_volume', None), float, int, str),
        'volume_unit': Unit.tons,
        'seller': {'name': seller} if seller else None,
    }

    for col in ('finished', 'sheet_mode'):
        item.pop(col, None)

    return item


def field_mapping():
    return {
        '0': ignore_key('month; redundant'),
        '1': ('port_name', None),
        '2': ('vessel', lambda x: {'name': x}),
        '3': ('cargo_volume', lambda x: x if str(x) not in INVALID_FIELD_VALUES else None),
        '4': ('eta', convert_xldate),
        '5': ignore_key('eta_time; redundant'),
        '6': ('berthed', convert_xldate),
        '7': ignore_key('berthed_time; redundant'),
        '8': ('finished', convert_xldate),
        '9': ignore_key('finished_time; redundant'),
        '10': ('departure', convert_xldate),
        '11': ignore_key('departure_time; redundant'),
        '12': ignore_key('next zone; redundant'),
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
    if raw_date_float not in INVALID_FIELD_VALUES:
        return xldate_to_datetime(raw_date_float, sheet_datemode=0).isoformat()
    return None


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
