from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


EVENT_MAPPING = {'Departed': 'departure', 'Expected': 'eta', 'Present': 'arrival'}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    event = item.pop('event_type', None)
    if event:
        # build proper portcall date
        item[event] = item.pop('matching_date')
    else:
        # discard events without a proper mapping
        return

    # build Vessel sub model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'imo': item.pop('vessel_imo'),
        'dead_weight': item.pop('vessel_dwt'),
    }

    return item


def portcall_mapping():
    return {
        'berth': ('berth', None),
        'breadth': ignore_key('vessel beam'),
        'breadth_extreme': ignore_key('alternate vessel beam'),
        'call_sign': ignore_key('vessel callsign'),
        'datetime': ('matching_date', normalize_datetime),
        'deadweight': ('vessel_dwt', normalize_numeric),
        'draught': ignore_key('get draught from ais signal instead'),
        'flag_code': ignore_key('vessel flag code in ISO3166-alpha3'),
        'gross_tonnage': ignore_key('vessel gross tonnage'),
        'imo': ('vessel_imo', lambda x: x if len(x) == 7 else None),
        'length': ignore_key('vessel length'),
        'length_registered': ignore_key('ignore'),
        'mmsi': ignore_key('vessel MMSI number'),
        'name': ('vessel_name', None),
        'nett_tonnage': ignore_key('vessel nett tonnage'),
        'operator': ignore_key('vessel operator'),
        'port_name': ('port_name', None),
        'port_of_registry': ignore_key('vessel home port'),
        'power_kw_max': ignore_key('ignore'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'ship_category_code': ignore_key('ignore'),
        'status_date': ignore_key('ignore'),
        'style_image': ignore_key('irrelevant'),
        'ucrn': ignore_key('internal portcall number used by port authority'),
        'year_of_build': ignore_key('vessel build year'),
        'event_type': ('event_type', lambda x: EVENT_MAPPING.get(x)),
    }


def normalize_numeric(raw_data):
    """Normalize numeric data.

    Args:
        raw_data:

    Returns:
        Optional[int]:

    Examples:
        >>> normalize_numeric('2019')
        2019
        >>> normalize_numeric('42500.10')
        42500
        >>> normalize_numeric('0')

    """
    return try_apply(raw_data, float, int) if raw_data != '0' else None


def normalize_datetime(raw_date):
    """Normalize datetime strings and remove seconds detail.

    Args:
        raw_date:

    Returns:
        str: ISO8901 datetime string

    Examples:
        >>> normalize_datetime('2019-07-01T07:14:15Z')
        '2019-07-01T07:14:00'

    """
    return f'{to_isoformat(raw_date, dayfirst=False)[:-2]}00'
