import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import xldate_to_datetime
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys, protect_against
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(
        raw_item, field_mapping(reported_date=raw_item['reported_date']), skip_missing=True
    )

    if not item['vessel']:
        return

    # build cargo sub-model
    item['cargo'] = normalize_cargo(item.pop('volume'), item.pop('product'))

    # get lay can start by priority
    etb, pob, eta = item.pop('etb', None), item.pop('pob', None), item.pop('eta', None)
    item['lay_can_start'] = etb or pob or eta

    return item


def field_mapping(**kwargs):
    return {
        'VESSEL': ('vessel', lambda x: None if 'TBN' in x or not x else {'name': x}),
        'ETA': ('eta', lambda x: normalize_date(x, **kwargs)),
        'ETB': ('etb', lambda x: normalize_date(x, **kwargs)),
        'POB': ('pob', lambda x: normalize_date(x, **kwargs)),
        'ETD': (ignore_key('departure')),
        'DAYS DELAY': (ignore_key('days of delay')),
        'CARGO': ('volume', None),
        'SHIPPER': ('charterer', None),
        'TYPE': ('product', None),
        'DESTINATION': ('arrival_zone', lambda x: None if x == 'TBA' else [x]),
        'BERTH': (ignore_key('berth')),
        'PRINCIPLE SHIPPER': ('charterer', lambda x: '' if x == 'TBA' else x),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: parse_date(x)),
        'port_name': ('departure_zone', None),
    }


def normalize_cargo(volume, product):
    """Normalize cargo.

    Args:
        volume:
        product:

    Returns:

    """
    if product == 'TBA':
        return None

    return {
        'product': product,
        'movement': 'load',
        'volume': try_apply(volume, float, str),
        'volume_unit': Unit.tons,
    }


def normalize_date(raw_date, reported_date=None):
    """Normalize lay can date with reported year as reference.

    Examples:
        >>> normalize_date(43393.666666666664)
        '2018-10-20T16:00:00'
        >>> normalize_date('10/10', '2018-12-12T16:00:00')
        '2018-10-10T00:00:00'

    Args:
        raw_date (str | float):
        reported (str):

    Returns:
        str:

    """
    if isinstance(raw_date, str):
        reported = parse_date(reported_date)
        _match = re.match(r'\d{1,2}/\d{1,2}', raw_date)
        return to_isoformat(f'{raw_date}/{reported.year}') if _match else None

    if isinstance(raw_date, float):
        return convert_xldate(raw_date)


@protect_against()
def convert_xldate(raw_date_float):
    """Convert an xldate cell into an ISO-8601 string depending on value of immediate cell.

    Args:
        raw_date_float (float):

    Returns:
        str: ISO-8601 string if no errors, else empty string
    """
    return xldate_to_datetime(raw_date_float, sheet_datemode=0).isoformat()
