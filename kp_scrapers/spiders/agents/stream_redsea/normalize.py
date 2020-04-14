import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


UNIT_MAPPING = {'Mt': Unit.tons, 'm3': Unit.cubic_meter, '(m3)': Unit.cubic_meter}


BLACKLIST_VESSEL_TYPES = ['livestock carrier', 'container ship']


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # remove unwanted vessel types
    if item.get('vessel_type') and item['vessel_type'].lower() in BLACKLIST_VESSEL_TYPES:
        return

    # remove vessels not named yet
    if not item['vessel_name'] or any(
        sub in item['vessel_name'].lower()
        for sub in ['-', 'tbc', 'terminal', 'vessel', 'remarks', '\n']
    ):
        return

    for col in ['arrival', 'berthed', 'departure']:
        if item.get(col):
            item[col] = normalize_date(item[col], item['reported_date'])

    # build vessel sub model
    if item.get('vessel_length'):
        length = normalize_num(item['vessel_length'])[0]
    else:
        length = None
    if item.get('vessel_dwt'):
        dwt = normalize_num(item['vessel_dwt'])[0]
    else:
        dwt = None
    item['vessel'] = {
        'name': item.pop('vessel_name', None),
        'imo': item.pop('vessel_imo', None),
        'length': length,
        'dead_weight': dwt,
        'type': item.pop('vessel_type', None),
    }

    # build cargo sub model
    vol, unit = normalize_num(item['cargo_volume'])
    # lng attachment might not have product info
    if 'liquefied natural gas' in item['file_name'].lower():
        prod = 'lng'
    elif item.get('cargo_product'):
        prod = may_strip(item.pop('cargo_product', None))

    item['cargo'] = {
        'product': prod,
        'movement': item.pop('cargo_movement', None),
        'volume': vol,
        'volume_unit': UNIT_MAPPING.get(unit, None),
    }

    # remove products that are tbc or none
    if not item['cargo']['product'] or 'tbc' in item['cargo']['product'].lower():
        return

    for col in ('vessel_length', 'vessel_dwt', 'cargo_product', 'cargo_volume', 'file_name'):
        item.pop(col, None)
    return item


def field_mapping():
    return {
        'Vessel Name': ('vessel_name', None),
        'Vessel Type': ('vessel_type', None),
        'IMO': ('vessel_imo', lambda x: try_apply(x, float, int, str)),
        'Length': ('vessel_length', None),
        'Dimensions': ('vessel_length', None),
        'DWT': ('vessel_dwt', None),
        'Berth Number': ('berth', None),
        'Draft': ignore_key('redundant'),
        'Last Port': ignore_key('redundant'),
        'OPS': ('cargo_movement', lambda x: x.lower()),
        'Operation': ('cargo_movement', lambda x: x.lower()),
        'Cargo Type': ('cargo_product', may_strip),
        'Qtty': ('cargo_volume', None),
        'Est. Qtty': ('cargo_volume', None),
        'A.T.A': ('arrival', None),
        'E.T.A': ('arrival', None),
        'A.T.B': ('berthed', None),
        'E.T.B': ('berthed', None),
        'A.T.D': ('departure', None),
        'E.T.D': ('departure', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', normalize_rptd_date),
        'port_name': ('port_name', normalize_port),
        'file_name': ('file_name', None),
    }


def normalize_num(raw_num):
    """Remove unwanted words, extract digits only
    Examples:
        >>> normalize_num('1234 mt')
        ('1234', 'mt')
        >>> normalize_num('106 m3')
        ('106', 'm3')

    Args:
        raw_num (str):

    Returns:
        str:
    """
    _match_num = re.match(r'([0-9,]+)?(.*)', raw_num)
    if _match_num:
        return _match_num.group(1), may_strip(_match_num.group(2))

    return None, None


def normalize_rptd_date(raw_reported_date):
    """Extract reported date from email header
    Examples:
        >>> normalize_rptd_date('Stream Ships Service ( 3S ) Egypt - Red Sea Ports Line-up ( 03.05.2019 )') # noqa
        '2019-05-03T00:00:00'

    Args:
        raw_reported_date (str):

    Returns:
        str:
    """
    _rptd_match = re.match('.*\((.*)\)', raw_reported_date)
    if _rptd_match:
        return to_isoformat(may_strip(_rptd_match.group(1)), dayfirst=True)


def normalize_date(raw_date, rpt_date):
    """Extract reported date from email header
    Examples:
        >>> normalize_date('17.04 - 23:35', '2019-04-05T00:00:00')
        '2019-04-17T23:35:00'
        >>> normalize_date('30.12 - 00:00', '2019-01-01T00:00:00')
        '2018-12-30T00:00:00'
        >>> normalize_date('01.01 - 00:00', '2018-12-30T00:00:00')
        '2019-01-01T00:00:00'
        >>> normalize_date('27.04.2019', '2019-04-05T00:00:00')
        '2019-04-27T00:00:00'

    Args:
        raw_date (str):
        rpt_date (rpt_date):

    Returns:
        str:
    """
    _year = parse_date(rpt_date).year

    _match_date = re.match('(\d{1,2}\.\d{1,2}\.\d{2,4})', raw_date)
    if _match_date:
        return to_isoformat(raw_date, dayfirst=True)

    _match_date_time = re.match('(\d{1,2})\.(\d{1,2})\s\-\s(\d{1,2})\:(\d{1,2})', raw_date)
    if _match_date_time:
        _day, _month, _hour, _min = _match_date_time.groups()
        f_date = to_isoformat(f'{_day}-{_month}-{_year} {_hour}:{_min}', dayfirst=True)

        # to accomodate dates at the end of the year, 90 days was chosen as a gauge to
        # ensure the date returned is sensible
        if (parse_date(rpt_date) - parse_date(f_date)).days < -90:
            return to_isoformat(f'{_day}-{_month}-{int(_year)-1} {_hour}:{_min}', dayfirst=True)
        elif (parse_date(rpt_date) - parse_date(f_date)).days > 90:
            return to_isoformat(f'{_day}-{_month}-{int(_year)+1} {_hour}:{_min}', dayfirst=True)
        else:
            return f_date


def normalize_port(raw_port):
    """Extract reported date from email header
    Examples:
        >>> normalize_port('Abidaya Port')
        'abidaya'
        >>> normalize_port('Port Tee')
        'tee'

    Args:
        raw_port (str):

    Returns:
        str:
    """
    return may_strip(raw_port.lower().replace('port', ''))
