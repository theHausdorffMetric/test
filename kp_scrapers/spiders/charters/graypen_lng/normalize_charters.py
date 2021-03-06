import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.services import kp_api
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


IRRELEVANT_PATTERN = ['CNR', 'TBN']


MOVEMENT_MAPPING = {'L': 'load', 'D': 'discharge'}


UNIT_MAPPING = {'KT': Unit.kilotons}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable SpotCharter event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:
    """
    item = map_keys(raw_item, charters_mapping())
    # discard vessels if it's yet to be named (TBN)
    if not item['vessel']:
        return

    # only process charterers that carry ethane cargos
    if 'ethane' not in item['cargo_product'].lower():
        return

    for col in ('berthed', 'eta', 'departure'):
        if item.get(col):
            item[col] = normalize_dates(item[col], item['reported_date'])

    item['lay_can_start'] = None
    while not item['lay_can_start']:
        for col in ('berthed', 'eta', 'departure'):
            if item.get(col) and item[col]:
                item['lay_can_start'] = item[col]
                break
        break

    # process "import" portcalls accordingly (charters are defined by their departure)
    if 'discharge' in item['cargo_movement']:
        item['arrival_zone'] = item.pop('zone', None)
        logger.info(f'Attempting to find matching export port call for {item["vessel"]["name"]}')
        # get trade from oil platform
        _trade = kp_api.get_session('lpg', recreate=True).get_import_trade(
            vessel=item['vessel']['name'],
            origin='',
            dest=item['arrival_zone'],
            end_date=item['lay_can_start'],
        )
        # mutate item with relevant laycan periods and load/discharge port info
        _process_import_charter(item, _trade)

    if 'load' in item['cargo_movement']:
        item['departure_zone'] = item.pop('zone', None)

    volume, units, grades = normalize_vol_unit(
        item.pop('cargo_volume', None), item.pop('cargo_product', None)
    )

    # build Cargo sub-model
    item['cargo'] = {
        'product': grades,
        'movement': item.pop('cargo_movement', None),
        'volume': volume,
        'volume_unit': units,
    }

    for _col in ('berthed', 'eta', 'departure'):
        item.pop(_col, None)

    return item


def charters_mapping():
    return {
        'Berth': ('zone', None),
        'No.': ignore_key('irrelevant'),
        'Vessel Name': ('vessel', lambda x: {'name': x} if 'NIL' not in x else None),
        'ETA': ('eta', None),
        'ETB': ('berthed', None),
        'ETS': ('departure', None),
        'Supp/Rcvr': ignore_key('irrelevant'),
        'Charterer': ('charterer', None),
        'L/D': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x, None)),
        'Quantity': ('cargo_volume', None),
        'Grade': ('cargo_product', None),
        'Last/Next Port': ignore_key('irrelevant'),
        'reported_date': ('reported_date', lambda x: parse_date(x).strftime('%d %b %Y')),
        'provider_name': ('provider_name', None),
    }


def normalize_vol_unit(raw_vol, raw_product):
    """Normalize volume and units, when product is lng, the figure has
    to be multiplied by 1000

    Examples:
        >>> normalize_vol_unit('123kt', 'ethane')
        ('123', 'kilotons', 'ethane')
        >>> normalize_vol_unit('123', 'LNG')
        ('123000', 'cubic_meter', 'LNG')
        >>> normalize_vol_unit('TBA', 'LNG')
        (None, None, 'LNG')

    Args:
        raw_vol (str): may contain units inside
        raw_product (str):

    Returns:
        Tuple[str, str, str]
    """
    _match = re.match(r'([0-9.]+)([A-z]+)?', raw_vol)

    if _match:
        f_vol, f_units = _match.groups()

        if not f_units and 'LNG' in raw_product:
            f_units = Unit.cubic_meter
            f_vol = int(f_vol) * 1000

        if f_units:
            f_units = UNIT_MAPPING.get(f_units.upper(), f_units)

        return str(f_vol), f_units, raw_product

    return None, None, raw_product


def _process_import_charter(item, trade):
    """Transform an import spot charter into a properly mapped export spot charter.

    Args:
        item (Dict[str, str]):
        trade (Dict[str, str] | None):
    """
    # laycan period should be +/- 1 day from trade date (c.f. analysts)
    lay_can = parse_date(trade['Date (origin)'], dayfirst=False) if trade else None
    item['lay_can_start'] = (lay_can - dt.timedelta(days=1)).isoformat() if trade else None
    item['lay_can_end'] = (lay_can + dt.timedelta(days=1)).isoformat() if trade else None
    # use origin port as departure zone, destination port as arrival zone
    # if no API match, let analysts manually match since no previous port provided
    item['departure_zone'] = trade['Origin'] if trade else None
    item['arrival_zone'] = [trade['Destination']] if trade else [item['departure_zone']]


def normalize_dates(raw_date, rep_date):
    """Normalize volume and units

    Examples:
        >>> normalize_dates('SLD 02/01/19', '01 Jan 2019')
        '2019-01-02T00:00:00'
        >>> normalize_dates('15 - MAR', '01 Jan 2019')
        '2019-03-15T00:00:00'
        >>> normalize_dates('31 - DEC', '01 Jan 2019')
        '2018-12-31T00:00:00'
        >>> normalize_dates('01 - JAN', '01 Jan 2019')
        '2019-01-01T00:00:00'
        >>> normalize_dates('ANCH', '01 Jan 2019')

    Args:
        raw_date (str):
        rep_date (str):

    Returns:
        str:
    """
    if raw_date:
        _year = parse_date(rep_date, dayfirst=True).year

        _match_full_date = re.match(r'[A-z ]+(\d+\/\d+\/\d+)', raw_date)
        if _match_full_date:
            return to_isoformat(_match_full_date.group(1), dayfirst=True)

        _match_short_date = re.match(r'([0-9]+)\W+([A-z]+)', raw_date)
        if _match_short_date:
            _day, _month = _match_short_date.groups()
            _month = parse_date(f'{_day} {_month}', dayfirst=True).month
            _date = dt.datetime(year=int(_year), month=int(_month), day=int(_day))

            # to accomodate end of year parsing, prevent dates too old or far into
            # the future. 100 days was chosen as a gauge
            if (_date - parse_date(rep_date)).days < -100:
                _date = dt.datetime(year=int(_year) + 1, month=(_month), day=int(_day))

            if (_date - parse_date(rep_date)).days > 100:
                _date = dt.datetime(year=int(_year) - 1, month=int(_month), day=int(_day))
            return _date.isoformat()

        return None

    return None
