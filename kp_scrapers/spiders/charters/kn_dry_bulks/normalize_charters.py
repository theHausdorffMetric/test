import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import is_isoformat, to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.services import kp_api
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


UNIT_MAPPING = {'MT': Unit.tons}


CHARTERER_BLACKLIST = ['?', '-']


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, charters_mapping(), skip_missing=True)

    # discard items with no charterer field
    if 'charterer' not in item.keys():
        return

    item['vessel'] = {'name': item.pop('vessel_name')}

    item['cargo_volume'], item['cargo_units'] = normalize_volume_unit(item['cargo_volume_unit'])

    # build cargo sub-model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': item.pop('cargo_units', None),
    }

    # FIXME: API trick does not work if products were intentionally left out
    # to use veson to back calculate
    _trade = None
    # get trade from cpp or lpg platform
    if item['lay_can_start']:
        _trade = kp_api.get_session('coal', recreate=True).get_import_trade(
            vessel=item['vessel']['name'],
            origin=item['departure_zone'],
            dest=item['arrival_zone'][0],
            end_date=item['lay_can_start'],
        )

    # mutate item with relevant laycan periods and load/discharge port info
    post_process_import_item(item, _trade)

    item.pop('cargo_volume_unit', None)

    return item


def charters_mapping():
    return {
        'PORT': (ignore_key('irrelevant')),
        'VSL NAME': ('vessel_name', lambda x: normalize_vessel_name(x)),
        'CARGO': ('cargo_product', None),
        'QNTY': ('cargo_volume_unit', None),
        'OWNERS ': (ignore_key('irrelevant')),
        'DIS PORT': ('arrival_zone', lambda x: [x if x != '' else x]),
        'LOAD PORT': ('departure_zone', lambda x: normalize_departure_zone(x)),
        'SHIPPERS': (ignore_key('irrelevant')),
        'SHIPPER': (ignore_key('irrelevant')),
        'RECEIVERS': (ignore_key('irrelevant')),
        'RECEIVER': (ignore_key('irrelevant')),
        'CHARTS': ('charterer', lambda x: normalize_charterer(x)),
        'ARRIVED': (ignore_key('irrelevant')),
        'BERTHED': ('lay_can_start', lambda x: normalize_laycan(x)),
        'SAILED': (ignore_key('irrelevant')),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_vessel_name(raw_vessel_name):
    """Normalize vessel names, remove unnessary letters.

    Examples:
        - MV ORARLE
        - MV.ORACLE

    Examples:
        >>> normalize_vessel_name('MV ORACLE')
        'ORACLE'
        >>> normalize_vessel_name('ORACLE')
        'ORACLE'

    Args:
        raw_vessel_name (str):

    Returns:
        str:

    """
    _match = re.match(r'^MV(?:[. ])?(.*)', raw_vessel_name)
    if _match:
        return _match.group(1)

    return raw_vessel_name


def normalize_volume_unit(raw_volume_unit):
    """Normalize volume unit strings

    Examples:
        - 10000MT
        - 10000
        - 10000 MT

    Examples:
        >>> normalize_volume_unit('10000MT')
        ('10000', 'tons')
        >>> normalize_volume_unit('10000')
        ('10000', 'tons')

    Args:
        raw_volume_unit (str):

    Returns:
        str, str:

    """
    _match = re.match(r'([0-9.,]+)([A-z ]+)?', raw_volume_unit)
    if _match:
        volume, units = _match.groups()
        volume = volume.replace(',', '')
        units = UNIT_MAPPING.get(may_strip(units), may_strip(units)) if units else Unit.tons

        return volume, units


def normalize_charterer(raw_charterer):
    """Normalize charterer

    Examples:
        - '-'
        - 'SAIL'


    Examples:
        >>> normalize_charterer('-')
        ''
        >>> normalize_charterer('SAIL')
        'SAIL'

    Args:
        raw_charterer (str):

    Returns:
        str:
    """
    for x in CHARTERER_BLACKLIST:
        if raw_charterer == x:
            return ''

    return raw_charterer


def normalize_laycan(raw_laycan):
    """Normalize laycan

    Examples:
        - '02.02.19'


    Examples:
        >>> normalize_laycan('02.02.19')
        '2019-02-02T00:00:00'

    Args:
        raw_laycan (str):

    Returns:
        str:
    """
    return raw_laycan if is_isoformat(raw_laycan) else to_isoformat(raw_laycan, dayfirst=True)


def normalize_departure_zone(raw_departure):
    """Normalize departure zone

    Examples:
        - 'NGUYEN, VIETNAM'

    Examples:
        >>> normalize_departure_zone('NGUYEN, VIETNAM')
        'VIETNAM'

    Args:
        raw_laycan (str):

    Returns:
        str:
    """
    _match = re.match(r'(?:[A-z ]+)?\,\s?(.*)', raw_departure)
    if _match:
        return _match.group(1)

    return raw_departure


def post_process_import_item(item, trade):
    """Transform an import spot charter into a properly mapped export spot charter.

    Args:
        item (Dict[str, str]):
        trade (Dict[str, str] | None):

    """
    if trade:
        # laycan period should be +/- 1 day from trade date (c.f. analysts)
        lay_can = parse_date(trade['Date (origin)'], dayfirst=False)
        item['lay_can_start'] = (lay_can - dt.timedelta(days=1)).isoformat()
        item['lay_can_end'] = (lay_can + dt.timedelta(days=1)).isoformat()
        # use origin port as departure zone, destination port as arrival zone
        item['departure_zone'] = trade['Origin']
        item['arrival_zone'] = [trade['Destination']]
    else:
        item['lay_can_start'] = None
        item['lay_can_end'] = None
        # use previous port as departure zone, current port as arrival zone
        item['departure_zone'] = item['departure_zone']
        item['arrival_zone'] = item['arrival_zone']
