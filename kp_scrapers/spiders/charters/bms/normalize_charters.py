import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_apply, may_strip
from kp_scrapers.lib.services import kp_api
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


CHARTERER_NAME_BLACKLIST = ['', 'CNR', 'TBA']

DATE_BLACKLIST = ['N/A', 'TBA']

ZONE_MAPPING = {
    'For orders': 'OPTIONS',
    'Borga (Porvoo)': 'Borga',
    'Naantali (Nadendal)': 'Naantali',
    'Kaliningrad region': 'Kaliningrad',
    'Estonian ports 2': 'Estonia',
}

CHARTER_MAPPING = {'sub': SpotCharterStatus.on_subs, 'subs': SpotCharterStatus.on_subs}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, charters_mapping())

    # discard items without vessel names
    vessel_name, charter_status = item.pop('vessel_name_and_charter_status')
    if not vessel_name:
        logger.warning(f'Item has no vessel name, discarding:\n{item}')
        return

    # assign charter status, if any
    item['status'] = CHARTER_MAPPING.get(charter_status.lower())

    # build a proper vessel dict according to Vessel model
    item['vessel'] = {'name': vessel_name, 'imo': item.pop('vessel_imo', None)}

    # get buyer or seller
    if item['is_export']:
        player = 'seller'
    else:
        player = 'buyer'

    cargo_player = item.pop('buyer_seller', None)
    # build a proper cargo dict according to Cargo model
    item['cargo'] = {
        'product': item.pop('product', None),
        'volume': item.pop('volume', None),
        'volume_unit': Unit.tons,
        'movement': 'load',
        player: {'name': cargo_player} if cargo_player else None,
    }

    # sometimes, `lay_can_start` may be missing. we'll use alternate date
    if not item.get('lay_can_start'):
        item['lay_can_start'] = item.pop('lay_can_start_alt')

    # post-process import spot charters
    # this is necessary since by default,
    # spot charters are defined by their export dates, not import dates
    if not item['is_export']:
        _trade = None
        if item['lay_can_start']:
            # get trade from either oil or cpp platform
            for platform in ('oil', 'cpp'):
                _trade = kp_api.get_session(platform, recreate=True).get_import_trade(
                    vessel=item['vessel']['name'],
                    origin=item['previous_zone'],
                    dest=item['current_zone'],
                    end_date=item['lay_can_start'],
                )
                if _trade:
                    break

        # mutate item with relevant laycan periods and load/discharge port info
        post_process_import_item(item, _trade)

    # discard irrelevant fields
    for field in ('lay_can_start_alt', 'is_export', 'current_zone', 'previous_zone'):
        item.pop(field, None)

    # need cargo info for BMS_Charters_Clean
    if raw_item['spider_name'] != 'BMS_Charters_Clean':
        item.pop('cargo')

    yield item


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
        item['departure_zone'] = item['previous_zone']
        item['arrival_zone'] = [item['current_zone']]


def charters_mapping():
    return {
        'Arrived': ('lay_can_start', normalize_laycan_date),
        'AGENT': ('shipping_agent', None),
        'Berthed': ('lay_can_start_alt', normalize_laycan_date),
        'BL DD': ('lay_can_start_alt', normalize_laycan_date),
        'CHARTERER': ('charterer', normalize_charterer),
        'COUNTRY OF DEST': (ignore_key('not specific enough; we already have "NEXT PORT"')),
        'ETA': ('lay_can_start', normalize_laycan_date),
        'ETB': ('lay_can_start_alt', normalize_laycan_date),
        'ETS': ('lay_can_end', normalize_laycan_date),
        'GRADE DETAIL': ('product', None),
        'GRADE GROUP': (ignore_key('cargo is being extracted by grades spider')),
        'IMO NR': ('vessel_imo', lambda x: may_apply(x, float, int, str) if x else None),
        'LOAD POSITION': (ignore_key('irrelevant')),
        'LOAD/DISCH': ('is_export', lambda x: x.lower() == 'load'),
        'NEXT PORT': ('arrival_zone', lambda x: [ZONE_MAPPING.get(x) or x.upper()] if x else None),
        'PORT': ('current_zone', None),
        'PRE. PORT': ('previous_zone', None),
        'provider_name': ('provider_name', None),
        'QTT IN MT': ('volume', None),
        'region_name': ('departure_zone', lambda x: ZONE_MAPPING.get(x, x)),
        'reported_date': ('reported_date', normalize_reported_date),
        'Sailed': ('lay_can_end', normalize_laycan_date),
        'SHIPPERS/RECEIVERS': ('buyer_seller', lambda x: x.split('/')[-1] if x else None),
        'STATUS': (ignore_key('irrelevant')),
        'TERMINAL': (ignore_key('not required for spot charters yet')),
        'VESSEL': (
            'vessel_name_and_charter_status',
            # don't use the separator value
            lambda x: [may_strip(each) for idx, each in enumerate(x.partition('/')) if idx != 1],
        ),
    }


def normalize_laycan_date(date_str):
    """Cleanup ISO8601 laycan-related date.

    Args:
        raw_date (str):

    Returns:
        str | None: ISO8601-formatted date string

    Examples:
        >>> normalize_laycan_date('2018-07-28T23:45:00')
        '2018-07-28T23:45:00'
        >>> normalize_laycan_date('N/A')
        >>> normalize_laycan_date('')

    """
    if not date_str or any(sub in date_str for sub in DATE_BLACKLIST):
        return None

    return date_str


def normalize_reported_date(raw_date):
    """Normalize raw reported date into an ISO8601-formatted string.

    This function WILL discard timezone data.
    Also, take note that we need to send reported date as a DD MMM YYYY string.

    Args:
        raw_date (str):

    Returns:
        str: ISO8601-formatted date string

    Examples:
        >>> normalize_reported_date('Fri, 5 Jul 2018 17:15:23 +0200')
        '05 Jul 2018'
        >>> normalize_reported_date('BMS Crude & Fuel')

    """
    date_match = re.match(r'.*:\d+(?!\+)', raw_date)
    return parse_date(date_match.group(0)).strftime('%d %b %Y') if date_match else None


def normalize_charterer(raw_charterer):
    """Normalize raw charterer string.

    This function will remove "/" and "?" suffixes.

    Examples:
        >>> normalize_charterer('LITASCO/NESTE?')
        'LITASCO/NESTE'
        >>> normalize_charterer('NESTE?')
        'NESTE'
        >>> normalize_charterer('NESTE/')
        'NESTE'
        >>> normalize_charterer('CNR')
        >>> normalize_charterer('')

    """
    charterer = raw_charterer[:-1] if raw_charterer.endswith(('/', '?')) else raw_charterer
    return None if charterer in CHARTERER_NAME_BLACKLIST else charterer
