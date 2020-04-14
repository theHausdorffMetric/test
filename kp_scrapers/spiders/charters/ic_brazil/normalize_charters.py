import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.services import kp_api
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


BLACKLIST = ['TBC', 'N/A', '-', 'NIL', 'TBI']


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:
    """
    item = map_keys(raw_item, field_mapping())

    # remove vessels not named yet
    if not item['vessel']['name']:
        return

    if item['raw_port_name']:
        item['port_name'] = item['raw_port_name']

    item.pop('raw_port_name', None)

    item['cargo_unit'] = Unit.tons if item['cargo_volume'] else None

    # build cargo sub model
    item['cargo'] = {
        'product': may_strip(item.pop('cargo_product')),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': item.pop('cargo_unit', None),
    }

    # check if vessel movement is import/export
    # export movements can just be returned as is, since a SpotCharter is defined by exports
    if item['is_export'] == 'load':
        item['departure_zone'] = item['port_name']
        item['arrival_zone'] = [item['next_zone']]

    # import movements needed to be treated specially to obtain the proper laycan dates
    else:
        _trade = None
        # get trade from cpp or oil platform
        _platforms = ['oil', 'cpp']
        if item['lay_can_start']:
            for platform in _platforms:
                _trade = kp_api.get_session(platform, recreate=True).get_import_trade(
                    vessel=item['vessel']['name'],
                    origin=item['previous_zone'],
                    dest=item['port_name'],
                    end_date=item['lay_can_start'],
                )
                if _trade:
                    break

        # mutate item with relevant laycan periods and load/discharge port info
        post_process_import_item(item, _trade)

    for x in ['raw_port_name', 'port_name', 'previous_zone', 'next_zone', 'is_export']:
        item.pop(x, None)

    return item


def field_mapping():
    return {
        'VESSEL': ('vessel', lambda x: {'name': normalize_string(x)}),
        'ETA': ignore_key('redundant'),
        'ETB': ('lay_can_start', lambda x: normalize_string(x)),
        'ETS': ignore_key('redundant'),
        'WAITING TIME (DAYS)': ignore_key('redundant'),
        'WT': ignore_key('redundant'),
        'BERTH': ignore_key('redundant'),
        'STATUS': ignore_key('redundant'),
        'OPERATION': ('is_export', lambda x: x.lower()),
        'DESTINATION': ('next_zone', lambda x: normalize_portname(normalize_string(x))),
        'CHARTERER': ('charterer', lambda x: normalize_string(x)),
        'ORIGIN': ('previous_zone', None),
        'COMMODITY': ('cargo_product', lambda x: normalize_string(x)),
        'PRODUCT': ('cargo_product', lambda x: normalize_string(x)),
        'QUANTITY': ('cargo_volume', lambda x: normalize_string(try_apply(x, str))),
        'SHIPPER': ignore_key('redundant'),
        'RECEIVER': ignore_key('redundant'),
        'AGENT': ignore_key('redundant'),
        'raw_port_name': ('raw_port_name', lambda x: normalize_portname(x)),
        'PORT': ('port_name', lambda x: normalize_portname(x)),
        'provider_name': ('provider_name', None),
        'reported_date': (
            'reported_date',
            lambda x: dt.datetime.strptime(x[:10], '%Y-%m-%d').strftime('%d %b %Y'),
        ),
    }


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
        item['arrival_zone'] = [item['port_name']]


def normalize_string(raw_value):
    """Remove unnecessary strings

    Args:
        raw_value (str):

    Examples:
        >>> normalize_string(None)
        >>> normalize_string('TBC')
        >>> normalize_string('DHT LEOPARD')
        'DHT LEOPARD'
    """
    if (raw_value and str(raw_value) in BLACKLIST) or raw_value == '':
        return None

    return may_strip(raw_value)


def normalize_portname(raw_portname):
    """Remove unnecessary strings in port names to increase matching rate

    Args:
        raw_value (str):

    Examples:
        >>> normalize_portname('BRAZIL (BR)')
        'BRAZIL'
        >>> normalize_portname('Sao Luis Port')
        'Sao Luis'
    """
    if raw_portname and 'Port' in raw_portname:
        return raw_portname.replace(' Port', '')

    if raw_portname:
        match = re.match(r'(.*) \(.*', raw_portname)
        return match.group(1) if match else raw_portname

    return raw_portname
