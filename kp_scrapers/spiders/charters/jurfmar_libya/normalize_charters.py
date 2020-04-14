import datetime as dt
import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.services import kp_api
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_PLAYERS = ['', '???']

UNIT_MAPPING = {'MT': Unit.tons, 'KB': Unit.barrel}

CRUDE_PRODUCTS = [
    'AMNA/SIRTICA BLEND',
    'AMNA',
    'SIRTICA',
    'SARIR/MESLA C.O.',
    'ES SIDER C.O',
    'BREGA C.O',
    'BU ATTIFEL',
    'EL SHARARA C.O.',
    'SARIR',
    'MESLA',
]

SPECIAL_PORT_MAPPING = {
    'BENGZHI, LIBYA': 'Benghazi',
    'CHINA ORDERS': 'China',
    'ELEFSIS, GREECE': 'Elefsina',
    'FOR ORDERS': 'Options',
    'HULVA, SPAIN': 'Huelva',
    'LA CORUNA, SPAIN': 'Coruna',
    'LAVERA, FRANCE': 'Fos',
    'M. BREGA': 'Marsa Al Brega',
    'M. HARIGA': 'Marsa Al Hariga',
    'M.BREGA': 'Marsa Al Brega',
    'M.HARIGA': 'Marsa Al Hariga',
    'MALTA OPL': 'Malta',
    'NAPOLI, ITALY': 'Naples',
    'SOUTH CRETE, GREECE': 'Sea of Crete',
    'TAMAN, RUSSIA': 'Taman',
    'TEMRYUK, RUSSIA': 'Black Sea',
    'VASSILIKO,CY': 'Cyprus',
}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, charters_mapping(), skip_missing=True)
    # completely disregard cancelled vessel movements
    if item['cancelled']:
        return

    # remove vessels that have not been named
    if 'TBN' in item['vessel_name']:
        return

    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('vessel_imo')}
    if not item['lay_can_start']:
        item['lay_can_start'] = item['lay_can_start_alt']

    # check if vessel movement is import/export
    # export movements can just be returned as is, since a SpotCharter is defined by exports
    if item['is_export']:
        item['departure_zone'] = item['current_zone']
        item['arrival_zone'] = [item['next_zone']]

    # import movements needed to be treated specially to obtain the proper laycan dates
    else:
        _trade = None
        # get trade from cpp or lpg platform
        if item['lay_can_start']:
            for platform in ('oil', 'cpp', 'lpg'):
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

    # build cargo sub-model for lpg product
    if item.get('cargo_product'):

        # because the volume/product column may be swapped, we need to check before we normalize
        # the product/volume individually
        if item['cargo_product'][0].isdigit():
            item['cargo_volume'], item['cargo_product'] = (
                item['cargo_product'],
                item['cargo_volume'],
            )

        volume, unit = normalize_cargo_volume(item['cargo_volume'])
        item['cargo'] = {
            'product': item.pop('cargo_product', None),
            'movement': 'load',
            'volume': volume,
            'volume_unit': unit,
        }

    # pop for crude oil, granular grades inserted in grades spider
    if item['cargo']['volume_unit'] == Unit.barrel:
        item['cargo']['product'] = 'crude oil'

    # cleanup unused fields
    for field in (
        'previous_zone',
        'current_zone',
        'next_zone',
        'is_export',
        'lay_can_start_alt',
        'cancelled',
        'cargo_product',
        'cargo_volume',
    ):
        item.pop(field, None)

    return item


def charters_mapping():
    return {
        'B/L DATE': (ignore_key('irrelevant')),
        'CHARTERER': ('charterer', normalize_player_name),
        'ETA': ('lay_can_start_alt', normalize_laycan_date),
        'ETS': ('lay_can_start', normalize_laycan_date),
        'GRADE': ('cargo_product', None),
        'GRADE ': ('cargo_product', None),
        'IMO': ('vessel_imo', lambda x: try_apply(x, float, int, str)),
        'IMO NUMBER': ('vessel_imo', lambda x: try_apply(x, float, int, str)),
        'LOAD/DISCHARGE': ('is_export', lambda x: 'load' in x.lower()),
        'NEXT PORT': ('next_zone', normalize_port_name),
        'PORT': ('current_zone', normalize_port_name),
        'PREVIOUS PORT': ('previous_zone', normalize_port_name),
        'provider_name': ('provider_name', None),
        'QUANTITY': ('cargo_volume', None),
        'REMARK': (ignore_key('irrelevant')),
        'reported_date': ('reported_date', None),
        'STATUS': ('cancelled', lambda x: 'cancel' in x),
        'SUPPLIER': ('seller', normalize_player_name),
        'VESSEL': ('vessel_name', None),
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
        item['arrival_zone'] = [item['current_zone']]


def normalize_laycan_date(raw_date):
    """Normalize raw laycan date to an ISO-8601 formatted string.

    Args:
        raw_date (str):

    Returns:
        str | None:

    Examples:
        >>> normalize_laycan_date('AM/08.01.18')
        '2018-01-08T06:00:00'
        >>> normalize_laycan_date('PM/03.01.18')
        '2018-01-03T18:00:00'
        >>> normalize_laycan_date('1930/12.07.18')
        '2018-07-12T19:30:00'
        >>> normalize_laycan_date('NOON/18.07.18')
        '2018-07-18T12:00:00'
        >>> normalize_laycan_date('24.08.18')
        '2018-08-24T00:00:00'
        >>> normalize_laycan_date('')
        >>> normalize_laycan_date('UNDER BERTHING')

    """
    if not raw_date or not has_numbers(raw_date):
        return None

    # map arbitrary time designations into something consistent
    time_map = {'AM': '0600', 'PM': '1800', 'NOON': '1200'}

    time, _, date = raw_date.partition('/')
    return to_isoformat(' '.join((date, time_map.get(time, time))), dayfirst=True)


def normalize_port_name(raw_port):
    """Normalize port name to allow for more precise mapping downstream.

    If no mapping given, strip string of substring after commas.

    Args:
        raw_port (str):

    Returns:
        str:

    Examples:
        >>> normalize_port_name('ELEFSIS, GREECE')
        'Elefsina'
        >>> normalize_port_name('Tripoli (Libya)')
        'Tripoli'
        >>> normalize_port_name('BILBAO, SPAIN')
        'BILBAO'
        >>> normalize_port_name('UK')
        'UK'

    """
    return SPECIAL_PORT_MAPPING.get(raw_port, may_strip(raw_port.split(',')[0].split('(')[0]))


def normalize_player_name(raw_player):
    """Replace unknown player name with Charterer Not Reported ('CNR')

    Args:
        raw_player (str):

    Returns:
        str:

    Examples:
        >>> normalize_player_name('UNIPEC')
        'UNIPEC'
        >>> normalize_player_name('???')
        >>> normalize_player_name('')

    """
    return None if raw_player in IRRELEVANT_PLAYERS else raw_player


def has_numbers(input):
    """Check if input string contains any numeric characters.

    TODO could be made generic

    Args:
        input (str):

    Returns:
        bool: True if at least one numeric char

    """
    return any(char.isdigit() for char in input)


def normalize_cargo_volume(raw_volume):
    """Normalize raw cargo volume to a valid volume and volume_unit.

    Args:
        raw_volume (str):

    Returns:
        Tuple[str, str]: tuple of (volume, volume_unit)

    Examples:
        >>> normalize_cargo_volume('9000 MT')
        ('9000', 'tons')
        >>> normalize_cargo_volume('600 KB +/-5%')
        ('600000', 'barrel')
        >>> normalize_cargo_volume('')
        (None, None)
        >>> normalize_cargo_volume('300 KN')
        ('300', None)
    """
    if not raw_volume:
        return None, None

    # obtain unit from string
    for alias in UNIT_MAPPING:
        if alias in raw_volume:
            unit = UNIT_MAPPING[alias]
            break
        else:
            unit = None

    # special volume transformation for `KB -> barrel` unit
    return raw_volume.split()[0] + ('000' if unit == 'barrel' else ''), unit
