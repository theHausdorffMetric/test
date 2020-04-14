import datetime as dt
import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.services import kp_api
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


IRRELEVANT_PLAYERS = ['requested']


PRODUCT_MAPPING = {
    'ME': 'Merey 16',
    'MS': 'Mesa 30',
    'DCO': 'Diluted Crude Oil',
    'PZH': 'PetroZuataHeavy',
    'SBH': 'Hamaca Blend',
    'MO': 'Morichal 16',
    'Z-300': 'Zuata 300',
    'SB': 'Santa Barbara',
    'GAN': 'Natural Gasoline',
    'LVN': 'Light Virgin Naphtha',
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

    # build cargo sub-model
    volume = item['cargo_volume'] if item.get('cargo_volume') else item['cargo_volume_nominated']
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': volume,
        'volume_unit': Unit.kilobarrel,
    }

    # if date is blank assume reported date
    if not item.get('laycan'):
        item['laycan'] = parse_date(item['reported_date'], dayfirst=True).strftime('%Y-%m-%d')

    # check if vessel movement is import/export
    # export movements can just be returned as is, since a SpotCharter is defined by exports
    if 'export' in item['sheet_name']:
        item['departure_zone'] = 'Port of Jose'
        item['arrival_zone'] = ''
        item['lay_can_start'] = to_isoformat(item.pop('laycan', None))

    # import movements needed to be treated specially to obtain the proper laycan dates
    if 'import' in item['sheet_name']:
        item['arrival_zone'] = 'Port of Jose'
        _trade = None
        # get trade from cpp or oil platform
        for platform in ('oil', 'cpp'):
            _trade = kp_api.get_session(platform, recreate=True).get_import_trade(
                vessel=item['vessel']['name'],
                origin='',
                dest=item['arrival_zone'],
                end_date=item['laycan'],
            )
            if _trade:
                break

        # mutate item with relevant laycan periods and load/discharge port info
        post_process_import_item(item, _trade)

    # cleanup unused fields
    for field in ('laycan', 'sheet_name', 'cargo_volume_nominated', 'cargo_volume'):
        item.pop(field, None)

    return item


def charters_mapping():
    return {
        'Date': ('laycan', to_isoformat),
        'Dock': ignore_key('irrelevant'),
        'Vessel': ('vessel', lambda x: {'name': x}),
        'Charterer': ('charterer', normalize_player_name),
        'Grade': ('cargo_product', lambda x: PRODUCT_MAPPING.get(x.upper(), x)),
        'Qty Nominated': ('cargo_volume_nominated', lambda x: try_apply(x, float, int, str)),
        'Qty Loaded': ('cargo_volume', lambda x: try_apply(x, float, int, str)),
        'Loading Rate': ignore_key('irrelevant'),
        'Status': ignore_key('irrelevant'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: parse_date(x).strftime('%d %b %Y')),
        'sheet_name': ('sheet_name', None),
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
        item['departure_zone'] = ''
        item['arrival_zone'] = [item['arrival_zone']]


def normalize_player_name(raw_player):
    """Replace unknown player name with Charterer Not Reported ('CNR')

    Args:
        raw_player (str):

    Returns:
        str:

    Examples:
        >>> normalize_player_name('UNIPEC')
        'UNIPEC'
        >>> normalize_player_name('Requested')

    """
    return None if raw_player.lower() in IRRELEVANT_PLAYERS else raw_player
