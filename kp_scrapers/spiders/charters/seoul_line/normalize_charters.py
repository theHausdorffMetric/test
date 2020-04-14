import datetime as dt
import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import is_isoformat, to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.services import kp_api
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


IRRELEVANT_PATTERN = ['CNR', 'TBN']


MOVEMENT_MAPPING = {'load': 'load', 'disch': 'discharge'}


PORT_MAPPING = {
    'keoje': 'yeosu',
    'daesan': 'daesan',
    'okyc': 'yeosu',
    'ulsan': 'ulsan',
    'yosu': 'yeosu',
}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, charters_mapping())

    # remove vessels not named
    if not item['vessel']['name']:
        return

    movement = item.pop('cargo_movement', '')
    item['current_port'] = PORT_MAPPING.get(item['current_port'], item['current_port'])

    # build a proper cargo dict according to Cargo model
    item['cargo'] = {'product': 'crude/co', 'volume': None, 'volume_unit': None, 'movement': 'load'}

    item['lay_can_start'] = normalize_dates(item['lay_can_start'], item['year'])

    if 'discharge' in movement:
        _trade = None
        if item['lay_can_start']:
            # get trade from either oil or cpp platform
            for platform in ('oil', 'cpp'):
                _trade = kp_api.get_session(platform, recreate=True).get_import_trade(
                    vessel=item['vessel']['name'],
                    origin=item.get('load_dis_port', ''),
                    dest=item['current_port'],
                    end_date=item['lay_can_start'],
                )
                if _trade:
                    break

        # mutate item with relevant laycan periods and load/discharge port info
        post_process_import_item(item, _trade)

    if 'load' in movement:
        item['departure_zone'] = item['current_port']
        item['arrival_zone'] = (
            item['load_dis_port'] if 'REVERT' not in item['load_dis_port'] else ''
        )

    for field in ('load_dis_port', 'current_port', 'year'):
        item.pop(field, None)

    return item


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
        item['arrival_zone'] = [trade['Destination']]
        item['departure_zone'] = trade['Origin']
    else:
        item['lay_can_start'] = None
        item['lay_can_end'] = None
        item['departure_zone'] = None
        item['arrival_zone'] = item['departure_zone']


def charters_mapping():
    return {
        'VESSEL': ('vessel', lambda x: {'name': may_strip(x) if x else None}),
        'ETA': ignore_key('eta'),
        'ETB': ('lay_can_start', None),
        'ETD': ignore_key('departure'),
        'GRADE': ignore_key('grades spider'),
        'QTY(K.BLS)': ignore_key('grades spider'),
        'LESSEE': ignore_key('lessee'),
        'L/D': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x.lower(), x)),
        'DISPORT': ('load_dis_port', lambda x: x.partition('(')[0]),
        'CHARTERER': ('charterer', None),
        'reported_date': ('reported_date', lambda x: parse_date(x).strftime('%d %b %Y')),
        'provider_name': ('provider_name', None),
        'port_name': ('current_port', lambda x: may_strip(x.lower().replace('line-up', ''))),
        'year': ('year', None),
    }


def normalize_dates(raw_date, raw_year):
    """Normalize raw laycan date.

    Args:
        raw_date (str):
        raw_year (str):

    Returns:
        str:

    Examples:
        >>> normalize_dates('2/8', '2019')
        '2019-02-08T00:00:00'
        >>> normalize_dates('2/8 11:00', '2019')
        '2019-02-08T11:00:00'
    """
    if not is_isoformat(raw_date):
        datetime_array = raw_date.split(' ')
        if len(datetime_array) == 1:
            try:
                return to_isoformat(f'{datetime_array[0]}/{raw_year}', dayfirst=False)
            except Exception:
                return raw_date

        if len(datetime_array) == 2:
            if datetime_array[1] in ['2400', '24:00']:
                datetime_array[1] = '0000'

            if datetime_array[1].replace('.', '').lower() == 'am':
                datetime_array[1] = '0900'

            if datetime_array[1].replace('.', '').lower() == 'pm':
                datetime_array[1] = '1500'

            try:
                return to_isoformat(
                    f'{datetime_array[0]}/{raw_year} {datetime_array[1]}', dayfirst=False
                )
            except Exception:
                return raw_date

    return raw_date
