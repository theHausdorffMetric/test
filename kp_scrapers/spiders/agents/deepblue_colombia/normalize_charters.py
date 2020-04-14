import datetime as dt
from datetime import datetime
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import is_isoformat, to_isoformat
from kp_scrapers.lib.parser import may_remove_substring
from kp_scrapers.lib.services import kp_api
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

KP_API_DATE_PARAM_FORMAT = '%Y-%m-%d'
CHARTERER_NAME_BLACKLIST = ['', 'CNR', 'TBA', 'TBC']
STRING_BLACKLIST = ['TBA', 'TBC', 'N/A']
MOVEMENT_MAPPING = {
    'DISCHARGING': 'discharge',
    'TO DISCHARGE': 'discharge',
    'DISCHARGED': 'discharge',
    'LOADING': 'load',
    'TO LOAD': 'load',
    'LOADED': 'load',
}
PLATFORMS = ['oil', 'lng', 'cpp', 'lpg', 'coal']
ABBREVIATIONS = ["a.m", "am", "p.m", "pm"]
LETTERS = {
    'ñ': 'n',
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

    # discard items without vessel names
    if not item.get('vessel_name'):
        logger.warning(f'Item has no vessel name, discarding:\n{item}')
        return

    # sometimes, `lay_can_start` may be missing. we'll use alternate date
    if not item.get('lay_can_start'):
        item['lay_can_start'] = item.pop('lay_can_start_alt')

    # check if years of etc, eta and etb are similar to the reported year
    item = normalize_check_dates(item, ['lay_can_start', 'lay_can_start_alt', 'lay_can_end'])

    # take into account barrel unit
    volume = item.get('cargo_volume', None)
    cargo_type = item.pop('cargo_type', None)
    if volume is not None:
        if cargo_type == 'CRUDE':
            item['cargo_volume_unit'] = Unit.barrel
        else:
            item['cargo_volume_unit'] = Unit.tons

    # process item conditionally to the existence of charterer and cargo movement
    processed_item = []

    if not item.get('charterer'):
        logger.warning(f'Item has no valid charterer, discarding:\n{item}')
        return

    if not item.get('cargo_movement'):
        logger.warning(f'Item has no valid cargo movement, discarding:\n{item}')
        return

    if item.get('cargo_movement') == 'load':
        processed_item = process_exports_item(item)

    if item.get('cargo_movement') == 'discharge':
        processed_item = process_imports_item(item)

    yield processed_item


def charters_mapping():
    return {
        'DATE OF ARRIVAL': ('lay_can_start', normalize_laycan_date),
        'AGENCY': ('shipping_agent', None),
        'CHARTERER': ('charterer', normalize_charterer),
        'ETA': ('lay_can_start', normalize_laycan_date),
        'ETB': ('lay_can_start_alt', normalize_laycan_date),
        'ETC': ('lay_can_end', normalize_laycan_date),
        'PRODUCT': ('cargo_product', None),
        'TYPE': ('cargo_type', None),
        'OPERATION': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x)),
        'PORT LOAD/DISCH': ('load_disch_zone', normalize_port_name),
        'port_name': ('current_zone', normalize_port_name),
        'provider_name': ('provider_name', None),
        'TOTAL MT': ('cargo_volume', normalize_volume),
        'reported_date': ('reported_date', normalize_reported_date),
        'SHIPOWNER': ('shipowner', None),
        'STATUS': (ignore_key('irrelevant')),
        'TERMINAL': (ignore_key('not required for spot charters yet')),
        'VESSEL': ('vessel_name', None),
    }


def normalize_laycan_date(date_str):
    """Cleanup laycan-related date.

    Args:
        date_str (str):

    Returns:
        str | None: date string without "am" or 'pm' features

    """
    if not date_str or any(sub in date_str for sub in STRING_BLACKLIST):
        return None

    # remove 'am' and 'pm' abbreviations in laycan-dates
    if not is_isoformat(date_str):
        for abbreviation in ABBREVIATIONS:
            if re.compile(abbreviation).search(date_str):
                date_str = date_str.replace(abbreviation, '')
                # assume hours of arrival for 'am' and 'pm' equal to 06:00 and 18:00
                if abbreviation == 'a.m' or abbreviation == 'am':
                    date_str = date_str + '06:00'
                else:
                    date_str = date_str + '18:00'
        # parse raw date and format it
        date_str = may_remove_substring(date_str, ["'", '.', ' '])
        date_str = datetime.strptime(date_str, '%d-%b%H:%M')

    return date_str


def normalize_reported_date(raw_date):
    """Normalize raw reported date into an ISO8601-formatted string.
    This function WILL discard timezone data.

    Args:
        raw_date (str):
    Returns:
        str: ISO8601-formatted date string
    Examples:
        >>> normalize_reported_date('Fri, 5 Jul 2018 17:15:23 +0200')
        '2018-07-05T17:15:23'
        >>> normalize_reported_date('BMS Crude & Fuel')
    """
    date_match = re.match(r'.*:\d+(?!\+)', raw_date)
    return to_isoformat(date_match.group(0)) if date_match else None


def normalize_check_dates(item, dates):
    """Check if the year of each date is similar to the reported year.
    Args:
        dates : List of str
        item (Dict[str, str])

    Returns:
        Dict[str, str]:  item
    """

    reported_year = int(re.match(r'(\d+)', item.get('reported_date')).group(0))
    for key in dates:
        if item.get(key) is not None:
            date = item.get(key)
            if isinstance(date, datetime):
                # assume absolute difference of 1 year
                # example : reporting in December for vessel arrivals in next January
                if abs(date.year - reported_year) > 1:
                    date = date.replace(year=reported_year)
                    item[key] = date

    # We need to send reported date as a DD MMM YYYY string :
    date_match = re.match(r'.*:\d+(?!\+)', item.get('reported_date'))
    item['reported_date'] = parse_date(date_match.group(0)).strftime('%d %b %Y')
    return item


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


def normalize_port_name(port_str):
    """Cleanup port name.

        Args:
            port_str (str):

        Returns:
            str | None: string without undesired letters
    """
    if not port_str or port_str in STRING_BLACKLIST:
        return None

    for letter in LETTERS.keys():
        if re.compile(letter).search(port_str):
            port_str = port_str.replace(letter, LETTERS[letter])
    return port_str


def normalize_volume(raw_volume):
    """Split a raw cargo into multiple cargoes.
    Args:
        raw_volume (str):
    Returns:
        str|None
    """
    if not raw_volume or raw_volume in STRING_BLACKLIST:
        return None

    return may_remove_substring(raw_volume, [',', 'BLS'])


def process_exports_item(item):
    """Process export spot charters.

    Args:
        item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    return {
        'charterer': item['charterer'],
        'arrival_zone': [item['load_disch_zone']],
        'departure_zone': item['current_zone'],
        'lay_can_start': item['lay_can_start'],
        'lay_can_end': item['lay_can_end'],
        'provider_name': item['provider_name'],
        'reported_date': item['reported_date'],
        'vessel': {'name': item['vessel_name'], 'imo': item.get('vessel_imo', None)},
        'cargo': {
            'product': item['cargo_product'],
            'volume': item['cargo_volume'],
            'volume_unit': item['cargo_volume_unit'] if item['cargo_volume'] else None,
            'movement': item['cargo_movement'],
        },
    }


def process_imports_item(item):
    """Process import spot charters.

    Because of how spot charters are defined, we cannot use import items directly as a spot charter.
    We need to call the Kpler API to obtain the associated laycan dates with the given discharging
    dates, in order to construct a valid spot charter item.

    Args:
        item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    lay_can_start, lay_can_end = get_imports_lay_can_dates(item)
    return {
        'charterer': item['charterer'],
        'arrival_zone': [item['current_zone']],
        'departure_zone': item['load_disch_zone'],
        'lay_can_start': lay_can_start,
        'lay_can_end': lay_can_end,
        'provider_name': item['provider_name'],
        'reported_date': item['reported_date'],
        'vessel': {'name': item['vessel_name'], 'imo': item.get('vessel_imo')},
        'cargo': {
            'product': item['cargo_product'],
            'volume': item['cargo_volume'],
            'volume_unit': item['cargo_volume_unit'] if item['cargo_volume'] else None,
            'movement': item['cargo_movement'],
        },
    }


def get_imports_lay_can_dates(item):
    """Get laycan dates for import vessel movements.

        Call the API with origin parameter (to improve accuracy).
        If no known origin, call the API without the origin parameter.
        Match trades by checking that both destination date and installations are
        accurate to what's stipulated in the report (return the first one that matches).
        Finally, get the lay_can_start and lay_can_end from the final matched trade.

        Args:
            item (Dict[str, str]):

        Returns:
            Tuple[str | None, str | None]:

        """
    # get trade from different platforms
    if item['load_disch_zone'] and item['lay_can_start']:

        if isinstance(item['lay_can_start'], datetime):
            import_date = to_isoformat(item['lay_can_start'].strftime(KP_API_DATE_PARAM_FORMAT))
        else:
            import_date = to_isoformat(item['lay_can_start'])

        for platform in PLATFORMS:
            trade = kp_api.get_session(platform, recreate=True).get_import_trade(
                vessel=item['vessel_name'],
                origin=item['load_disch_zone'],
                dest=item['current_zone'],
                end_date=import_date,
            )
            if trade:
                break

        # obtain lay_can dates from trade
        if trade:
            # lay_can_start is 2 days before origin date,
            # lay_can_end is 1 day after origin date (c.f. analysts)
            lay_can = parse_date(trade['Date (origin)'], dayfirst=False)
            lay_can_start = (lay_can - dt.timedelta(days=2)).isoformat()
            lay_can_end = (lay_can + dt.timedelta(days=1)).isoformat()
        else:
            lay_can_start = None
            lay_can_end = None
    else:
        lay_can_start = None
        lay_can_end = None

    return lay_can_start, lay_can_end
