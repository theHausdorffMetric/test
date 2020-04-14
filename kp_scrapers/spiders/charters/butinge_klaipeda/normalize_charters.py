import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.services import kp_api
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_PATTERN = ['CNR', 'TBN']


MOVEMENT_MAPPING = {'loading': 'load', 'discharge': 'discharge'}

PORT_MAPPING = {'Butinge SPM': 'Butinge', 'BSPM': 'Butinge'}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, charters_mapping())

    # extract relevant months, there is a risk of scraping January again for current year and the
    # next year
    if item['month'] not in extract_relevant_month(item['reported_date']):
        return

    # remove vessels not named
    if 'TBN' in item['vessel']['name']:
        return

    # build a proper cargo dict according to Cargo model
    item['cargo'] = {
        'product': item.pop('product', None),
        'volume': item.pop('volume', None),
        'volume_unit': Unit.kilotons,
        'movement': 'load',
    }

    if 'discharge' in item['is_export']:
        _trade = None
        if item['lay_can_start']:
            # get trade from either oil or cpp platform
            for platform in ('oil', 'cpp'):
                _trade = kp_api.get_session(platform, recreate=True).get_import_trade(
                    vessel=item['vessel']['name'],
                    origin=item.get('previous_port', ''),
                    dest=item['departure_zone'],
                    end_date=item['lay_can_start'],
                )
                if _trade:
                    break

        # mutate item with relevant laycan periods and load/discharge port info
        post_process_import_item(item, _trade)

    if 'load' in item['is_export'] and 'Klaipeda' in (item['sheet_name'] and item['arrival_zone']):
        # data can be incorrect in klaipeda sheet, arrival is the same as departure
        # when is_export = load, replace such instances with a blank
        item['arrival_zone'] = ''

    if item['spider_name'] == 'UN_KlaipedaButinge_Fixtures_OIL':
        item.pop('cargo', None)

    for field in ('is_export', 'sheet_name', 'spider_name', 'previous_port', 'month'):
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
        'TERMINAL': ignore_key('Terminal'),
        'MONTH': ('month', None),
        'POSITION': ignore_key('Position'),
        'VESSEL\'S NAME': ('vessel', lambda x: {'name': x}),
        'VESSEL': ('vessel', lambda x: {'name': x}),
        'OPERATION': ('is_export', lambda x: MOVEMENT_MAPPING.get(x, x)),
        'CARGO': ('product', None),
        'Q-TY (KT)': ('volume', None),
        'QUANTITY KT': ('volume', None),
        'SHIPPER': ignore_key('Shipper'),
        'CHARTERER': ('charterer', lambda x: None if x in IRRELEVANT_PATTERN else x),
        'ARRIVAL': ignore_key('Arrival'),
        'BERTHED': ('lay_can_start', None),
        'BERTHING': ('lay_can_start', None),
        'SAILED': ignore_key('Sailed'),
        'STATUS': ignore_key('Status'),
        'DESTINATION': (
            'arrival_zone',
            lambda x: None if x in IRRELEVANT_PATTERN else [x.replace(',', '')],
        ),
        'ARRIVED FROM': ('previous_port', None),
        'SAILED TO': (
            'arrival_zone',
            lambda x: None if x in IRRELEVANT_PATTERN else [x.replace(',', '')],
        ),
        'PORT': ('departure_zone', lambda x: PORT_MAPPING.get(x, x)),
        'reported_date': ('reported_date', lambda x: normalize_reported_date(x)),
        'provider_name': ('provider_name', None),
        'sheet_name': ('sheet_name', None),
        'spider_name': ('spider_name', None),
    }


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


def extract_relevant_month(raw_reported_date):
    """Since report is cumulative, scrape current month and previous
    to prevent excessive scraping

    Args:
        raw_reported_date (str):

    Returns:
        List[str, str]:

    Examples:
        >>> extract_relevant_month('05 Jul 2018')
        ['July', 'June', 'August']
    """
    current_month = parse_date(raw_reported_date, dayfirst=True).strftime('%B')
    previous_month = (
        parse_date(raw_reported_date, dayfirst=True) - relativedelta(months=1)
    ).strftime('%B')
    next_month = (parse_date(raw_reported_date, dayfirst=True) + relativedelta(months=1)).strftime(
        '%B'
    )

    return [current_month, previous_month, next_month]
