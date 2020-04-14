import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_PATTERN = ['CNR', 'TBN']


MOVEMENT_MAPPING = {'loading': 'load', 'discharge': 'discharge'}

PORT_MAPPING = {'Butinge SPM': 'Butinge', 'BSPM': 'Butinge'}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, grades_mapping())

    # extract relevant months
    if item['month'] not in extract_relevant_month(item['reported_date']):
        return

    # remove vessels not named
    if 'TBN' in item['vessel']['name']:
        return

    # build Cargo sub-model
    item['cargo'] = {
        'product': item.pop('product', None),
        'volume': item.pop('volume', None),
        'volume_unit': Unit.kilotons,
        'movement': item.pop('movement', None),
    }

    item.pop('month')

    return item


def grades_mapping():
    return {
        'TERMINAL': ignore_key('TERMINAL'),
        'MONTH': ('month', None),
        'POSITION': ignore_key('Position'),
        'VESSEL\'S NAME': ('vessel', lambda x: {'name': x}),
        'VESSEL': ('vessel', lambda x: {'name': x}),
        'OPERATION': ('movement', lambda x: MOVEMENT_MAPPING.get(x, x)),
        'CARGO': ('product', None),
        'Q-TY (KT)': ('volume', None),
        'QUANTITY KT': ('volume', None),
        'SHIPPER': ignore_key('Shipper'),
        'CHARTERER': ignore_key('Charterer'),
        'ARRIVAL': ('arrival', None),
        'ARRIVAL': ('arrival', None),
        'BERTHED': ('berthed', None),
        'BERTHING': ('berthed', None),
        'SAILED': ('departure', None),
        'SAILING': ('departure', None),
        'STTAUS': ignore_key('Status'),
        'DESTINATION': ignore_key('Destination'),
        'PORT': ('port_name', lambda x: PORT_MAPPING.get(x, x)),
        'reported_date': ('reported_date', lambda x: normalize_reported_date(x)),
        'provider_name': ('provider_name', None),
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
        '2018-07-05T17:15:23'
        >>> normalize_reported_date('BMS Crude & Fuel')
    """

    date_match = re.match(r'.*:\d+(?!\+)', raw_date)
    return to_isoformat(date_match.group(0), dayfirst=True) if date_match else None


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
