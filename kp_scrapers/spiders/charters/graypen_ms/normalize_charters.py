import datetime as dt
import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.services import kp_api
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_PATTERN = ['CNR', 'TBN']


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable SpotCharter event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:
    """
    item = map_keys(raw_item, charter_mapping())
    # discard vessels if it's yet to be named (TBN)
    if not item['vessel']:
        return

    # process "import" portcalls accordingly (charters are defined by their departure)
    if 'import' in item.pop('cargo_movement', '').lower():
        logger.info(f'Attempting to find matching export port call for {item["vessel"]["name"]}')
        # get trade from oil platform
        _trade = kp_api.get_session('oil', recreate=True).get_import_trade(
            vessel=item['vessel']['name'],
            origin='',
            dest=item['departure_zone'],
            end_date=item['lay_can_start'],
        )
        # mutate item with relevant laycan periods and load/discharge port info
        _process_import_charter(item, _trade)

    return item


def charter_mapping():
    return {
        'Cargo': ('cargo_movement', None),
        'Cargo No': ('cargo_movement', None),
        'Cargo No.': ('cargo_movement', None),
        'Charterer': ('charterer', lambda x: None if x in IRRELEVANT_PATTERN else x),
        'Charterers': ('charterer', lambda x: None if x in ['CNR', 'TBN'] else x),
        'Date': ('lay_can_start', lambda x: to_isoformat(x, dayfirst=True)),
        'Dates': ('lay_can_start', lambda x: to_isoformat(x, dayfirst=True)),
        'Grade': ignore_key('to be extracted in grades spider'),
        'Next Port': (
            'arrival_zone',
            lambda x: None if x in IRRELEVANT_PATTERN else [x.replace(',', '')],
        ),
        'Notes': ignore_key('remarks'),
        'provider_name': ('provider_name', None),
        'port_name': ('departure_zone', None),
        'QTY': ignore_key('to be extracted in grades spider'),
        'Quantity': ignore_key('to be extracted in grades spider'),
        'reported_date': (
            'reported_date',
            # date is passed as ISO8601 string from spider module
            lambda x: parse_date(x, dayfirst=False).strftime('%d %b %Y'),
        ),
        'Shipper': ignore_key('irrelevant'),
        'Shipper/Receiver': ignore_key('irrelevant'),
        'Shipper/Receivers': ignore_key('irrelevant'),
        'Supp/Rcvr': ignore_key('irrelevant'),
        'Supplier': ignore_key('irrelevant'),
        'Vessel': ('vessel', lambda x: {'name': x} if 'TBN' not in x else None),
    }


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
