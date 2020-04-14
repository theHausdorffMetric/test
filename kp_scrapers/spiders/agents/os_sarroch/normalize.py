from datetime import timedelta
from itertools import repeat, zip_longest
import logging

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_CARGO = ['technical call']

MOVEMENT_MAPPING = {'disch': 'discharge', 'disch.': 'discharge', 'load': 'load', 'load.': 'load'}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(reported=raw_item['reported_date']), skip_missing=True)
    if not item['vessel']:
        return

    if not item['cargo_product']:
        logger.info(f"Portcall by vessel {item['vessel']['name']} has no cargo info, discarding")
        return

    if not item['cargo_movement']:
        logger.info(f"Portcall by vessel {item['vessel']['name']} is for bunkering, discarding")
        return

    # according to analyst, if not date is presented, take reported date as berthed date
    if not item['eta'] and not item['berthed'] and not item['departure']:
        item['berthed'] = parse_date(item['reported_date']).isoformat()

    # build Cargo sub-model
    for cargo in normalize_cargoes(
        raw_product=item.pop('cargo_product'),
        raw_movement=item.pop('cargo_movement', None),
        raw_volume=item.pop('cargo_volume', ''),
        raw_player=item.pop('cargo_player', ''),
    ):
        item['cargo'] = cargo
        yield item


def field_mapping(**kwargs):
    return {
        '0': (ignore_key('empty field')),
        '1': ('vessel', lambda x: None if 'TBN' in x else {'name': x}),
        '2': ignore_key('berth terminal'),
        '3': ('eta', lambda x: normalize_date(x, **kwargs)),
        '4': ('berthed', lambda x: normalize_date(x, **kwargs)),
        '5': (ignore_key('status')),
        '6': ('departure', lambda x: normalize_date(x, **kwargs)),
        '7': (ignore_key('previous port')),
        '8': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x.strip().lower(), None)),
        '9': ('cargo_product', lambda x: None if x.lower() in IRRELEVANT_CARGO else x),
        '10': ('cargo_volume', lambda x: x.replace(',', '')),
        '11': ('cargo_player', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'port_name': ('port_name', None),
    }


def normalize_cargoes(raw_product, raw_movement, raw_volume, raw_player):
    """Normalize raw cargoes into a list of defined Cargo models.

    Args:
        raw_product (str):
        raw_movement (str):
        raw_volume (str):

    Yields:
        Dict[str, str]:

    """
    cargoes = zip_longest(
        raw_product.split('+'),  # products
        repeat(raw_movement, len(raw_product.split('+'))),  # movements
        raw_volume.split('+'),  # volumes
        repeat(Unit.tons, len(raw_product.split('+'))),  # volume units
    )
    if 'load' in raw_movement:
        player = 'seller'
    if 'discharge' in raw_movement:
        player = 'buyer'

    for cargo in cargoes:
        yield {
            'product': cargo[0],
            'movement': cargo[1],
            # sometimes the source will have a typo,
            # and will place product in both product and volume columns
            'volume': cargo[2] if cargo[2] and cargo[2].isdigit() else None,
            'volume_unit': cargo[3] if cargo[2] and cargo[2].isdigit() else None,
            player: {'name': raw_player} if raw_player else None,
        }


def normalize_date(raw_date, reported):
    """Normalize date with reported date as reference.

    The raw date comes with two formats:
        - 20/AM
        - 21/02:00

    It only contains day information, we'll have to fill in the month and year info referring
    reported date.

    There might be month rollover and year rollover situation.

    Examples:
        >>> normalize_date('20/AM', '2018-12-19T00:00:00')
        '2018-12-20T00:00:00'
        >>> normalize_date('14/EPM', '2019-02-12T00:00:00')
        '2019-02-14T00:00:00'
        >>> normalize_date('21/02:00', '2018-12-19T00:00:00')
        '2018-12-21T02:00:00'
        >>> normalize_date('01/23:00', '2018-1-31T00:00:00')
        '2018-02-01T23:00:00'
        >>> normalize_date('02/0600', '2018-12-28T00:00:00')
        '2019-01-02T06:00:00'

    Args:
        raw_date (str):
        reported (str):

    Returns:

    """
    if not raw_date:
        return

    reported = parse_date(reported)
    # remove `:` found in time (e.g 02:00)
    raw_date = raw_date.replace(':', '').upper()
    # remove mentions of vague time periods (i.e., AM/PM)
    if any(vague_time in raw_date.partition('/')[2] for vague_time in ['AM', 'PM']):
        raw_date = raw_date.split('/')[0]

    matching_date = parse_date(f'{reported.year}-{reported.month}-{raw_date}')
    # account for rollover scenarios
    # for example, matching date is `02/14:00` and reported date is `30 Jan`
    # 15 days is a safe threshold/tolerance for deciding if a date belongs to current month or not
    if matching_date - reported < timedelta(days=-15):
        matching_date += relativedelta(months=1)

    return matching_date.isoformat()
