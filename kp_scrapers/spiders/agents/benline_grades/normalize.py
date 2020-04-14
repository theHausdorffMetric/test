import logging

from kp_scrapers.lib.date import is_isoformat, to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


BLACKLIST = ['tba', 'rvtg', 'nil']

MOVEMENT_MAPPING = {
    'dshg': 'discharge',
    'disch': 'discharge',
    'disc': 'discharge',
    'dischsrge': 'discharge',
}


@validate_item(CargoMovement, normalize=True, strict=True, log_level='error')
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]: normalized cargo movement item

    """
    item = map_keys(raw_item, field_mapping())

    # discard vessels that are unknown/irrelevant
    if not item['vessel'].get('name') or item['vessel'].get('name') in BLACKLIST:
        return

    product = item.pop('cargo_product', None)
    # for all TBA cargo movements to be coal in coal file
    if product == 'TBA COAL':
        product = 'Coal'
    # force all cargo movements to be iron ore if it is an iron file
    if item.get('file_name'):
        if 'iron' in item['file_name'].lower():
            product = 'Iron Ore'

    volume = item.pop('cargo_volume', None)
    # sometimes the volume and product fields may be swapped in the source file
    if volume and volume[0].isalpha():
        product, volume = volume, product
        volume = may_strip(volume.lower().replace(',', '').replace('mt', ''))

    # sanity check again, discard if no product extracted
    if not product:
        logger.warning("No product extracted in cargo movement, skipping")
        return

    seller = None
    buyer = None
    # build proper Cargo model
    if item.get('cargo_seller'):
        seller = item['cargo_seller']
    if item.get('cargo_buyer'):
        buyer = item['cargo_buyer']

    if item.get('cargo_player'):
        if item['cargo_movement'] == 'load':
            seller = item['cargo_player']
        if item['cargo_movement'] == 'discharge':
            buyer = item['cargo_player']

    if item.get('cargo_movement'):
        movement = item.pop('cargo_movement', None)
    else:
        movement = 'load'

    item['cargo'] = {
        'product': product,
        'movement': may_strip(MOVEMENT_MAPPING.get(movement, None)),
        'volume': try_apply(volume, float, int, str),
        'volume_unit': Unit.tons if volume else None,
        'seller': {'name': seller} if seller else None,
        'buyer': {'name': buyer} if buyer else None,
    }
    for col in ('file_name', 'cargo_seller', 'cargo_buyer', 'cargo_player', 'cargo_movement'):
        item.pop(col, None)

    if (
        not item.get('arrival')
        and not item.get('berthed')
        and not item.get('departure')
        and not item.get('eta')
    ):
        return

    if item.get('cargo').get('volume') == '0':
        item['cargo']['volume'] = None
        item['cargo']['volume_unit'] = None

    return item


def field_mapping():
    # declarative mapping for ease of developement/maintenance
    return {
        'vessel': ('vessel', lambda x: {'name': may_strip(x)}),
        'eta': ('eta', lambda x: normalize_dates(x)),
        'eta\n(dd-mm-yy)': ('eta', lambda x: normalize_dates(x)),
        'etb': ('berthed', lambda x: normalize_dates(x)),
        'etd': ('departure', lambda x: normalize_dates(x)),
        'days delay': ignore_key('days delay'),
        'destination': ignore_key('destination'),
        'volume': (
            'cargo_volume',
            lambda x: may_strip(x.replace(',', ''))
            if x and may_strip(x.lower()) not in BLACKLIST
            else None,
        ),
        'vol (mt)': (
            'cargo_volume',
            lambda x: may_strip(x.replace(',', ''))
            if x and may_strip(x.lower()) not in BLACKLIST
            else None,
        ),
        'shipper': ('cargo_seller', lambda x: check_blacklist(may_strip(x))),
        'cargo': ('cargo_product', lambda x: may_strip(x)),
        'type': ('cargo_product', lambda x: f'{may_strip(x)} COAL'),  # coal products only
        'type of coal': ('cargo_product', lambda x: f'{may_strip(x)} COAL'),  # coal products only
        'country': ignore_key('country'),
        'country of origin/destination': ignore_key('country of origin/destination'),
        'facility/terminal': ignore_key('facility/terminal'),
        'port': ('port_name', may_strip),
        'volume/mt': (
            'cargo_volume',
            lambda x: may_strip(x.replace(',', ''))
            if x and may_strip(x.lower()) not in BLACKLIST
            else None,
        ),
        'load/discharge': ('cargo_movement', lambda x: x.lower()),
        'shipper/ receiver': ('cargo_player', lambda x: check_blacklist(may_strip(x))),
        'receiver': ('cargo_buyer', lambda x: check_blacklist(may_strip(x))),
        'port_name': ('port_name', None),
        'installation': ('installation', None),
        'file_name': ('file_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def check_blacklist(raw):
    """ Normalize player strings
    Args:
        raw (str):

    Returns:
        str:

    Examples:
        >>> check_blacklist('TBA')
        >>> check_blacklist('ABC - AT PORT')
        'ABC'
        >>> check_blacklist('ABC - 12/10/2019')
        'ABC'
        >>> check_blacklist('(ABC) - 12/10/2019')
        'ABC'
        >>> check_blacklist('A-B-C - 12/10/2019')
        'A-B-C'
        >>> check_blacklist('A-B-C')
        'A-B-C'
    """
    if any(s in raw.lower() for s in BLACKLIST):
        return None

    if ' - ' in raw:
        return may_strip(raw.rpartition(' - ')[0].replace('(', '').replace(')', ''))

    return raw


def normalize_dates(raw_date):
    """ Normalize dates
    Args:
        raw_date (str):

    Returns:
        str:

    Examples:
        >>> normalize_dates('2019-10-10T00:00:00')
        '2019-10-10T00:00:00'
        >>> normalize_dates('11/01/2019')
        '2019-01-11T00:00:00'
        >>> normalize_dates('TBA')
    """
    if is_isoformat(raw_date):
        return raw_date

    try:
        return to_isoformat(raw_date, dayfirst=True)
    except Exception:
        return None
