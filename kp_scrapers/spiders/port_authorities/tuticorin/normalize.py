import re

from kp_scrapers.lib.date import (
    create_str_from_time,
    ISO8601_FORMAT,
    may_parse_date_str,
    to_isoformat,
)
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


PRODUCT_BLACKLIST = ['CONTAINER', 'BALLAST']

MOVEMENT_MAPPING = {'IMPORT': ['discharge'], 'EXPORT': ['load'], 'BOTH': ['discharge', 'load']}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # build vessel sub model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'length': item.pop('vessel_loa', None),
        'beam': item.pop('vessel_beam', None),
    }

    # build cargo sub model
    item['cargoes'] = list(
        normalize_cargo(
            item.pop('movement', None), item.pop('volume', None), item.pop('product', None)
        )
    )

    if not item['cargoes']:
        return

    # if there's eta time
    if item.get('eta_time'):
        eta_time = item.pop('eta_time')
        hour, minute = eta_time.split(':')
        item['eta'] = create_str_from_time(
            may_parse_date_str(item['eta'], ISO8601_FORMAT).replace(
                hour=int(hour), minute=int(minute)
            ),
            ISO8601_FORMAT,
        )

    # berth
    berth_from = item.pop('berth_from', None)
    berth_to = item.pop('berth_to', None)
    item['berth'] = berth_from or berth_to or item.get('berth', None)

    return item


def portcall_mapping():
    return {
        # vessels waiting
        'SL NO': (ignore_key('equivalent to serial number')),
        'VESSEL NAME': ('vessel_name', normalize_vessel_name),
        'LOA': ('vessel_loa', lambda x: try_apply(x, float, int)),
        'BEAM': ('vessel_beam', lambda x: try_apply(x, float, int)),
        'DRAFT': (ignore_key('vessel draft')),
        'GEAR': (ignore_key('unknown')),
        'I/E': ('movement', None),
        'QUANTITY': ('volume', None),
        'CARGO': ('product', None),
        'ANCHORAGE DATE': ('arrival', lambda x: to_isoformat(x, dayfirst=True)),
        'READINESS DATE': ('berthed', lambda x: to_isoformat(x, dayfirst=True)),
        'REQ.BERTH': ('berth', None),
        'AGENT NAME': ('shipping_agent', None),
        'STV.AGENT': (ignore_key('unknown agent')),
        'DELAY REASONS': (ignore_key('ignore remarks')),
        # vessels expected specific
        'EXPECTED DATE': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'BERTH': ('berth', None),
        'REMARKS': (ignore_key('ignore remarks')),
        # shipping movements specific
        'DATE': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'IMP/EXP': ('movement', None),
        'TONNAGE': ('volume', None),
        'PURPOSE': (ignore_key('unknown')),
        'BERTH (FROM)': ('berth_from', None),
        'BERTH (TO)': ('berth_to', None),
        'TIME': ('eta_time', None),
        # meta fields
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x.split(':')[-1], dayfirst=True)),
    }


def normalize_vessel_name(raw_name):
    """Normalize vessel name.

    Examples:
        >>> normalize_vessel_name('MV ZEALAND AMSTERDAM-(0020)')
        'ZEALAND AMSTERDAM'
        >>> normalize_vessel_name('MT BRIGHT WORLD/0015')
        'BRIGHT WORLD'
        >>> normalize_vessel_name('ZEALAND AMSTERDAM (0020)')
        'ZEALAND AMSTERDAM'

    Args:
        raw_name:

    Returns:
        str:

    """
    _match = re.match(r'(M[TV] )?([\w\s]*)([\W\d]*)?', raw_name)
    if _match:
        _, vessel_name, _ = _match.groups()
        return vessel_name.strip()


def normalize_cargo(movement, volume, product):
    """Normalize cargo with given information.

    Args:
        movement: Import / Export / Both
        volume: in cubic meters
        product: product name

    Yields:
        Dict:

    """
    for m in MOVEMENT_MAPPING[movement]:
        if not product or product in PRODUCT_BLACKLIST:
            return

        volume = ''.join(re.findall(r'\d*', volume))
        if volume == '0':
            return

        yield {'product': product, 'movement': m, 'volume': volume, 'volume_unit': Unit.tons}
