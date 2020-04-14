import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


IRRELEVANT_CARGOES = [
    'barge',
    'container',
    'contianer',
    'cruise',
    'dredger',
    'icgs',
    'navy',
    'passenger',
    'tug',
]

MOVEMENT_MAPPING = {'I': 'discharge', 'E': 'load'}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, field_mapping())

    # build cargoes sub-model
    item['cargoes'] = list(normalize_cargoes(item))
    if not item['cargoes']:
        return

    # build proper ETA
    item['eta'] = normalize_date(item.pop('eta_date'), item.pop('eta_time'))

    # discard irrelevant fields
    item.pop('cargo_product')
    item.pop('cargo_movement')
    item.pop('cargo_volume')

    return item


def field_mapping():
    return {
        'DATE': ('eta_date', may_strip),
        'ATA/ ETA': ('eta_time', lambda x: x if x else ''),
        'VESSEL': ('vessel', lambda x: {'name': x}),
        'CARGO': ('cargo_product', lambda x: x if x else 'container'),
        'QUANTITY (MT/TEU)': ('cargo_volume', may_strip),
        'AGENT': ('shipping_agent', None),
        'I/E': ('cargo_movement', may_strip),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', to_isoformat),
    }


def normalize_cargoes(item):
    # filter out irrelevant cargoes
    if not is_relevant_cargo(item['cargo_product'], item['cargo_movement']):
        return

    # multi cargoes, ignore volume to avoid confusion
    elif '+' in item['cargo_product']:
        for product in [may_strip(prod) for prod in item['cargo_product'].split('+')]:
            yield {'product': product}

    # single cargo, assign volume and movement
    else:
        yield {
            'product': item['cargo_product'],
            'movement': MOVEMENT_MAPPING.get(item['cargo_movement']),
            'volume': item['cargo_volume'],
            'volume_unit': Unit.tons,
        }


def is_relevant_cargo(product, movement):
    return movement in ['I', 'E'] and not any(
        alias in product.lower() for alias in IRRELEVANT_CARGOES
    )


def normalize_date(r_date, r_time):
    try:
        f_datetime = to_isoformat(f'{r_date} {r_time}')
    except Exception:
        logger.error('Invalid datetime %s %s', r_date, r_time)
        return None

    return f_datetime
