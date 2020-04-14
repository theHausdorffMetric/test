import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_PRODUCTS = ['-', ' gc', 'container', 'cnt.', 'defence', 'g.c', 'units']

IRRELEVANT_VESSEL_TYPES = ['container', 'vehicle']

MOVEMENT_MAPPING = {'Exp Tons': 'load', 'Imp Tons': 'discharge'}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, portcall_mapping())

    # discard irrelevant vessel types and cargoes
    vessel_type = item.pop('vessel_type')
    if not vessel_type:
        logger.info(f'Vessel {raw_item["0"]} is of an irrelevant type, discarding')
        return

    # build cargo sub-model
    item['cargoes'] = []
    for movement in ('load', 'discharge'):
        product = item.pop(f'cargo_product_{movement}')
        volume = item.pop(f'cargo_volume_{movement}')
        if volume:
            # product of General Cargo itself is meaningless
            if vessel_type == 'General Cargo' and not product:
                continue

            item['cargoes'].append(
                {
                    'product': vessel_type if not product else f'{product} ({vessel_type})',
                    'movement': movement,
                    'volume': volume,
                    'volume_unit': Unit.tons,
                }
            )

    # don't return vessels with filtered-out cargoes
    if item['cargoes']:
        return item


def portcall_mapping():
    return {
        '0': ('vessel', lambda x: {'name': x}),
        '1': ignore_key('shipping agent'),
        '2': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        '3': ignore_key('time of vessel arrival, not required'),
        '4': (
            'cargo_volume_load',
            lambda x: x.replace(',', '') if x.lower() not in ('-', 'nil') else None,
        ),
        '5': (
            'cargo_product_load',
            lambda x: (
                None if not x or any(alias in x.lower() for alias in IRRELEVANT_PRODUCTS) else x
            ),
        ),
        '6': (
            'cargo_volume_discharge',
            lambda x: x.replace(',', '') if x.lower() not in ('-', 'nil') else None,
        ),
        '7': (
            'cargo_product_discharge',
            lambda x: (
                None if not x or any(alias in x.lower() for alias in IRRELEVANT_PRODUCTS) else x
            ),
        ),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x.partition(',')[2])),
        'vessel_type': (
            'vessel_type',
            lambda x: None if any(alias in x.lower() for alias in IRRELEVANT_VESSEL_TYPES) else x,
        ),
    }
