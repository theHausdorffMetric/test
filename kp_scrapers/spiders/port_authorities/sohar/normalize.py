import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

PRODUCT_MAPPING = {'BULK': 'Dry Bulk', 'TANK': 'Liquids'}

RELEVANT_EVENTS = ['eta']


@validate_item(PortCall, normalize=True, strict=True, log_level='error')
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, portcall_mapping())

    # discard vessel movements describing irrelevant events
    if not any(ev in item for ev in RELEVANT_EVENTS):
        logger.info('Vessel %s has irrelevant portcall event, skipping', item['vessel']['name'])
        return

    # discard vessels with no mapped cargoes
    product = item.pop('cargo_product', None)
    if not PRODUCT_MAPPING.get(product):
        logger.info('Vessel %s has unmapped cargo, skipping: %s', item['vessel']['name'], product)
        return

    # build Cargo sub-model
    item['cargoes'] = [{'product': PRODUCT_MAPPING.get(product)}]

    return item


def portcall_mapping():
    return {
        'Date / Time AET (Actual End Time)': ('arrival', lambda x: to_isoformat(x, dayfirst=False)),
        'Date / Time ETA (Estimated Time of Arrival)': (
            'eta',
            lambda x: to_isoformat(x, dayfirst=False),
        ),
        'Date / Time ATD (Estimated Time of Departure)': (
            'departure',
            lambda x: to_isoformat(x, dayfirst=False),
        ),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'Vessel name': ('vessel', lambda x: {'name': x}),
        'Vessel Type': ('cargo_product', None),
    }
