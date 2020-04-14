import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

# source gives consistent vessel types
# easy win to map them to a proper product here, to save trouble on downstream processes
VESSEL_TYPE_PRODUCT_MAPPING = {
    'BULK CARRIERS': 'Dry Bulk',
    'CHEM. TANK': 'Liquids',
    'LNG TANKER': 'LNG',
    'LPG TANKER': 'LPG',
    'NGL TANKER': 'LNG',
    'PURE GEN.CARGO VESSELS': 'Dry Bulk',
    'TANKERS': 'Liquids',
}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, Any]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # discard vessels with irrelevant cargoes
    if not item.get('cargoes'):
        return

    return item


def portcall_mapping():
    return {
        'Anchor Date': ('arrival', lambda x: to_isoformat(dayfirst=True)),
        'Arrived From': ignore_key('previous port of call'),
        'Berth': ('berth', None),
        'Berth Date': ('berthed', to_isoformat),
        'Call Reason': ignore_key('reason for portcall'),
        'Cut-off date': ignore_key('TODO not sure; confirm with product owners'),
        'E.T.A.': ('eta', to_isoformat),
        'E.T.D.': ('departure', to_isoformat),
        'Plan Berth': ('berth', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'Rotation': ignore_key('internal portcall id'),
        'Sail To': ignore_key('TODO next_zone'),
        'Sailed Date': ('departure', to_isoformat),
        'Start/End Work Date': ignore_key('TODO not sure; confirm with product owners'),
        'Vessel Name': ('vessel', lambda x: {'name': x}),
        'Vessel Type': ('cargoes', map_vessel_type_to_product),
        'voyage': ignore_key('internal voyage id'),
        'agent': ('shipping_agent', None),
        'line': ignore_key('TODO not sure; confirm with product owners'),
        'rotationNumber': ignore_key('internal portcall id'),
    }


def map_vessel_type_to_product(raw_vessel_type):
    product = VESSEL_TYPE_PRODUCT_MAPPING.get(raw_vessel_type)
    if not product:
        logger.info('Irrelevant vessel type: %s', raw_vessel_type)

    return [{'product': product}] if product else None
