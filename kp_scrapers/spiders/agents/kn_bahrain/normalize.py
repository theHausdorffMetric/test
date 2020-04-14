from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.agents.kn_bahrain.constant import PRODUCT_MAPPING


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # build vessel sub-model
    if not item['vessel_name'] or 'out of service' in item['vessel_name'].lower():
        return

    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'length': item.pop('length', None),
        'dwt': item.pop('dwt', None),
    }

    # build cargo sub-model, if multiple cargoes, yield two items
    for cargo in normalize_cargo(item.pop('products', '')):
        item.update(cargo=cargo)
        yield item


def field_mapping():
    return {
        'Berth': (ignore_key('berth')),
        'Vessel Name': ('vessel_name', lambda x: None if 'TBN' in x.split() else x),
        'Flag': (ignore_key('ignore flag')),
        'Agent': (ignore_key('agent')),
        'Cargo Inspection': (ignore_key('cargo inspection')),
        'Destination(s)': (ignore_key('destination')),
        'Order Number(s)': (ignore_key('order number')),
        'Product Grade(s)': ('products', None),
        'Product Grade': ('products', None),
        'Loading/Discharge\nStart': ('berthed', lambda x: to_isoformat(x, dayfirst=True)),
        'Loading/Discharge\nFinished (Est.)': (
            'departure',
            lambda x: to_isoformat(x, dayfirst=True),
        ),
        'Anchored Time': ('arrival', lambda x: to_isoformat(x, dayfirst=True)),
        'LOA': ('length', None),
        'Dead Weight': ('dwt', None),
        'ETA': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', to_isoformat),
        'port_name': ('port_name', None),
    }


def normalize_cargo(products):
    """Normalize cargo with given products.

    Args:
        products:

    Yields:
        List[Dict[str, str]]:

    """
    for product in products.split('/'):
        yield {'product': PRODUCT_MAPPING.get(product.strip(), product.strip()), 'movement': 'load'}
