from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


# products that are to be excluded from the source
INVALID_PRODUCTS = ['CONTAINER', 'N/A']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw data into Portcall event

    Args:
        Dict[str, str]

    Return:
        Dict[str, Any]
    """

    item = map_keys(raw_item, field_mapping())

    # build vessel sub model
    item['vessel'] = {'name': item.pop('vessel_name', None), 'length': item.pop('length', None)}

    return item


def field_mapping():
    return {
        'reported_date': ('reported_date', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'Vessel Name': ('vessel_name', None),
        'Expected To Arrival': ('arrival', lambda x: to_isoformat(x, dayfirst=True)),
        'SCN NO': ignore_key('SCN NO'),
        'LOA': ('length', None),
        'Voyage No': ignore_key('Voyage No'),
        'Shipping Agent': ignore_key('Shipping Agent'),
        'Cargo Detail': (
            'cargoes',
            lambda x: [{'product': x} for x in x.split(',') if x and x not in INVALID_PRODUCTS],
        ),
    }
