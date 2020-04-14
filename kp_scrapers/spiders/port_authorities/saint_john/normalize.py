from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


MOVEMENT_MAPPING = {'for': 'load', 'from': 'discharge'}

CARGO_BLACKLIST = ['CONTAINERS', 'CRUISE']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # discard vessels with blacklisted cargoes
    if not item.get('cargoes'):
        return

    return item


def portcall_mapping():
    return {
        'AGENT': ('shipping_agent', None),
        'BERTH': ('berth', None),
        'CARGO_ACTIVITY': ('cargoes', normalize_cargoes),
        'DATE_OF_ARRIVAL': ('arrival', lambda x: to_isoformat(x, dayfirst=False)),
        'ETA': ('eta', lambda x: to_isoformat(x, dayfirst=False)),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'SHIP_LINE': ignore_key('ship line'),
        'VESSEL_NAME': ('vessel', lambda x: {'name': x}),
    }


def normalize_cargoes(cargo_activity):
    """Normalize cargoes from cargo activity.

    The format is like below:
        <product name> from/for <foreign or domestic port>

    From: discharge
    For: load

    Examples:
        >>> normalize_cargoes('Petroleum for Foreign Ports')
        [{'product': 'Petroleum', 'movement': 'load'}]
        >>> normalize_cargoes('Petroleum from Foreign Ports')
        [{'product': 'Petroleum', 'movement': 'discharge'}]
        >>> normalize_cargoes('Visiting Cruise Ship')
        >>> normalize_cargoes('Containers to and from Foreign Ports')

    Args:
        cargo_activity:

    Returns:
        List[Dict]:

    """
    for key in MOVEMENT_MAPPING:
        if key in cargo_activity.split():
            product, movement, _ = cargo_activity.partition(key)

            # if blacklisted products are present, return immediately
            if any(alias in product.upper() for alias in CARGO_BLACKLIST):
                return None

            return [{'product': may_strip(product), 'movement': MOVEMENT_MAPPING.get(movement)}]
