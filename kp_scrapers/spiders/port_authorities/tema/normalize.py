from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


CARGO_BLACKLIST = ['CONTAINERS', 'CONTS', 'DREDGING', 'FISH', 'FRUITS', 'VEHICLES']


def field_mapping():
    return {
        # common to all tables
        'AGENT': (ignore_key('not used in model')),
        'CARGO': ('cargoes', lambda x: [{'product': x}] if x else []),
        'NAME OF SHIP': ('vessel_name', None),
        'Name of the ship / voyage': ('vessel_name', None),
        'Type of Cargo': ('cargoes', lambda x: [{'product': x}] if x else []),
        'Type of Cargo ': ('cargoes', lambda x: [{'product': x}] if x else []),
        # static info
        'event': ('event', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        # for eta/arrival tables
        'DRAFT': (ignore_key('not used in model')),
        'ETA / ATA': ('matching_date', to_isoformat),
        'No': (ignore_key('not used in model')),
        # for berthed tables
        'ARR. TEMA': (ignore_key('not used in model')),
        'BOOKINGS': (ignore_key('not used in model')),
        'ETB/ATB': ('matching_date', to_isoformat),
        'ETD': (ignore_key('not used in model')),
        'L.O.A': ('vessel_length', lambda x: try_apply(x, float, int, str)),
        'SHIP STATUS': (ignore_key('not used in model')),
        'ST\'DORE': (ignore_key('not used in model')),
    }


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]: PortCall validated item

    """
    item = map_keys(raw_item, field_mapping())

    # discard items with empty vessels
    if not item['vessel_name']:
        return

    # build vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_length', None)}

    # discard items with irrelevant cargoes
    if any(cargo in (item['cargoes'] or [{}])[0].get('product', '') for cargo in CARGO_BLACKLIST):
        return

    # map matching_date to approriate event
    item[item['event']] = item.pop('matching_date', None)

    # remove rogue event field
    item.pop('event', None)

    return item
