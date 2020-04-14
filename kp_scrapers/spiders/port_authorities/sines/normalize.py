from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


CARGO_MOVEMENT_MAPPING = {'Embarque': 'load', 'Desembarque': 'discharge'}

VESSEL_TYPE_BLACKLIST = ['Contentores']

EVENT_MAPPING = {
    'Berthed ships': 'berthed',
    'Ships at anchor': 'arrival',
    'Ships in maneuvers': 'arrival',
    'Ships hovering': 'arrival',
    'Arrival forecasts': 'eta',
    'Output forecasts': 'departure',
}


# TODO ask analysts to fill in the rest of the mappings
INSTALLATION_MAPPING = {
    'Terminal de Gás Natural': 'Sines LNG Terminal',
    'Terminal de Granéis Líquidos': 'Sines Terminal de Graneis Liquidos',
    # 'Terminal Multipurpose': None,
    'Terminal Petroquímico': 'Sines Petrochemical Terminal',
    # 'Terminal XXI': None,
}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict:

    """
    item = map_keys(raw_item, portcall_mapping())

    # discard vessels of irrelevant types
    if any(_type in item.pop('vessel_type') for _type in VESSEL_TYPE_BLACKLIST):
        return

    # build Vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('vessel_imo')}

    # build proper portcall date, based on event description
    item[item.pop('event')] = item.pop('matching_date')

    return item


def portcall_mapping():
    return {
        'ARMADOR': ignore_key('armador ?'),
        'Agent': ('shipping_agent', may_strip),
        'Call': ignore_key('internal portcall number'),
        'Call sign': ignore_key('vessel callsign'),
        'cargoes': ('cargoes', lambda x: [map_keys(cargo, cargo_mapping()) for cargo in x]),
        'Commander': ignore_key('commander ?'),
        'Construction year': ignore_key('vessel build year'),
        'Countermark': ignore_key('countermark ?'),
        'Destiny': ignore_key('TODO use next zone to forecast future ETAs after this port'),
        'Flag': ignore_key('vessel flag'),
        'Forecast': ('matching_date', lambda x: to_isoformat(x, dayfirst=False)),
        'IMO': ('vessel_imo', may_strip),
        'Navigation type': ignore_key('navigation type ?'),
        'Nº travel': ignore_key('travel ?'),
        'Origin': ignore_key('previous port of call'),
        'Origin / Destiny': ignore_key('previous/next port of call'),
        'Port register': ignore_key('port register'),
        'port_name': ('port_name', None),
        'Protective agent': ignore_key('protective agent ?'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'Scale': ignore_key('scale ?'),
        'Ship name': ('vessel_name', may_strip),
        'Ship type': ('vessel_type', None),
        'Since': ('matching_date', lambda x: to_isoformat(x, dayfirst=False)),
        'Terminal': ('installation', lambda x: INSTALLATION_MAPPING.get(may_strip(x), x)),
        'Teus': ignore_key('TEUs; only relevant for container vessels'),
        'title': ('event', lambda x: EVENT_MAPPING.get(x)),
    }


def cargo_mapping():
    return {
        'Destiny': ignore_key('eventual discharge port of cargo'),
        'Merchandise': ('product', None),
        'Operation Type': ('movement', lambda x: CARGO_MOVEMENT_MAPPING.get(x)),
        'Origin': ignore_key('original loading port of cargo'),
    }
