import datetime as dt
import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

# maximum number of days before we consider a departure to be too old
MAX_DEPARTURE_AGE = 21

IRRELEVANT_PORT_NAME = ['-', 'por definir']

# FIXME should be added as zone aliases instead of a hardcoded mapping,
# but it's an easy win to add them here for now, since the source is consistent with the names
ZONE_MAPPING = {
    'Belgica': 'Belgium',
    'Brasil': 'Brazil',
    'Canadá': 'Canada',
    'Corea del Sur': 'South Korea',
    'Egipto': 'Egypt',
    'España': 'Spain',
    'Francia': 'France',
    'Gate': 'Rotterdam',
    'GATE': 'Rotterdam',
    'Holanda': 'Netherlands',
    'Inglaterra': 'United Kingdom',
    'Italia': 'Italy',
    'Japon': 'Japan',
    'Japón': 'Japan',
    'México': 'Mexico',
    'Reino Unido': 'United Kingdom',
    'Tailandia': 'Thailand',
    'Taiwán': 'Taiwan',
}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, portcall_mapping())

    # discard vessels with departures that are too old
    # NOTE website shows complete historical data since commencement of port operations
    # we only want the most recent data
    if (dt.datetime.utcnow() - item['departure']).days >= MAX_DEPARTURE_AGE:
        return

    item['departure'] = item['departure'].isoformat()

    # build Cargo sub-model
    # NOTE website shows only LNG vessel schedules
    item['cargoes'] = [
        {
            'product': 'LNG',
            'volume': item.pop('cargo_volume_cubic'),
            'volume_unit': Unit.cubic_meter,
        }
    ]

    return item


def portcall_mapping():
    return {
        '0': ignore_key('table row index'),
        '1': ignore_key('internal portcall number'),
        '2': ('departure', lambda x: parse_date(x, dayfirst=True)),
        '3': ('vessel', lambda x: {'name': x}),
        '4': ('next_zone', normalize_next_zone),
        '5': ('cargo_volume_cubic', lambda x: x.replace(',', '').split('.')[0]),
        '6': ignore_key('cargo volume in tons'),
        '7': ignore_key('caloric content of cargo'),
        '8': ignore_key('MPC ? to confirm with analysts'),
        '10': ignore_key('pricing/delivery point'),
        '11': ignore_key('marker value'),
        '12': ignore_key('reference value'),
        '13': ignore_key('price use'),
        '14': ignore_key('unit regalia'),
        '15': ignore_key('state'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_next_zone(raw_zone):
    """Normalize a raw zone name into something recognisable.

    The raw zone as provided by the source comes in the following format:
    <port_name>, <country_name>

    Occasionally, the port_name will not be known by the source,
    and be listed by an invalid string.

    This function will prioritise taking the port_name as the zone,
    but if it is invalid it will take the country_name instead.

    Args:
        raw_zone (str):

    Returns:
        str: normalized zone name

    Examples:
        >>> normalize_next_zone('-, Corea del Sur')
        'South Korea'
        >>> normalize_next_zone('FOS-SUR-MER (TONKIN), Francia')
        'FOS-SUR-MER (TONKIN)'
        >>> normalize_next_zone('-, Malasia')
        'Malasia'
        >>> normalize_next_zone('Gate, Holanda')
        'Rotterdam'

    """
    _port, _, _country = [may_strip(x) for x in raw_zone.partition(',')]
    zone_name = _country if _port.lower() in IRRELEVANT_PORT_NAME else _port
    return ZONE_MAPPING.get(zone_name, zone_name)
