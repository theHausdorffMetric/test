import datetime as dt
import html
import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


IRRELEVANT_PRODUCTS = [
    'Aé',
    'Biens non classé',
    'Conteneurs',
    'Machine',
    'Matériel',
    'Moteurs',
    'Tô' 'Vé',
]

IRRELEVANT_SHIPPING_AGENTS = [
    'CMA CGM',  # container shipping
    "COMMANDANT",  # no ship agent
    'CROISIEUROPE',  # river cruise
    'DRAGAGE TRANSPORT TRAVAUX MARITIMES',  # dredging
]

MOVEMENT_MAPPING = {'C': 'load', 'D': 'discharge'}

INSTALLATION_MAPPING = {
    'MONTOIR-DE-BRETAGNE': 'Montoir',
    'SAINT-NAZAIRE': None,
    'SITES AMONT': 'Donges',
}

MAX_TIME_DIFF = 60


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw data into Portcall event

    Args:
        Dict[str, str]

    Return:
        Dict[str, Any]
    """

    item = map_keys(raw_item, portcall_mapping())

    # use shipping agent as a proxy for determining which vessels are irrelevant for our interests
    # generally everything related to containers and other non-merchant vessels are irrelevant
    if item.get('shipping_agent') in IRRELEVANT_SHIPPING_AGENTS:
        logger.info('Discarding portcall since it is handled by an irrelevant shipping agent')
        return

    # compute new-ness/old-ness of current portcall
    if item.get('eta'):
        time_diff = (dt.datetime.utcnow() - parse_date(item['eta'], dayfirst=False)).days
    elif item.get('arrival'):
        time_diff = (dt.datetime.utcnow() - parse_date(item['arrival'], dayfirst=False)).days
    else:
        logger.error('Item does not contain a portcall date: %s', raw_item)
        return

    # we don't want forecast of future portcalls that are too old/new,
    # since it is quite likely they are not updated on the website and are incorrect
    if abs(time_diff) > MAX_TIME_DIFF:
        logger.info('Discarding portcall since it is too old/new')
        return

    # check if cargo data is present and useful, else do not yield item
    # note that cargo data is only present on arrival source page
    if item.pop('event') == 'arrival':
        cargo = do_process_cargo(item)
        if not cargo:
            return

        # append cargo data if it is relevant
        item['cargoes'] = cargo

    # build Vessel sub model
    item['vessel'] = {
        'name': item.pop('vessel_name', None),
        'length': item.pop('vessel_length', None),
    }

    return item


def portcall_mapping():
    return {
        'Agent': ('shipping_agent', None),
        'Berth': ('berth', None),
        'Cargo': ('cargo_product', None),
        'Docking': ('arrival', lambda x: to_isoformat(x, dayfirst=True)),
        'ETA *': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'ETD *': ignore_key('estimated time of departure; not used downstream for now'),
        'event': ('event', None),
        'Facility': ('installation', lambda x: INSTALLATION_MAPPING.get(x)),
        'I/M/O *': ignore_key('inbound/outbound/mooring movement; no cargo provided here'),
        'Length of Vessel': ('vessel_length', lambda x: try_apply(x, float, int)),
        'Loading': ('cargo_movement', None),
        'Name of Vessel': ('vessel_name', None),
        'Tonnage': ('cargo_volume', None),
        'Vessel': ('vessel_name', None),
        'provider_name': ('provider_name', None),
        'port_name': ('port_name', None),
        'reported_date': ('reported_date', None),
    }


def is_irrelevant_value(value):
    return value is None or value.strip() in ['', '-']


def do_process_cargo(item):
    if is_irrelevant_value(item.get('cargo_product')):
        logger.info('Portcall has no cargo data')
        return

    product = html.unescape(item.pop('cargo_product')).partition('-')[2].strip()
    movement = MOVEMENT_MAPPING.get(item.pop('cargo_movement', None))
    volume = try_apply(item.pop('cargo_volume', None), int)

    # discard events if it shows an arrived vessel, but with no cargo or irrelevant cargo
    if any(alias in product for alias in IRRELEVANT_PRODUCTS):
        logger.info('Portcall has irrelevant cargo')
        return

    return [{'product': product, 'movement': movement, 'volume': volume, 'volume_unit': Unit.tons}]
