import datetime as dt
import logging

from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

TIMEZONE_DIFF = -6

IRRELEVANT_CARGOES = ['CONTENEDOR', 'CONVENCIONAL', 'OTROS', 'PASAJEROS']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Event type (arrival, departure, or eta) are determined by the comparing the current
    datetime(in the local timezone) to the item's `start_date`, `end_date`, and `eta`.
    First line is the timeline with item dates, second line is the event the item
    is mapped to according to where the current date falls (relative to time scrapped).

                  eta          start_date       end_date
    ---------------|---------------|---------------|---------------
    Event type:
    <-------------eta--------------><---arrival---><----departure-->

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, key_map())

    # discard vessel movements with irrelevant cargoes
    if not item.get('cargoes'):
        logger.info(f'Discarding vessel {item["vessel_name"]} with irrelevant cargo')
        return

    # build vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_length')}

    current_dt = dt.datetime.utcnow() - dt.timedelta(hours=TIMEZONE_DIFF)
    # port call 'start' is in the future
    if item['start_date'] > current_dt:
        portcall = {'eta': item['eta'].isoformat()}
    # vessel has already left the port at an earlier date
    elif item['end_date'] < current_dt:
        portcall = {'departure': item['end_date'].isoformat()}
    # vessel has not left the port, but it should have arrived (aka 'started')
    else:
        portcall = {'arrival': item['start_date'].isoformat()}

    portcall.update(
        cargoes=item['cargoes'],
        port_name=item['port_name'],
        provider_name=item['provider_name'],
        reported_date=item['reported_date'],
        vessel=item['vessel'],
    )

    return portcall


def key_map():
    return {
        # transfer static information - all are required as per PortCall model
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', normalize_reported_date),
        # row information map, in order
        'Numero AZ': ignore_key('index no, irrelevant'),
        'Nombre Nave': ('vessel_name', None),
        'Puesto': ignore_key('position on dock, irrelevant'),
        'Cliente': ignore_key('client, irrelevant'),
        'Estibador': ignore_key('stevedore, irrelevant'),
        'ETA': ('eta', convert_to_datetime),
        'Fecha de inicio': ('start_date', convert_to_datetime),
        'Fecha de finalización': ('end_date', convert_to_datetime),
        'Tiempo': ignore_key('time/weather, irrelevant'),  # translate to either "weather" or "time"
        'Eslora': ('vessel_length', lambda x: try_apply(x, float, int, str)),
        'Equipo a Utilizar': ignore_key('equipment, irrelevant'),
        'Modalidad': (
            'cargoes',
            lambda x: None if any(irr in x for irr in IRRELEVANT_CARGOES) else [{'product': x}],
        ),
        # not all rows have these information - all irrelevant for now
        'Cantidad Cont. Importación': ignore_key('irrelevant'),
        'Cantidad Cont. Exportación': ignore_key('irrelevant'),
        'Tonelaje Importación': ignore_key('irrelevant'),
        'Tonelaje Exportación': ignore_key('irrelevant'),
    }


def convert_to_datetime(raw_date):
    """Converts raw date strings into tz-aware datetime objects.

    Args:
        raw_date_str (str):

    Returns:
        datetime.datetime:

    """
    return dt.datetime.strptime(raw_date, '%d/%m/%Y %H:%M')


def normalize_reported_date(raw_date):
    """Extracts and converts a spanish week/year to a ISO-8601 reported datestring

    Examples:
        >>> normalize_reported_date(' Semana: 29 Año: 2018 Terminal: Todas\\r\\n    ')
        '2018-07-16T00:00:00'

    """
    week = raw_date.split('Semana: ')[1].split()[0]
    year = raw_date.split('Año: ')[1].split()[0]

    # Set the day of the week to be monday as default
    return dt.datetime.strptime('1' + week + year, "%w%W%Y").isoformat()
