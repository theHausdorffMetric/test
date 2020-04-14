import datetime as dt  # noqa
import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_apply, may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

INVALID_CARGOES = ['CONTÊINERES']


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.
        REQUIRES ALL of the following fields:
        - port_name
        - provider_name (as defined in `BaseEvent`)
        - reported_date
        - vessel

    REQUIRES AT LEAST ONE of the following fields:
        - arrival
        - berthed
        - departure
        - eta

    Optional fields:
        - cargoes OR cargo
        - installation
        - next_zone
        - berth
        - shipping_agent

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping(reported_date=parse_date(raw_item['reported_date'])))
    if not (item.get('eta') or item.get('etb') or item.get('departure')):
        logger.info(f'Item contains no valid dates: {raw_item}')
        return None
    item['vessel']['imo'] = item.pop('imo', None)
    item['vessel']['dead_weight'] = item.pop('dead_weight', None)
    item['vessel']['beam'] = item.pop('beam', None)
    item['vessel']['length'] = item.pop('length', None)
    if item['cargoes'][0]['product'] in INVALID_CARGOES:
        return None
    if not item.get('movement'):
        return None
    if len(item['cargoes']) > 1:
        item['volume'] = int(int(item['volume']) / 2)
    cargoes = item.pop('cargoes', None)
    for cargo in cargoes:
        cargo['volume'] = item['volume']
        cargo['movement'] = item['movement']
        if cargo['movement'] == 'load':
            cargo['seller'] = {'name': item['buyer/seller']} if item.get('buyer/seller') else None
        elif cargo['movement'] == 'discharge':
            cargo['buyer'] = {'name': item['buyer/seller']} if item.get('buyer/seller') else None
    for col in ('movement', 'volume', 'buyer/seller'):
        item.pop(col, None)
    for cargo in cargoes:
        item['cargo'] = cargo
        yield item


def field_mapping(**kwargs):
    return {
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'Berços Berth (Calado Max.)': ('berth', None),
        'IMO': ('imo', None),
        'Navio Vessel': ('vessel', lambda x: {'name': may_strip(x.split('-')[0])}),
        'DWT': ('dead_weight', lambda x: x.replace('.', '')),
        'Comp. LOA': ('length', lambda x: x.split(',')[0]),
        'Boca Beam': ('beam', lambda x: x.split(',')[0]),
        'Prev. Chegada ETA': ('eta', lambda x: normalize_date(may_strip(x), **kwargs)),
        'Prev. Atrac. ETB': ('berthed', lambda x: normalize_date(may_strip(x), **kwargs)),
        'Prev. Desatrac. ETS': ('departure', lambda x: normalize_date(may_strip(x), **kwargs)),
        'movement': ('movement', lambda x: normalize_movement(x)),
        'Produto Cargo': ('cargoes', lambda x: list(normalize_cargo(x))),
        'Qtde. QTY (MT)': ('volume', lambda x: None if x == '-' else x.replace('.', '')),
        'Import./Export. Shipper/Receiver': ('buyer/seller', None),
    }


def normalize_movement(raw_movement):
    """Normalize raw movement.

    Args:
        raw_movement (str): raw movement string

    Returns:
        str: normalized movement

    Examples:
        >>> normalize_movement('D')
        'discharge'
        >>> normalize_movement('C')
        'load'
    """
    if raw_movement == 'D':
        movement = 'discharge'
    elif raw_movement == 'C':
        movement = 'load'
    else:
        logger.info(f'unable to normalize the movement or movement not interesting (A)')
        return None
    return movement


def normalize_date(raw_date, reported_date):
    """Normalize raw date to an ISO8601 compatible timestamp.

    Args:
        raw_date (str): raw date string
        reported_date (dt.datetime): used to infer missing month/year in the dates
        event (str): used to determine if we should increment month/year when inferring

    Returns:
        str: ISO8601 formatted timestamp

    Examples:
        >>> normalize_date('11/4', dt.date(year=2020, month=4, day=16))
        '2020-04-11T00:00:00'
    """
    split_date = raw_date.split('/')
    day, month, year = split_date[0], split_date[1], reported_date.year
    # handle year rollover scenarios
    if int(month) == 12 and reported_date.month == 1:
        year -= 1
    elif int(month) == 1 and reported_date.month == 12:
        year += 1

    return may_apply(f'{day}/{month}/{year}', to_isoformat)


def normalize_cargo(raw_cargo):
    """Normalize cargo string into 1 or more products and corresponding quantities

    Examples:
        >>> list(normalize_cargo('FERTILIZANTES'))
        [{'product': 'FERTILIZANTES', 'volume_unit': 'tons'}]

    Args:
        raw_cargo (str): raw cargo data

    Yields:
        Dict[str, str]:

    """
    for cargo in [may_strip(product) for product in raw_cargo.split('/')]:
        yield {
            'product': cargo,
            'volume_unit': Unit.tons,
        }
