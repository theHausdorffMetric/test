import datetime as dt
import logging

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.date import is_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


IRRELEVANT_PRODUCTS = ['BY PRODUCTS', 'CARGO', 'OTHERS', 'SPECIALTIES']

# if not provided, `port_name` will be normalized as None
PORT_MAPPING = {
    'BAHIA BLANCA': 'BAHIA BLANCA',
    'CAMPANA': 'CAMPANA',
    'DEL GUAZU': 'GUAZU',
    'LA PLATA': 'LA PLATA',
    'MONTEVIDEO': 'MONTEVIDEO',
    'NECOCHEA': 'NECOCHEA',
    'NUEVA PALMIRA': 'NUEVA PALMIRA',
    'RAMALLO': 'RAMALLO',
    'ROSARIO': 'ROSARIO',
    'SAN LORENZO': 'SAN LORENZO ARGENTINA',
    'SAN NICOLAS': 'SAN NICOLAS DE LOS ARROYOS',
    'ZARATE': 'ZARATE',
}

MOVEMENT_MAPPING = {'DISCH': 'discharge', 'LOAD': 'load'}

SPANISH_MONTH_MAPPING = {
    'enero': 'jan',
    'marzo': 'mar',
    'abr': 'apr',
    'mayo': 'may',
    'set': 'sep',
    'sept': 'sep',
    'dic': 'dec',
}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:
    """
    item = map_keys(raw_item, pc_mapping())

    # discard vessel movements to irrelevant ports
    if not item.get('port_name'):
        logger.info(f'Discarding vessel movement at irrelevant port: {raw_item["Port"]}')
        return

    # discard irrelevant products:
    _product_child = item.pop('cargo_product', '')
    _product_parent = item.pop('cargo_product_parent', '')
    if not _product_child or any(irr in _product_parent for irr in IRRELEVANT_PRODUCTS):
        logger.info(f'Discarding irrelevant product: {_product_parent} ({_product_child})')
        return

    # build proper Cargo sub-model
    item['cargoes'] = [
        {
            'product': f'{_product_parent} ({_product_child})',
            'movement': item.pop('cargo_movement', None),
        }
    ]

    # build proper ETA
    item['eta'] = normalize_date(item.get('eta'), item['reported_date'])
    item['berthed'] = normalize_date(item.get('berthed'), item['reported_date'])
    item['departure'] = normalize_date(item.get('eta'), item['reported_date'])

    return item if item['eta'] else None


def pc_mapping():
    return {
        'Area': ignore_key('alternate column for previous/next port'),
        'Berth': ignore_key('berth'),
        'Cargo': (
            'cargo_product',
            lambda x: '' if any(alias in x for alias in IRRELEVANT_PRODUCTS) else x,
        ),
        'Cat': ('cargo_product_parent', None),
        'Dest/Orig.': ignore_key('previous/next port'),
        'ETA': ('eta', None),
        'ETB': ('berthed', None),
        'ETS': ('departure', None),
        'Ops.': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x)),
        'Port': ('port_name', lambda x: PORT_MAPPING.get(x, x)),
        'provider_name': ('provider_name', None),
        'Quantity': ignore_key('TODO cargo volume; confirm volume unit with analysts'),
        'Remarks': ignore_key('remarks for the vessel movement'),
        'reported_date': ('reported_date', None),
        'Shipper': ignore_key('shipping agent'),
        'Vessel': ('vessel', lambda x: {'name': x}),
    }


def normalize_date(raw_date, reported_date):
    """Normalize ETA, ETB, ETS date.

    Args:
        raw_eta (str):
        reported_date (str): date is in ISO-8601 format

    Returns:
        str | None: date in ISO-8601 format

    Examples:
        >>> normalize_date('6-Oct', '2018-09-21T00:00:00')
        '2018-10-06T00:00:00'
        >>> normalize_date('6-Jan', '2018-12-21T00:00:00')
        '2019-01-06T00:00:00'
    """
    if not raw_date:
        return

    if is_isoformat(raw_date):
        return raw_date

    if len(raw_date.split('-')) != 2:
        logger.warning('Not a valid date: {}'.format(raw_date))
        return

    day, month = raw_date.split('-')

    # in case month is in spanish
    month = SPANISH_MONTH_MAPPING.get(month, month)

    _reported_date = parse_date(reported_date, dayfirst=False)

    try:
        date = parse_date(f'{day} {month} {_reported_date.year}', dayfirst=True)
    except ValueError:
        logger.warning('Not a valid date: {}'.format(raw_date))
        return None

    # sanity check for cases where there is year rollover
    if date - _reported_date < dt.timedelta(days=-180):
        date += relativedelta(years=1)

    return date.isoformat()
