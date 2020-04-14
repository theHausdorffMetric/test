import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.units import Unit


logger = logging.getLogger(__name__)


INVALID_CARGOES = ['', '-', 'NIL', 'TBC']

PORT_MAPPING = {'SANTOS': 'SANTOS SAO PAULO'}

UNIT_MAPPING = {
    'bmt': Unit.tons,  # long ton
    'l': Unit.liter,
    'lt': Unit.tons,  # long ton
    'm2': Unit.cubic_meter,  # typo in report
    'm3': Unit.cubic_meter,
    'm³': Unit.cubic_meter,
    'mt': Unit.tons,
    'nt': Unit.tons,  # short/net ton
    't': Unit.tons,
}

# maximum possible cargo quantity figure (given by analyst)
MAX_TO_CONVERT = 350000.0


def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # process matching date
    eta = normalize_date(item['eta'], parse_date(item['reported_date'], dayfirst=True))
    etb = normalize_date(item['etb'], parse_date(item['reported_date'], dayfirst=True))
    # prioritise ETB over ETA as `eta`, if present
    item['eta'] = etb or eta

    # split products, quantities, units and movement
    cargoes = normalize_cargoes(item['load'], item['discharge'], item['product'])

    # discard irrelevant fields upon final normalisation
    for key in ('discharge', 'load', 'product', 'etb'):
        item.pop(key, None)

    for cargo in cargoes:
        item['cargo'] = cargo
        yield item


def field_mapping():
    return {
        'eta': ('eta', None),
        'etb': ('etb', None),
        'discharge': ('discharge', normalize_quantity),
        'installation': ('installation', None),
        'load': ('load', normalize_quantity),
        'port_name': ('port_name', normalize_port_name),
        'product': (
            'product',
            lambda x: [may_strip(product) for product in x.split('/') if x not in INVALID_CARGOES],
        ),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
        'vessel': ('vessel', lambda x: {'name': normalize_vessel_name(x)}),
    }


def normalize_port_name(raw_port):
    """Normalize raw port name into something recognisable.

    Args:
        raw_port (str):

    Returns:
        str:

    Examples:
        >>> normalize_port_name('IMBITUBA PORT')
        'IMBITUBA'
        >>> normalize_port_name('VITORIA')
        'VITORIA'
        >>> normalize_port_name('SANTOS PORT')
        'SANTOS SAO PAULO'

    """
    port_name = raw_port.partition(' PORT')[0]
    return PORT_MAPPING.get(port_name, port_name)


def normalize_date(raw_date, reported_date):
    """Get matching date based on presence of eta or etb string and reported month

    Args:
        raw_date (str):
        reported_date (datetime.datetime):

    Returns:
        str: ISO-8601 formatted matching date

    Examples:
        >>> normalize_date('08.06', parse_date('05.07.2018', dayfirst=True))
        '2018-06-08T00:00:00'
        >>> normalize_date('01.01', parse_date('30.12.2017', dayfirst=True))
        '2018-01-01T00:00:00'
        >>> normalize_date('26.12', parse_date('05.01.2018', dayfirst=True))
        '2017-12-26T00:00:00'

    """
    # sanity check since report can show cancelled/empty port calls
    if not re.match(r'^\d{2}\.\d{2}$', raw_date):
        logger.info(f'Invalid raw date: {raw_date}')
        return None

    # get 'MM' out of DD.MM
    raw_month = int(raw_date.partition('.')[2])

    # year may be different depending on the month
    # e.g. if report on `28.12.2018` and raw matching date is `03.01`,
    # then final matching date should be `03.01.2019`
    # TODO find a more robust solution to handle year rollover
    reported_month, reported_year = reported_date.month, reported_date.year
    if reported_month in [11, 12] and raw_month in [1, 2, 3]:
        reported_year += 1
    elif reported_month in [1, 2] and raw_month in [10, 11, 12]:
        reported_year -= 1

    # rebuild matching_date
    return try_apply(f'{raw_date}.{reported_year}', lambda x: to_isoformat(x, dayfirst=True))


def normalize_quantity(raw_quantity):
    """Split quantity field into an absolute value and unit

    Examples:
        >>> normalize_quantity('6.600 M3')
        (6600.0, 'cubic_meter')
        >>> normalize_quantity('17.668.614 MT')
        (17668.614, 'tons')
        >>> normalize_quantity('11.015,132 MT')
        (11015.132, 'tons')
        >>> normalize_quantity('11.015,132MT')
        (11015.132, 'tons')
        >>> normalize_quantity('9,415. MT')
        (9415.0, 'tons')
        >>> normalize_quantity('580.000 mt')
        (580.0, 'tons')
        >>> normalize_quantity('580.000 BMT')
        (580.0, 'tons')
        >>> normalize_quantity('17.8 m3')
        (17.8, 'cubic_meter')
        >>> normalize_quantity(10792000.0)
        (10792.0, 'tons')
        >>> normalize_quantity('')
        (None, None)
        >>> normalize_quantity('-')
        (None, None)

    """
    _volume, _unit = _split_volume_and_unit(raw_quantity)
    if not _volume:
        return None, None

    # get proper volume unit
    unit = UNIT_MAPPING.get(_unit.lower())
    if not unit:
        logger.warning(f'Unknown raw volume unit: {_unit}')
        unit = _unit

    # replace dot with comma and use last comma as decimal if 2 or more present
    _volume = _volume.replace('.', ',')
    # find index after last, and replace if number of digits after, is 1 or 2 (e.g. 17,8 -> 17.8),
    # or if there are more than 2 ',', which means that the number is probably too large
    if 0 < len(_volume.split(',')[-1]) < 3 or _volume.count(',') > 1:
        _volume = _volume[::-1].replace(',', '.', 1)[::-1]
    volume = float(_volume.replace(',', ''))

    # sanity check for impossibly large numbers
    if volume > MAX_TO_CONVERT:
        volume /= 1000.0

    return volume, unit


def _split_volume_and_unit(raw_volume):
    """Split a raw volume string into its constituent value and unit.

    Examples:
        >>> _split_volume_and_unit('6.600 M3')
        ('6.600', 'M3')
        >>> _split_volume_and_unit('17.668.614 MT')
        ('17.668.614', 'MT')
        >>> _split_volume_and_unit('11.015,132 MT')
        ('11.015,132', 'MT')
        >>> _split_volume_and_unit('11.015,132MT')
        ('11.015,132', 'MT')
        >>> _split_volume_and_unit('9,415. MT')
        ('9,415.', 'MT')
        >>> _split_volume_and_unit('580.000 mt')
        ('580.000', 'mt')
        >>> _split_volume_and_unit('580.000 BMT')
        ('580.000', 'BMT')
        >>> _split_volume_and_unit('17.8 m3')
        ('17.8', 'm3')
        >>> _split_volume_and_unit(10792000.0)
        ('10792000.0', 'tons')
        >>> _split_volume_and_unit('7.910.00')
        ('7.910.00', 'tons')
        >>> _split_volume_and_unit('')
        (None, None)
        >>> _split_volume_and_unit('-')
        (None, None)
    """
    for idx, char in enumerate(str(raw_volume)):
        if char.isalpha():
            return may_strip(raw_volume[:idx]), may_strip(raw_volume[idx:])

    # sometimes units are absent and detected as float, assume default unit of 'tons'
    return (None, None) if raw_volume in INVALID_CARGOES else (str(raw_volume), 'tons')


def normalize_vessel_name(raw_vessel):
    """Remove substring after '1st', '2nd', '3rd', '4th' if present

    Args:
        raw_vessel (str):

    Returns:
        str: cleaned vessel name

    Examples:
        >>> normalize_vessel_name('Eagle 1st Berthing')
        'Eagle'
        >>> normalize_vessel_name('STI MANHATTAN 2nd')
        'Sti Manhattan'
        >>> normalize_vessel_name('BOW SANTOS')
        'Bow Santos'

    """
    for blacklist in ('1st', '2nd', '3rd', '4th'):
        vessel_split = raw_vessel.lower().split(blacklist)
        if len(vessel_split) > 1:
            return may_strip(vessel_split[0]).title()

    return raw_vessel.title()


def normalize_cargoes(load, discharge, products):
    """Get list of product, qty and unit from load, discharge and cargo fields

    There are three possible scenarios:
        1) <LOAD or DISCHARGE>, <SINGLE PRODUCT>
        2) <LOAD and DISCHARGE>, <SINGLE PRODUCT>
        3) <LOAD and DISCHARGE>, <DOUBLE PRODUCTS>

    For 1), the logic is straightforward.
    For 2), yield a load and discharge cargo movement event separately for each item.
    For 3), first product will always be a load, second product will always be a discharge, yield
    separately.

    TODO discuss normalization logic with analysts for potential products > 2

    Args:
        load (Tuple[float | None, str | None]): tuple of (volume, unit)
        discharge (Tuple[float | None, str | None]): tuple of (volume, unit)
        raw_product (List[str]): list of all products transacted at port

    Returns:
        List[Dict[str, str]]:

    Examples:  # noqa
        >>> normalize_cargoes((None, None), (None, None), ['ETHANOL'])
        [{'product': 'ETHANOL'}]
        >>> normalize_cargoes((23000.0, 'tons'), (None, None), ['SOYBEANOIL'])
        [{'product': 'SOYBEANOIL', 'movement': 'load', 'volume': '23000', 'volume_unit': 'tons'}]
        >>> normalize_cargoes((23000.0, 'tons'), (17000.0, 'tons'), ['SOYBEANOIL'])  # doctest: +NORMALIZE_WHITESPACE
        [{'product': 'SOYBEANOIL', 'movement': 'load', 'volume': '23000', 'volume_unit': 'tons'},
         {'product': 'SOYBEANOIL', 'movement': 'discharge', 'volume': '17000', 'volume_unit': 'tons'}]
        >>> normalize_cargoes((26000.0, 'cubic_meter'), (24900.0,  'cubic_meter'), ['GAS OIL S10', 'GAS OIL S500'])  # doctest: +NORMALIZE_WHITESPACE
        [{'product': 'GAS OIL S10', 'movement': 'load', 'volume': '26000', 'volume_unit': 'cubic_meter'},
         {'product': 'GAS OIL S500', 'movement': 'discharge', 'volume': '24900', 'volume_unit': 'cubic_meter'}]

    """
    # sanity check in case of no volumes detected
    # we still yield the product, since it is useful data
    if all(x == (None, None) for x in (load, discharge)):
        return [{'product': product} for product in products]

    # scenario 1) and 2)
    if len(products) == 1:
        return list(_build_single_cargo(products[0], load, discharge))
    # scenario 3)
    if len(products) == 2:
        return _build_double_cargo(products, load, discharge)

    # TODO did not see cargo == 0 or > 2, parsing logic has not been discussed with analysts yet
    logger.info(f'Item contains more than 2 cargoes: {products}')
    return [{'product': product} for product in products]


def _build_single_cargo(product, load, discharge):
    """Build `cargo` dicts for each movement for a single product.

    If there's 1 product but 2 quantities, create 2 entries (load and discharge)
    This function assumes the presence of either a load or discharge, where at least one movement
    exists.

    Args:
        product (str): product string
        load (Tuple[float | None, str | None]): tuple of (volume, unit)
        discharge (Tuple[float | None, str | None]): tuple of (volume, unit)

    Yields:
        Dict[str, str]:

    """
    if load != (None, None):
        yield _build_cargo(product, 'load', *load)
    if discharge != (None, None):
        yield _build_cargo(product, 'discharge', *discharge)


def _build_double_cargo(products, load, discharge):
    """Build `cargo` dicts for each movement for a single product.

    This function assumes the presence of both a load and discharge, and a length 2 `products` list

    Args:
        products (List[str, str]): product list containing only two elements
        load (Tuple[float, str]): tuple of (volume, unit)
        discharge (Tuple[float, str]): tuple of (volume, unit)

    Yields:
        List[Dict[str, str]]:

    """
    cargo_load = list(_build_single_cargo(products[0], load, (None, None)))
    cargo_discharge = list(_build_single_cargo(products[1], (None, None), discharge))
    return cargo_load + cargo_discharge


def _build_cargo(product, movement, quantity, unit):
    """Wrapper for encapsulating logic for creating a Cargo model.
    """
    return {
        'product': product,
        'movement': movement,
        'volume': try_apply(quantity, int, str),
        'volume_unit': unit,
    }
