import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


UNIT_MAPPING = {'BBLS': Unit.barrel, 'BBL': Unit.barrel, 'MT': Unit.tons}


MOVEMENT_MAPPING = {'LOAD': 'load', 'DISCH.': 'discharge', 'DISCH': 'discharge'}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping())
    # remove vessels that are None type
    if not item['vessel']:
        return

    if item.get('country_port'):
        _, item['port_name'] = split_country_port(item.pop('country_port'))

    cargo_unit = item.pop('cargo_unit', None)
    unit = cargo_unit if item.get('cargo_volume', None) != '' else None
    movement = item.pop('cargo_movement', None)

    item['installation'], item['berth'] = split_installation_berth(
        item.pop('installation_berth', None)
    )

    # build cargo sub-model, if multiple cargoes, yield two items
    for product, quantity in zip(
        *normalize_cargo(
            item.pop('cargo_product', None), item.pop('cargo_volume', None), cargo_unit
        )
    ):
        # cpp analyst request
        if item['port_name'].lower() == 'skikda' and product.lower() == 'lsfo':
            product = 'LSSRFO'

        item['cargo'] = {
            'product': product,
            'movement': movement,
            'volume': quantity if quantity != '' else None,
            'volume_unit': unit,
        }

        yield item


def field_mapping():
    return {
        'COUNTRY': (ignore_key('Country')),
        'COUNTRY PORT': ('country_port', None),
        'PORT': ('port_name', None),
        'TERMINAL': ('installation_berth', None),
        'VESSEL': ('vessel', lambda x: {'name': x} if 'TBN' not in x else None),
        'PREVIOUS': (ignore_key('Previous')),
        'NEXT': (ignore_key('Next')),
        'LOAD/DISCH.': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x, None)),
        'GRADE DETAIL': ('cargo_product', lambda x: x.replace('N/A', '')),
        'CARGO': ('cargo_product', None),
        'QUANTITY': ('cargo_volume', None),
        'BBLS/MT': ('cargo_unit', lambda x: UNIT_MAPPING.get(x, x)),
        'DATE': ('berthed', lambda x: parse_date(x, dayfirst=True).isoformat()),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        # some reports denoted QUANTITY as QTTY, LOAD/DISCH as LOAD/DISC.,BBLS/MT as BBL/MT
        'QTTY': ('cargo_volume', None),
        'LOAD/DISCH': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x, None)),
        'BBL/MT': ('cargo_unit', lambda x: UNIT_MAPPING.get(x, x)),
    }


def normalize_cargo(product_str, qty_str, unit_str):
    """Parse product string into 1 or more products and corresponding quantities

    Assign normally if a qty is given for each product.
    There are cases where there are more products than quantities,
    we divide that qty by the number of products.
    (Does not work for rare case: k products, n quantities, where k > n and n > 1)

    A special case where, there are multiple sub products within a main product. The code will
    handle that as well. Logic is design as per discussion with PO. Refer to the last example below.

    Examples:
        >>> normalize_cargo('GASOIL/JET', '1000-2000', 'tons')
        (['GASOIL', 'JET'], ['1000', '2000'])
        >>> normalize_cargo('GASOIL/JET', '2000', 'tons')
        (['GASOIL', 'JET'], ['1000', '1000'])
        >>> normalize_cargo('GASOIL/JET', '', 'tons')
        (['GASOIL', 'JET'], [None, None])
        >>> normalize_cargo('GASOIL', '', 'barrel')
        (['GASOIL'], [''])
        >>> normalize_cargo('GASOIL', '1000', 'tons')
        (['GASOIL'], ['1000'])
        >>> normalize_cargo('CRUDE OIL(AMNA/SIRTICA BLEND)', '671-30', 'barrel')
        (['CRUDE OIL(AMNA)', 'CRUDE OIL(SIRTICA BLEND)'], ['671000', '30000'])
        >>> normalize_cargo('CRUDE OIL(SIRTICA BLEND)', '671-30', 'barrel')
        (['CRUDE OIL(SIRTICA BLEND)'], ['671000'])
    """
    main_product, sub_product = seperate_the_main_product(product_str)
    products = [
        _add_the_main_product(may_strip(product), main_product)
        for product in sub_product.split('/')
    ]

    if qty_str:
        quantities = []
        for quantity in qty_str.split('-'):
            # source will sometimes include units in volume column
            _match = re.match(r'([0-9]+)', quantity)
            if _match:
                quantity = _match.group(0)

            # source occasionally will use the wrong units,
            # i.e 600 barrels, this should be 600000 barrels
            # or 600 kb. Value of 2000 was taken based on vlcc size
            if unit_str == 'barrel' and int(quantity) < 2000:
                quantity = str(int(quantity) * 1000)

            quantities.append(quantity)

    else:
        quantities = ['']  # to handle case where quantity is not given in the source.

    if len(products) != len(quantities):
        if '' in quantities:
            quantities = [None for _ in enumerate(products)]
        elif '' not in quantities:
            quantities = [str(int(quantities[0]) // len(products))] * len(products)

    return products, quantities


def seperate_the_main_product(product_str):
    """Parse the product string into main and sub products

    EXAMPLES:
        >>> seperate_the_main_product('CRUDE OIL(AMNA/SIRTICA BLEND)')
        ('CRUDE OIL', 'AMNA/SIRTICA BLEND')
        >>> seperate_the_main_product('AMNA/SIRTICA BLEN')
        (None, 'AMNA/SIRTICA BLEN')
    """
    _match = re.match(r'(.*)\((.*)\)', product_str)
    if _match:
        return _match.groups()
    else:
        return None, product_str


def _add_the_main_product(sub_product, main_product):
    """Append the sub products to the main products

    EXAMPLES:
        >>> _add_the_main_product('AMNA', 'CRUDE OIL')
        'CRUDE OIL(AMNA)'
        >>> _add_the_main_product('AMNA', None)
        'AMNA'
    """
    if main_product:
        return main_product + "(" + sub_product + ")"
    else:
        return sub_product


def split_installation_berth(raw_berth_installation):
    """Append the sub products to the main products

    EXAMPLES:
        >>> split_installation_berth('BTC - 2E')
        ('BTC', '2E')
        >>> split_installation_berth('BTC')
        ('BTC', None)
        >>> split_installation_berth('N/A')
        (None, None)
    """
    list_berth_installation = raw_berth_installation.split('-')

    if list_berth_installation[0] != 'N/A':
        if len(list_berth_installation) == 2:

            return may_strip(list_berth_installation[0]), may_strip(list_berth_installation[1])

        elif len(list_berth_installation) == 1:

            return list_berth_installation[0], None

    return None, None


def split_country_port(raw_country_port):
    """occasionally country and port cols are merged together

    EXAMPLES:
        >>> split_country_port('Spain La Coruna')
        ('Spain', 'La Coruna')
    """
    list_country_port = raw_country_port.split(' ')
    return list_country_port[0], ' '.join(list_country_port[1:])
