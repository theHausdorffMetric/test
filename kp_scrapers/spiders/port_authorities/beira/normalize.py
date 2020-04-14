import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.i18n import translate_substrings
from kp_scrapers.lib.parser import may_remove_substring, may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_CARGO = ['EQUIPMENT', 'FISH', 'N/A', 'TBA', 'WATER']

MULTI_CARGO_SIGN = ['+', '&']

CARGO_MOVEMENT_MAPPING = {'EXP': 'load', 'IMP': 'discharge'}

PORTUGESE_TO_ENGLISH_MONTHS = {
    'jan': 'JAN',
    'fev': 'FEB',
    'março': 'MAR',
    'marco': 'MAR',
    'abril': 'APR',
    'maio': 'MAY',
    'junho': 'JUN',
    'julho': 'JUL',
    'agosto': 'AUG',
    'set': 'SEP',
    'out': 'OCT',
    'nov': 'OCT',
    'dez': 'DEC',
}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Tranform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:
    """
    item = map_keys(raw_item, field_mapping())
    # discard container vessels and irrelevant cargo vessels
    if item.pop('is_container_vessel', False) or not item.get('cargo_product'):
        return

    # build vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_length', None)}

    _movement = item.pop('cargo_movement', None)
    item['cargoes'] = [
        {'product': prod, 'movement': _movement} for prod in item.pop('cargo_product')
    ]

    return item


def field_mapping():
    return {
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'VESSEL': ('vessel_name', lambda x: x.replace('M.V ', '')),
        'QUAY': ignore_key('quay number of portcall'),
        'IMP/EXP': ('cargo_movement', lambda x: CARGO_MOVEMENT_MAPPING.get(x)),
        'LOA': ('vessel_length', lambda x: try_apply(x.replace('M', ''), float, int, str)),
        'TYPE': ('is_container_vessel', lambda x: x == 'CONT'),
        'AGENT': ignore_key('shipping agent'),
        'CARRIER': ignore_key('carrier'),
        'TERMINAL': ignore_key('berth terminal'),
        'TNS': ('cargo_product', normalize_cargo),
        'ATD': ('departure', normalize_date),
        'ETD': ('departure', normalize_date),
        'ATB': ('berthed', normalize_date),
        'ETB': ('berthed', normalize_date),
        'ATA': ('arrival', normalize_date),
        'ETA': ('eta', normalize_date),
    }


def normalize_date(raw_date):
    """Convert raw date to ISO 8601 format.

    Examples:
        >>> normalize_date("03-August-2018'")
        '2018-08-03T00:00:00'
        >>> normalize_date("27-July-2018\' at 1700HRS")
        '2018-07-27T17:00:00'
        >>> normalize_date('10-Sep 2018 at 1430hrs')
        '2018-09-10T14:30:00'
        >>> normalize_date('05-out-18 1400 Hrs')
        '2018-10-05T14:00:00'
        >>> normalize_date('05-out- 18 1400 Hrs')
        '2018-10-05T14:00:00'
        >>> normalize_date('12-out-2018 as 0400HRS')
        '2018-10-12T04:00:00'

    Args:
        raw_date (str):

    Returns:
        str:

    """
    clean_date = may_remove_substring(raw_date.lower(), ['at', 'as', 'hrs', '\'']).replace('-', ' ')
    clean_date = translate_substrings(clean_date, PORTUGESE_TO_ENGLISH_MONTHS)
    return to_isoformat(clean_date, dayfirst=True)


def normalize_cargo(raw_cargo):
    """Normalizes cargo into format defined in `PortCall`

    Cargo information can be in the following formats:
        1. 26.600M/TONES OF UREA IN BULK
        2. 22.000M/TONES OF FERTILIZER & 5.000M/TONES OF GRANITE BLOCKS
        3. 31.771M/TONES OF GASOLINE+GASOIL + JETA1
        4. 31.771M/TONES OF GASOLINE&GASOIL & JETA1

    Args:
        raw_cargo (str):

    Returns:
        List[str]: list of products

    Examples:
        >>> normalize_cargo('3.018M/TONES OF AMMONIUN NITRATE')
        ['AMMONIUN NITRATE']
        >>> normalize_cargo('22,000 TONS GRAIN UREA')
        ['GRAIN UREA']
        >>> normalize_cargo('DISC 15.000M/TONS ON AMMONIUM NITRATE IN BIG BAGS')
        ['AMMONIUM NITRATE IN BIG BAGS']
        >>> normalize_cargo('DISC 15.000M/TONSOF AMMONIUM NITRATE IN BIG BAGS')
        ['AMMONIUM NITRATE IN BIG BAGS']
        >>> normalize_cargo('31.771M/TONES OF GASOLINE+GASOIL + JETA1')
        ['GASOLINE', 'GASOIL', 'JETA1']
        >>> normalize_cargo('DISC 1.912M/TONS OF EQUIPMENTS')
        >>> normalize_cargo('8,000 TONS')
        >>> normalize_cargo('FRESH WATER FILLING')
    """
    # discard cargo if it's been blacklisted
    if any(alias in raw_cargo for alias in IRRELEVANT_CARGO):
        logger.warning(f'Discarding irrelevant cargo: {raw_cargo}')
        return None

    raw_cargo = raw_cargo.replace('\n', ' ')
    _split = re.split(r'(?:TONNES|TONES|TONS|TON)\s*(?:OF|ON)?\s*', raw_cargo, maxsplit=1)
    if len(_split) == 1:
        logger.warning(f'Unable to normalize cargo data, returning as is: {raw_cargo}')
        return [may_strip(raw_cargo)]

    products = _split[1]
    if not products:
        return None

    for sign in MULTI_CARGO_SIGN:
        if sign in products:
            return [may_strip(each) for each in products.split(sign)]

    return [may_strip(products)]
