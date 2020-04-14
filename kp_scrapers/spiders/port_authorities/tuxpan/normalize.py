import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys, remove_diacritics
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


PREFIX_TO_REMOVE = ['AB.', 'PLAT.', 'REM.', 'B.', 'AB.', 'B/O ', 'D.', 'B/M', 'B/T']

DATE_PATTERN = r'%d/%m/%Y %H:%M:%S %p'
ISO_PATTERN = r'%Y-%m-%dT%H:%M:%S'

MATCHING_DATE_PATTERN_EXACT = r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2})'
MATCHING_DATE_PATTERN_FUZZY = r'(\d{2}/\d{2}/\d{4})'

IRRELEVANT_PRODUCT = ['', 'NINGUNA', 'REPARACIÓN']

logger = logging.getLogger(__name__)


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Process and normalize item.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, field_mapping())

    # build vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('length', None)}

    # build cargoes sub-model
    item['cargoes'] = normalize_cargoes(item)
    item.pop('product', '')
    item.pop('product_no_danger', '')
    item.pop('volume', '')
    if not item['cargoes']:
        return

    return item


def field_mapping():
    return {
        'BUQUES EN OPERACIÓN': ('vessel_name', normalize_vessel_name),
        'VIAJE': ignore_key('trip'),
        'ARRIVAL': ('arrival', normalize_date),
        'FECHA DE ARRIBO': ('arrival', normalize_date),
        'MUELLE/BOYA': ignore_key('berth'),
        'PROCEDENCIA': ignore_key('origin'),
        'PELIGROSA': ('product', None),
        'MERCANCIA': ('product', None),
        'NO PELIGROSA': ('product_no_danger', None),
        'TONELADAS': ('volume', None),
        'AGENCIA CONSIGNATARIA': ignore_key('shipping agent'),
        'OPERADORA/PRESTADOR': ignore_key('operator/provider'),
        'E.T.D.': ignore_key('irrelevant'),
        'BUQUES PROGRAMADOS': ('vessel_name', normalize_vessel_name),
        'ETA': ('eta', normalize_date),
        'BANDERA': ignore_key('flag'),
        'TRB': ignore_key('trb'),
        'ESLORA\n(MTS)': ('length', None),
        'MANGA\n(MTS)': ignore_key('irrelevant'),
        'CALADO MAXIMO (FTS)': ignore_key('draught'),
        'T.E.T.': ignore_key('irrelevant'),
        'BUQUES FONDEADOS': ('vessel_name', normalize_vessel_name),
        'BERTHED': ('berthed', normalize_date),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_vessel_name(vessel_name):
    """Normalize vessel name.

    Examples:
        >>> normalize_vessel_name('B/T STI BOSPHORUS')
        'STI BOSPHORUS'

    Args:
        vessel_name (str):

    Returns:
        str:

    """
    for prefix in PREFIX_TO_REMOVE:
        vessel_name = vessel_name.replace(prefix, '', 1).strip()
    return vessel_name


def normalize_date(raw_date):
    """Normalize date.

    Date strings given may not contain time information, so this function will iterate across
    all possible patterns that have been found in the pdf so far.

    Examples:
        >>> normalize_date('03/10/2018 11:30')
        '2018-10-03T11:30:00'
        >>> normalize_date('03/10/2018')
        '2018-10-03T00:00:00'
        >>> normalize_date('SIN PROGRAMA')

    Args:
        raw_date (str):

    Returns:
        str: ISO 8601 format

    """
    # matching date has both date and time info
    if re.match(MATCHING_DATE_PATTERN_EXACT, raw_date):
        return to_isoformat(raw_date, dayfirst=True)

    # matching date only has date info, no time info
    elif raw_date.split():
        date_match = re.match(MATCHING_DATE_PATTERN_FUZZY, raw_date.split()[0])
        if date_match:
            return to_isoformat(raw_date, dayfirst=True)

    # matching date is missing from the table
    logger.warning('Unable to match date: {}'.format(raw_date))
    return None


def normalize_cargoes(item):
    """Normalize cargoes.

    Args:
        item (Dict[str, str]):

    Returns:
        List[Dict[str, str]] | None:

    """
    product = item.get('product', item.get('product_no_danger', ''))
    if product in IRRELEVANT_PRODUCT:
        return None

    return [
        {
            'product': remove_diacritics(product),
            # 'volume': item.get('volume').replace(',', ''),
            # 'volume_unit': Unit.tons,
        }
    ]
