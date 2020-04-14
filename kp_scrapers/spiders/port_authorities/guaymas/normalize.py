import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.i18n import SPANISH_TO_ENGLISH_MONTHS, translate_substrings
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MOVEMENT_MAPPING = {
    'CARGA': 'load',
    'DESCARGA': 'discharge',
    'DESEMBARQUE': 'discharge',
    'EMBARQUE': 'load',
}

CARGO_BLACKLIST = ['CONTENE']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, pc_mapping())

    # discard unmatched vessels
    if not item.get('vessel'):
        return

    # discard irrelevant vessels
    if not item.get('cargo_product'):
        return
    if any(alias in item['cargo_product'] for alias in CARGO_BLACKLIST):
        logger.warning(f'Vessel is carrying irrelevant cargo, discarding: {item["vessel"]["name"]}')
        return

    # normalize eta/arrival dates
    item.update(normalize_eta(item.get('eta', ''), item['reported_date']))
    # build cargo sub-model
    item['cargoes'] = [
        {'product': may_strip(prod), 'movement': item.get('cargo_movement')}
        for prod in item.get('cargo_product', '').split('/')
    ]

    # remove rogue fields
    item.pop('cargo_movement', None)
    item.pop('cargo_product', None)

    return item


def pc_mapping():
    return {
        'AGENCIACONSIGNATARIA': ignore_key('shipping agent'),
        'BUQUENACIONAL/ESLORA/TBR': ('vessel', normalize_vessel_attributes),
        'BUQUENACIONALIDAD/ESLORA/TBR': ('vessel', normalize_vessel_attributes),
        'CALADOSTRAMO': ignore_key('draught and berth; not required yet'),
        'DESTINOFECHADESALIDA': ignore_key('TODO extract next port ETA info'),
        'EMBARCADOR/RECIBIDOR': ignore_key('receiver'),
        'PROCEDENCIA/DESTINODELACARGA': ignore_key('irrelevant'),
        'PROCEDENTEDEE.T.A.': ('eta', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', normalize_reported_date),
        'TIPODEMANIOBRA': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x.partition('\n')[0])),
        'TONELAJEPRODUCTO': ('cargo_product', lambda x: x.partition('\n')[2]),
        'TRAFICO': ignore_key('irrelevant'),
    }


def normalize_vessel_attributes(raw_vessel):
    """Normalize raw vessel description string into a valid Vessel model.

    Args:
        raw_vessel (str):

    Returns:
        Dict[str, str]:

    Examples:  # noqa
        >>> normalize_vessel_attributes('HIGH MERCURY\\nI. MARSHALL /183/29,733 TONS')
        {'name': 'HIGH MERCURY', 'flag_name': 'I. MARSHALL', 'length': '183', 'gross_tonnage': '29733'}
        >>> normalize_vessel_attributes('FM PROSPERITY\\nSTVINCENT&G/123.50/7,460TONS')
        {'name': 'FM PROSPERITY', 'flag_name': 'STVINCENT&G', 'length': '123', 'gross_tonnage': '7460'}
        >>> normalize_vessel_attributes('FREJA HAFNIA\\nPANAMA/185.93 MTS ')
        {'name': 'FREJA HAFNIA', 'flag_name': 'PANAMA', 'length': '185', 'gross_tonnage': None}
        >>> normalize_vessel_attributes('THORM THYRA\\nSINGAPUR/183 MTS/30,128 TONS')
        {'name': 'THORM THYRA', 'flag_name': 'SINGAPUR', 'length': '183', 'gross_tonnage': '30128'}
        >>> normalize_vessel_attributes('BALTIC SPIRIT\\n188.6/17,174 TONS')
        {'name': 'BALTIC SPIRIT', 'flag_name': None, 'length': '188', 'gross_tonnage': '17174'}
        >>> normalize_vessel_attributes('THORM THYRA\\nSINGAPUR/ 183 MTS/30,128 TONS')
        {'name': 'THORM THYRA', 'flag_name': 'SINGAPUR', 'length': '183', 'gross_tonnage': '30128'}
    """
    vessel_name, _, vessel_attrs = raw_vessel.partition('\n')
    # empty vessel attributes; return name only
    if not may_strip(vessel_attrs):
        return {'name': may_strip(vessel_name)}

    pattern = (
        # flag country
        r'(?P<flag_name>[A-Z\s\.\&]+)?'
        # vessel length
        r'(?:\/)?\s*(?P<length>[\.\d]+)\s*(?:MTS)?'
        # gross tonnage
        r'(?:\/)?(?:(?P<gross_tonnage>[\d,\.]*)\s*TONS)?'
    )
    match = re.match(pattern, vessel_attrs)
    if not match:
        logger.error(f"Unknown vessel attributes format: {raw_vessel}")
        return None

    vessel = {
        'name': may_strip(vessel_name),
        'flag_name': may_strip(match.group(1)) if match.group(1) else None,
        'length': may_strip(match.group(2)).split('.')[0] if match.group(2) else None,
        'gross_tonnage': (
            may_strip(match.group(3)).replace(',', '').replace('.', '') if match.group(3) else None
        ),
    }
    return vessel


def normalize_eta(raw_str, reported_date):
    """Normalize origin and ETA info from a fuzzy string.

    Vessel identification information in the pdf is formatted and delimited as such:
    <ORIGIN> \n <ETA>

    However, the "origin" field may be not present occasionally (i.e., it has "TBC")

    Args:
        raw_str (str):

    Returns:
        Dict[str, str]: return eta/arrival in ISO-8601 format in a formatted dict

    """
    # ETAs may not be present occasionally and instead show 'FONDEADO' (docked)
    if any(sub in raw_str for sub in ('FONDEADO', 'FOPNDEADO')):
        logger.info('Vessel has already arrived in anchorage: {}'.format(reported_date))
        return {'eta': None, 'arrival': reported_date}

    # translate spanish month into english and get datetime object
    # NOTE we do this because `dateutil` is not locale aware
    _, _, eta = raw_str.partition('\n')
    if eta:
        return {
            'arrival': None,
            'eta': to_isoformat(translate_substrings(eta.lower(), SPANISH_TO_ENGLISH_MONTHS)),
        }

    logger.warning(f'Unable to process eta date: {raw_str}')
    return {'arrival': None, 'eta': None}


def normalize_reported_date(reported_date):
    """Normalize reported date string into ISO8601 format.

    Args:
        reported_date (str):

    Returns:
        str:

    """
    return to_isoformat(
        translate_substrings(reported_date.lower(), SPANISH_TO_ENGLISH_MONTHS),
        dayfirst=True,
        # allows us to ignore spanish prepositions like `de` and `del`
        fuzzy=True,
    )
