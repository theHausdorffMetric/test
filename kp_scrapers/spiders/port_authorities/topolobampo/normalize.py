import logging

from kp_scrapers.lib.date import may_parse_date_str, to_isoformat
from kp_scrapers.lib.i18n import translate_substrings
from kp_scrapers.lib.parser import may_remove_substring
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_VALUES = ['TBC', '- - - -']

MOVEMENT_MAPPING = {'EXPORTACIÓN': 'load', 'IMPORTACIÓN': 'discharge'}

SPANISH_TO_ENGLISH_MONTHS = {
    'enero': 'january',
    'febrero': 'february',
    'marzo': 'march',
    'abril': 'april',
    'mayo': 'may',
    'junio': 'june',
    'julio': 'july',
    'agosto': 'august',
    'septiembre': 'september',
    'octubre': 'october',
    'octubure': 'october',
    'noviembre': 'november',
    'diciembre': 'december',
}

VESSEL_TYPE_BLACKLIST = ['CRUCERO']


@validate_item(PortCall, normalize=True, strict=True)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # discard irrelevant vessels
    if not item['vessel_name'] or item.pop('vessel_type', None) in VESSEL_TYPE_BLACKLIST:
        return

    # build Vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_length', None)}

    # build Cargo sub-model
    if item.get('cargo_product'):
        item['cargoes'] = [
            {
                'product': item.pop('cargo_product'),
                'movement': item.pop('cargo_movement'),
                'volume': item.pop('cargo_volume'),
                'volume_unit': Unit.tons,
            }
        ]

    return item


def field_mapping():
    return {
        'AGENCIES': ('shipping_agent', _clean_string),
        'BERTH': ('berthed', normalize_date_str),
        'CARGO DESCRIPTION': ('cargo_product', _clean_string),
        'ETA': ('eta', normalize_date_str),
        'ETD': ignore_key('estimated departure of vessel at port'),
        'FLAG': ignore_key('vessel flag; ignored since it has not been normalised to ISO3166'),
        'FROM': ignore_key('previous port of call of vessel'),
        # split by `.` to get rid of decimal points
        'LENGTH': ('vessel_length', lambda x: _clean_string(x.split('.')[0])),
        'NEXT PORT': ignore_key('next port of call'),
        'port_name': ('port_name', None),
        'PROPOSED DOCK': ('berth', _clean_string),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', normalize_reported_date),
        'TRADE': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x)),
        'TRAFFIC': ignore_key('traffic ??? unsure'),
        'TONNAGE': ('cargo_volume', lambda x: int(x.replace(',', ''))),
        'TYPE': ('vessel_type', _clean_string),
        'USER': ignore_key('vessel owner'),
        'VESSEL': ('vessel_name', _clean_string),
    }


def normalize_date_str(raw_str):
    """Normalize raw date strings into something that can be parsed by `to_isoformat`.

    Date strings from the source can displayed in one of three formats:
        - 18/03/2018 00:30
        - 18/03/2018 AM
        - 18/03/2018

    The second format cannot be parsed by `to_isoformat` and hence needs to be standardised
    before passing it to `to_isoformat`.

    Args:
        raw_str (str): raw date string

    Returns:
        str: standardised date string

    Examples:
        >>> normalize_date_str('18/03/2018 16:45')
        '2018-03-18T16:45:00'
        >>> normalize_date_str('18/03/2018 AM')
        '2018-03-18T00:00:00'
        >>> normalize_date_str('18/03/2018 PM')
        '2018-03-18T00:00:00'
        >>> normalize_date_str('18/03/2018')
        '2018-03-18T00:00:00'
        >>> normalize_date_str('TBC')
        >>> normalize_date_str('- - - -')

    """
    date_object = (
        may_parse_date_str(raw_str, '%d/%m/%Y %H:%M')
        or may_parse_date_str(raw_str, '%d/%m/%Y %p')
        or may_parse_date_str(raw_str, '%d/%m/%Y')
    )
    return date_object.isoformat() if date_object else None


def normalize_reported_date(raw_str):
    """Normalize reported date of page at time of scraping.

    Reported date is contained as one of the page headers in this format (as of 2 April 2018):
    "PROGRAMACIÓN DE ARRIBOS AL <date> DE <spanish_month> DE <year> - 12 :00 hrs."

    Args:
        raw_date (str): reported date in raw form

    Returns:
        str: reported date in ISO-8601 format

    Examples:
        >>> normalize_reported_date('PROGRAMACIÓN DE ARRIBOS AL 30 DE ABRIL DE 2019 - 12 :00 hrs.')
        '2019-04-30T12:00:00'

    """
    raw_str = may_remove_substring(raw_str, blacklist=['PROGRAMACIÓN DE ARRIBOS AL', 'hrs.', 'DE'])
    raw_date, raw_time = raw_str.split(' - ')
    raw_date = translate_substrings(raw_date.lower(), translation_dict=SPANISH_TO_ENGLISH_MONTHS)

    return to_isoformat(' '.join([raw_date, raw_time.replace(' ', '')]))


def _clean_string(raw):
    # few records in the source comes with an added 'm' at the end
    return None if raw in IRRELEVANT_VALUES else raw.replace('m', '')
