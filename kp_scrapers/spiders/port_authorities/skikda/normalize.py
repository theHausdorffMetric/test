import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.i18n import FRENCH_TO_ENGLISH_MONTHS, translate_substrings
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

CARGO_BLACKLIST = ['cont', 'roul', 'diver', 'pass', 'big bag', 'p/v', 'dechargement']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        Dict[str, str]:

    Returns:
        Dict[str, str]:
    """
    item = map_keys(raw_item, field_mapping())

    # discard irrelevant vessels/cargoes
    if not item.get('cargoes'):
        logger.info(f'Vessel {raw_item["Navire"]} has irrelevant cargo {raw_item["Marchandise"]}')
        return

    # discard vessel movements without dates
    if not item.get('eta_arrival') and not item.get('berthed'):
        logger.info(f'Vessel {raw_item["Navire"]} has no port call date')
        return

    # properly assign shared eta/arrival fields
    _context = item.pop('url', '')
    if 'attendus' in _context:
        item['eta'] = item.pop('eta_arrival')
    elif 'rade' in _context:
        item['arrival'] = item.pop('eta_arrival')

    return item


def field_mapping():
    return {
        'Agent Consignataire': ignore_key('shipping agent'),
        'Date / Heure Arrivée': ('eta_arrival', normalize_pc_date),
        'Date et Heure d\'entrée': ('berthed', normalize_pc_date),
        'Date et Heure de Départ': ignore_key('date of departure'),
        'Marchandise': ('cargoes', normalize_cargoes),
        'Navire': ('vessel', lambda x: {'name': x}),
        'Observations': ignore_key('irrelevant'),
        'Pavillon': ignore_key('vessel flag'),
        'port_name': ('port_name', None),
        'Poste': ignore_key('berth'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', normalize_reported_date),
        'Provenance': ignore_key('previous port'),
        'Tonnage': ignore_key('cargo volume (not reliable enough)'),
        'url': ('url', None),
    }


def normalize_pc_date(raw_date):
    """Normalize portcall-related dates into ISO8601-formatted date strings.

    Args:
        raw_date (str | None):

    Returns:
        str: ISO8601-formatted date string

    Examples:
        >>> normalize_pc_date('lun 12/03/2018 à 16:40')
        '2018-03-12T16:40:00'
        >>> normalize_pc_date('mer 14/03/2018 à 12:00')
        '2018-03-14T12:00:00'
        >>> normalize_pc_date('mar 18/09/2018 à 01:00')
        '2018-09-18T01:00:00'
        >>> normalize_pc_date('')

    """
    _, _, raw_date = raw_date.partition(' ') if raw_date else (None, None, None)
    return to_isoformat(raw_date, dayfirst=True, fuzzy=True) if raw_date else None


def normalize_reported_date(raw_date):
    """Normalize reported date into an ISO8601 date string.

    Args:
        raw_date (str):

    Returns:
        str: ISO8601-formatted date string

    Examples:
        >>> normalize_reported_date('Attendus lundi 18 juin 2018')
        '2018-06-18T00:00:00'
        >>> normalize_reported_date('Quai mardi 6 août 2018')
        '2018-08-06T00:00:00'
        >>> normalize_reported_date('Rade mercredi 19 septembre 2018')
        '2018-09-19T00:00:00'
    """
    return to_isoformat(
        translate_substrings(' '.join(raw_date.split()[2:]), FRENCH_TO_ENGLISH_MONTHS)
    )


def normalize_cargoes(raw_cargo):
    """Normalize raw product to a valid Cargo object.

    Args:
        raw_cargo (str | None):

    Returns:
        Dict[str, str]:
    """
    if not raw_cargo or any(alias in raw_cargo.lower() for alias in CARGO_BLACKLIST):
        return None

    return [{'product': product} for product in re.split(r'[\-\/]', raw_cargo)]
