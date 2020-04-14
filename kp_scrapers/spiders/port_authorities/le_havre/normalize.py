import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_apply, may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


VESSEL_TYPE_BLACKLIST = [
    'AUM',  # AUTOMOTEUR
    'DSH',  # SUCTION HOPPER DREDGER
    'DTS',  # TRAILING SUCTION HOPPER DREDGER
    'FFS',  # FISHING
    'FRC',  # RIVER CRUISE
    'GGC',  # GENERAL CARGO
    'MPR',  # PASSENGER
    'MVE',  # VEHICLE CARRIER
    'OBA',  # BARGE
    'OPO',  # PONTOON
    'OYT',  # YACHT
    'PRR',  # PASSENGER RO/RO
    'ROR',  # OCEANOGRAPHIC RESEARCH VESSEL
    'TAS',  # ASHPALT TANKER
    'UCC',  # CONTAINER CARRIER
    'XPT',  # PUSHER TUG
    'XTG',  # TUG
]

VESSEL_TYPE_MAPPING = {
    # 'CBO': 'Crude Oil Tanker',  # unknown vessel type; TODO discover full name on website
    # 'COO': 'Crude Oil Tanker',  # unknown vessel type; TODO discover full name on website
    # 'LNP': 'LPG Tanker',  # unknown vessel type; TODO discover full name on website
    'LPG - LIQUEFIED GAS TANKER': 'LPG Tanker',
    # 'OHT': 'Crude Oil Tanker',  # unknown vessel type; TODO discover full name on website
    'TCH - CHEM.TANK': 'Chemical/Oil Products Tanker',
    'TCO - CHEM.OIL CARRIER': 'Chemical/Oil Products Tanker',
    'TCR - CRUDE OIL TANKER': 'Crude Oil Tanker',
    'TPD - PRODUCT TANKER': 'Products Tanker',
}

BERTH_TO_INSTALLATION_MAPPING = {
    'ANTIFER': 'Antifer',
    'ATO 1': 'ATO',
    'ATO 2': 'ATO',
    'CIM 8': 'Le Havre',
    'CIM 10': 'Le Havre',
    'CIE INDUS.MARITIME': 'Le Havre',
    'SOG 1': 'Le Havre 1',
    'SOG 2': 'Le Havre 1',
    'SOG 3': 'Le Havre 1',
}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # discard irrelevant vessels
    if not item['vessel_type']:
        logger.info("Irrelevant vessel type, skipping vessel %s", item['vessel_name'])
        return

    # build vessel sub model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'imo': item.pop('vessel_imo'),
        'flag_name': item.pop('vessel_flag', None),
        'vessel_type': item.pop('vessel_type', None),
        'dead_weight': item.pop('vessel_dwt', None),
        'length': item.pop('vessel_length', None),
        'gross_tonnage': item.pop('vessel_gt', None),
    }

    # build proper portcall dates
    item['arrival'], item['eta'] = choose_arrival_and_eta(item)

    return item


def portcall_mapping():
    return {
        'Numéro escale GPHM': ignore_key('internal port identification number'),
        'Numéro APPLUS': ignore_key('internal port identification number'),
        'Numéro VOS': ignore_key('internal port identification number'),
        # ETA (rough estimation)
        'PVA': ('eta_rough', lambda x: to_isoformat(x, dayfirst=True)),
        # ETA (precise; only available 24h before arrival) =
        '24H': ('eta_precise', lambda x: to_isoformat(x, dayfirst=True)),
        # time when radio contact is made
        'VHF': ('eta_radio_contact', lambda x: to_isoformat(x, dayfirst=True)),
        # time when vessel arrived at anchorage
        'Arrivé sur rade': ('arrival_at_anchorage', lambda x: to_isoformat(x, dayfirst=True)),
        # scheduled time for pilot to board
        'Prév. pilote bord': ('arrival_pilot_boarding', lambda x: to_isoformat(x, dayfirst=True)),
        # time when pilot has boarded vessel
        'Pilote bord': ('arrival_pilot_onboard', lambda x: to_isoformat(x, dayfirst=True)),
        'LH10': ('arrival_lh10', lambda x: to_isoformat(x, dayfirst=True)),
        # time when pilot has navigated vessel to berth
        'Entrée digues': ('arrival_at_dock', lambda x: to_isoformat(x, dayfirst=True)),
        'PVD': ignore_key('estimated time of departure'),
        'Sortie digues': ('departure', lambda x: to_isoformat(x, dayfirst=True)),
        'Ordre': ignore_key('order in which vessel is docked at a berth'),
        'Quai': ('berth', lambda x: BERTH_TO_INSTALLATION_MAPPING.get(x)),
        'Terminal': ignore_key('terminal'),
        'Poste': ignore_key('post'),
        'Accostage': ('berthed', lambda x: to_isoformat(x, dayfirst=True)),
        'Cmd ferme': ignore_key('cmd closes'),
        'Appareillage': ignore_key('equipment'),
        'Provenance': ignore_key('origin'),
        'Dernier touché': ignore_key('last_zone'),
        'Prochain touché': ignore_key('next zone; TODO we should use it to compute next ETAs'),
        'Destination': ignore_key('destination'),
        'Nom Bateau': ('vessel_name', may_strip),
        'N° de Lloyd': ('vessel_imo', may_strip),
        'Largeur': ignore_key('vessel beam'),
        'Longueur': ('vessel_length', normalize_numeric),
        'Pavillon': ('vessel_flag', normalize_flag),
        'Type Navire': ('vessel_type', normalize_vessel_type),
        'Indicatif': ignore_key('indicative'),
        'Tirant d\'eau max': ignore_key('vessel max draught'),
        'Jauge brute': ('vessel_gt', normalize_numeric),
        'Jauge net': ignore_key('vessel nett tonnage'),
        'Port en lourd': ('vessel_dwt', normalize_numeric),
        'Nb TEU': ignore_key('TEU; only relevant for containers'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_numeric(raw_data):
    """Normalize numeric data such as vessel length, dwt.

    Args:
        raw_data (str):

    Returns:
        Optional[int]:

    """
    return may_apply(raw_data, float, int) if raw_data != '0' else None


def normalize_flag(raw_flag):
    """Normalize flag.

    Args:
        raw_flag (str): containing flag code and flag name

    Returns:
        str:

    """
    iso3166_code, _, name = raw_flag.partition('-')
    return may_strip(name)


def normalize_vessel_type(raw_type):
    """Normalize vessel type

    Args:
        raw_type (str):

    Returns:
        Optional[str]:

    """
    if not raw_type or any(raw_type.startswith(alias) for alias in VESSEL_TYPE_BLACKLIST):
        return None

    return VESSEL_TYPE_MAPPING.get(raw_type, raw_type)


def choose_arrival_and_eta(item):
    """Find the most accurate eta/arrival date.

    Args:
        item:

    Returns:
        Tuple[Optional[str], Optional[str]]: tuple of (arrival, eta) ISO8601 timestamps

    """
    _arrival_at_anchorage = item.pop('arrival_at_anchorage')
    _arrival_pilot_boarding = item.pop('arrival_pilot_boarding')
    _arrival_pilot_onboard = item.pop('arrival_pilot_onboard')
    _arrival_lh10 = item.pop('arrival_lh10')
    _arrival_at_dock = item.pop('arrival_at_dock')
    arrival = (
        _arrival_at_dock
        or _arrival_lh10
        or _arrival_pilot_onboard
        or _arrival_pilot_boarding
        or _arrival_at_anchorage
    )

    _eta_rough = item.pop('eta_rough', None)
    _eta_precise = item.pop('eta_precise', None)
    _eta_contact = item.pop('eta_radio_contact', None)
    eta = _eta_contact or _eta_precise or _eta_rough

    return arrival, eta
