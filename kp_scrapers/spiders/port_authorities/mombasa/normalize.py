import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


INSTALLATION_MAPPING = {'SOT': 'Shimanzi', 'KOT': 'Kipevu', 'MBK': 'Mbaraki'}

MOVEMENT_MAPPING = {'L': 'load', 'D': 'discharge'}

PRODUCT_BLACKLIST = ['-F', 'MTS', 'VEHICLES', 'EQUIPMENT']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # build vessel sub model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'call_sign': item.pop('call_sign', None),
        'length': item.pop('vessel_length', None),
    }

    # build cargoes sub model and extract installation info if any
    item['cargoes'], item['installation'] = normalize_cargoes(
        item['vessel']['name'],
        item.pop('disch_volume', None),
        item.pop('load_volume', None),
        item.pop('remarks', None),
    )

    if not item['cargoes']:
        return

    return item


def portcall_mapping():
    return {
        # ships expected tables
        'VESSEL NAME': ('vessel_name', normalize_vessel_name),
        'VES.SCHEDULE': (ignore_key('vessel schedule')),
        'CALL SIGN': ('call_sign', None),
        'VOYAGE NO.': (ignore_key('voyage number')),
        'ETA': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'LOA': ('vessel_length', normalize_numeric_value),
        'DRAFT': (ignore_key('draught')),
        'AGENT': ('shipping_agent', None),
        'DISCH': ('disch_volume', normalize_numeric_value),
        'LOAD': ('load_volume', normalize_numeric_value),
        'FBW': (ignore_key('unknown')),
        'BER': (ignore_key('unknown')),
        'BOOKED': (ignore_key('booked date')),
        'REMARKS': ('remarks', None),
        # waiters
        '0': ('arrival', lambda x: to_isoformat(x, dayfirst=True)),
        '1': ('vessel_name', None),
        '2': ('vessel_length', normalize_numeric_value),
        '3': (ignore_key('draught')),
        '4': (ignore_key('unknown')),
        '5': ('remarks', None),
        # meta info
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_numeric_value(raw_val):
    """Transform values (length, gross tonnage)

    Args:
        raw_val: value in string

    Returns:

    """
    return try_apply(raw_val, float, int)


def normalize_date(raw_date):
    """Normalize reported date and eta date, both day first.

    Examples:
        >>> normalize_date('15/04/2019 1600')
        '2019-04-15T16:00:00'
        >>> normalize_date('15- APRIL-2019')
        '2019-04-15T00:00:00'

    Args:
        raw_date:

    Returns:
        str: date in ISO 8601 format

    """
    return parse_date(raw_date).isoformat()


def normalize_cargoes(vessel, disch_volume, load_volume, remarks):
    """Normalize cargo.

    Args:
        vessel: vessel name for ref
        disch_volume:
        load_volume:
        remarks:

    Returns:
        Dict:

    """
    cargo, _, raw_installation = remarks.partition('@')
    installation = get_installation(raw_installation)

    if any(alias in cargo for alias in PRODUCT_BLACKLIST):
        return None, installation

    cargoes = []
    # waiters field
    if disch_volume is None and load_volume is None:
        pattern = r'([L|D]?)\s?(\d*)\s?([A-Za-z\'\s]+)([\/&]?)'
        ref_movement = None
        for group in re.findall(pattern, cargo):
            movement = MOVEMENT_MAPPING.get(group[0])
            volume = normalize_numeric_value(group[1])
            product = normalize_product_name(group[2])
            # movement could be empty, in this case, will reference previous movement
            ref_movement = movement or ref_movement

            # only when product and volume both present, it's in the format of regex pattern, we
            # are confident that the info is correct.
            if not (product and volume):
                continue

            cargoes.append(
                {
                    'product': product,
                    'movement': ref_movement,
                    'volume': volume,
                    'volume_unit': Unit.tons,
                }
            )
        if not cargoes:
            logger.error(f'could not process cargo from remarks: {remarks} for vessel {vessel}')
    else:
        if cargo.startswith('D ') or cargo.startswith('L '):
            cargo = cargo[2:]
        cargo = may_strip(cargo)
        if load_volume != 0:
            cargoes.append(
                {
                    'product': cargo,
                    'movement': 'load',
                    'volume': load_volume,
                    'volume_unit': Unit.tons,
                }
            )
        if disch_volume != 0:
            cargoes.append(
                {
                    'product': cargo,
                    'movement': 'discharge',
                    'volume': disch_volume,
                    'volume_unit': Unit.tons,
                }
            )
    return cargoes, installation


def normalize_product_name(raw_name):
    """Normalize product name.

    Args:
        raw_name:

    Returns:

    """
    return may_strip(raw_name[3:] if raw_name.startswith('MT ') else raw_name)


def get_installation(raw_str):
    """Get installation.

    Examples:
        >>> get_installation('KOT')
        'Kipevu'
        >>> get_installation('SOT')
        'Shimanzi'
        >>> get_installation('SOT/KOT')
        'Shimanzi'

    Args:
        raw_str: string contains installation info

    Returns:
        str:

    """
    for alias in INSTALLATION_MAPPING:
        if alias in raw_str:
            return INSTALLATION_MAPPING[alias]


def normalize_vessel_name(raw_name):
    """Normalize vessel name.

    Args:
        raw_name:

    Returns:

    """
    if raw_name.startswith('MT ') or raw_name.startswith('MV '):
        return raw_name[3:]

    return raw_name
