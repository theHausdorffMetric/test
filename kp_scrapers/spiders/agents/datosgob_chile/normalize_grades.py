import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

STRING_BLACKLIST = ['N/A', 'TBA', 'TBC']


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, grades_mapping())

    # discard items without vessel names
    vessel_name = item.pop('vessel_name')
    if not vessel_name:
        logger.warning(f'Item has no vessel name, discarding:\n{item}')
        return

    # discard items without valid dates
    if not (item.get('arrival')):
        logger.warning(f'Item has no valid portcall dates, discarding:\n{item}')
        return

    # build Vessel sub-model
    item['vessel'] = {'name': vessel_name, 'imo': item.pop('vessel_imo', None)}

    # build Cargo sub-model
    product = item.pop('cargo_product', [])
    movement = item.pop('cargo_movement', None)

    # associate volumes to their respective products
    volume, unit = normalize_volume_unit(item.pop('cargo_volume', None))

    # discard items without valid cargo volume
    if not volume or volume < 1:
        logger.warning(f'Item has no valid cargo volume, discarding:\n{item}')
        return

    item['cargo'] = {
        'product': product,
        'movement': movement,
        'volume': try_apply(volume, float, int),
        'volume_unit': unit,
    }

    yield item


def grades_mapping():
    return {
        'arrival': ('arrival', normalize_pc_date),
        'cargo_product': ('cargo_product', normalize_product),
        'cargo_movement': ('cargo_movement', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'cargo_volume': ('cargo_volume', None),
        'reported_date': ('reported_date', None),
        'vessel_name': ('vessel_name', None),
    }


def normalize_pc_date(date_str):
    """Cleanup ISO8601 portcall-related date.

    Args:
        raw_date (str):

    Returns:
        str | None: ISO8601-formatted date string

    Examples:
        >>> normalize_pc_date('2018-07-28T23:45:00')
        '2018-07-28T23:45:00'
        >>> normalize_pc_date('N/A')
        >>> normalize_pc_date(' ')

    """
    if not may_strip(date_str) or any(sub in date_str for sub in STRING_BLACKLIST):
        return None

    return to_isoformat(date_str)


def normalize_product(raw_product):
    """Normalize a raw cargo into a well-known product.
    Args:
        raw_product (str):
    Returns:
        str:
    Examples:
        >>> normalize_product('N/A')
        []
        >>> normalize_product('SINCODIGO~ULSD~HHH~UUU')
        'ULSD'
        >>> normalize_product('VGO')
        'VGO'
    """
    if not raw_product or raw_product in STRING_BLACKLIST:
        return []

    if re.compile('ULSD').search(raw_product):
        return 'ULSD'
    if re.compile('DIESEL').search(raw_product):
        return 'DIESEL'
    if re.compile('JET A-1').search(raw_product):
        return 'JET A-1'
    if re.compile('KEROSENE').search(raw_product):
        return 'KEROSENE'
    if re.compile('PETROLEO CRUDO').search(raw_product):
        grade_type = re.compile(r'F~(?P<grade>.*)(?=~)').search(raw_product)
        if grade_type:
            return grade_type.group('grade')

    return raw_product


def normalize_volume_unit(raw_volume):
    """Normalize a raw volume into a float quantity and a unit.
       Args:
           raw_volume (str):
       Returns:
            float ,str:
       Examples:
           >>> normalize_volume_unit('N/A')
           (None, None)
           >>> normalize_volume_unit('5.6 LIBRAS')
           (None, None)
           >>> normalize_volume_unit('42.0 GALONES')
           (1.0, 'barrel')
           >>> normalize_volume_unit('42.0 BARRILES')
           (42.0, 'barrel')
           >>> normalize_volume_unit('42')
           (42.0, 'barrel')
       """
    if not raw_volume or raw_volume in STRING_BLACKLIST or re.compile('LIBRAS').search(raw_volume):
        return None, None

    volume_match = re.match(r'\d+(|\.)\d+', raw_volume)
    if volume_match:
        volume = volume_match.group(0)
        if re.compile('GALON').search(raw_volume):
            return float(volume) / 42, Unit.barrel
        else:
            return float(volume), Unit.barrel
    else:
        return None, None
