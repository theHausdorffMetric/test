import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MOVEMENT_MAPPING = {'C': 'load', 'S': 'discharge'}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    if not item['product_volume']:
        return

    item['vessel'] = {
        'name': item.pop('vessel_name', None),
        'flag_code': item.pop('vessel_flag', None),
    }

    item['cargo_volume'], item['cargo_product'] = normalize_vol_cargo(
        item.pop('product_volume', None)
    )

    if item['installation_movement']:
        item['cargo_movement'], item['installation'] = normalize_inst_movement(
            item.pop('installation_movement', None)
        )

    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': item.pop('cargo_movement', None),
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.tons,
    }

    return item


def field_mapping():
    return {
        'Data': ('berthed', to_isoformat),
        'Accosto': (ignore_key('irrelevant')),
        'Nome': ('vessel_name', None),
        'Bandiera': ('vessel_flag', None),
        'TSL': (ignore_key('irrelevant')),
        'Provenienza': (ignore_key('irrelevant')),
        'Operazioni': ('installation_movement', None),
        'Prodotti': ('product_volume', None),
        'Destinazione': (ignore_key('irrelevant')),
        'Agenzia': ('shipping_agent', None),
        'Note': (ignore_key('irrelevant')),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'port_name': ('port_name', None),
    }


def normalize_vol_cargo(raw_product_string):
    """Normalize volume and cargo information

    Examples:
        >>> normalize_vol_cargo('30000 GASOLIO')
        ('30000', 'GASOLIO')
        >>> normalize_vol_cargo('C+ISAB SUD')
        (None, None)

    Args:
        raw_product_string (str):

    Returns:

    """
    _match_product = re.match(r'([0-9]+)\s(.*)', may_strip(raw_product_string))

    if _match_product:
        _vol, _product = _match_product.groups()

        return _vol, _product

    logger.warning(f'unable to parse {raw_product_string}')
    return None, None


def normalize_inst_movement(raw_inst_movement):
    """Normalize installation and movement

    Examples:
        >>> normalize_inst_movement('C/ISAB SUD')
        ('load', 'ISAB SUD')
        >>> normalize_inst_movement('C+ISAB SUD')
        (None, None)

    Args:
        raw_inst_movement (str):

    Returns:

    """
    _match_ins_mov = re.match(r'([A-z])\/(.*)', may_strip(raw_inst_movement))

    if _match_ins_mov:
        _movement, _installation = _match_ins_mov.groups()

        return MOVEMENT_MAPPING.get(_movement, None), _installation

    logger.warning(f'unable to parse {raw_inst_movement}')
    return None, None
