import logging
import re
from typing import Any, Dict, List

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MOVEMENT_MAPPING = {'L': 'load', 'D': 'discharge'}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw item to Portcall model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, field_mapping())

    # ignore empty vessels
    if not item['vessel']['name']:
        return

    # yield individual items for multiple cargos
    if item['cargo_product']:
        for f_cargo in split_cargo_volume(item.pop('cargo_product')):
            # discard null products
            item['cargo'] = {
                'product': f_cargo,
                'movement': item.pop('cargo_movement', None),
                'volume': None,
                'volume_unit': None,
            }

            yield item


def field_mapping() -> Dict[str, tuple]:
    return {
        'reported_date': ('reported_date', None),
        'provider_name': ('provider_name', None),
        'port_name': ('port_name', None),
        'vessel': ('vessel', lambda x: {'name': x}),
        'eta': ('eta', lambda x: normalize_date_time(x)),
        'etb': ('berthed', lambda x: normalize_date_time(x)),
        'ets': ('departure', lambda x: normalize_date_time(x)),
        'berth': ('berth', may_strip),
        'movement': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(may_strip(x), None)),
        'product': ('cargo_product', None),
    }


def normalize_date_time(raw_datetime: str) -> str:
    if 'N/A' not in raw_datetime:
        return to_isoformat(raw_datetime, dayfirst=True)
    return None


def split_cargo_volume(raw_cargo_information: str) -> List[str]:
    if raw_cargo_information:
        return re.split(r'[\/\+]', raw_cargo_information)
