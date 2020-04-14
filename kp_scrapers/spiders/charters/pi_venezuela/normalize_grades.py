import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


PRODUCT_MAPPING = {
    'ME': 'Merey 16',
    'MS': 'Mesa 30',
    'DCO': 'Diluted Crude Oil',
    'PZH': 'PetroZuataHeavy',
    'SBH': 'Hamaca Blend',
    'MO': 'Morichal 16',
    'Z-300': 'Zuata 300',
    'SB': 'Santa Barbara',
    'GAN': 'Natural Gasoline',
    'LVN': 'Light Virgin Naphtha',
}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, grades_mapping(), skip_missing=True)

    item['port_name'] = 'Port of Jose'

    # if date is blank assume reported date
    if not item.get('departure'):
        item['departure'] = item['reported_date']

    volume = item['cargo_volume'] if item.get('cargo_volume') else item['cargo_volume_nominated']
    movement = 'load' if 'export' in item['sheet_name'] else 'discharge'
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': movement,
        'volume': volume,
        'volume_unit': Unit.kilobarrel,
    }

    # discard irrelevant fields
    for field in ('sheet_name', 'cargo_volume_nominated', 'cargo_volume'):
        item.pop(field, None)

    return item


def grades_mapping():
    return {
        'Date': ('departure', to_isoformat),
        'Dock': ('berth', None),
        'Vessel': ('vessel', lambda x: {'name': x}),
        'Charterer': ignore_key('charters spider'),
        'Grade': ('cargo_product', lambda x: PRODUCT_MAPPING.get(x.upper(), x)),
        'Qty Nominated': ('cargo_volume_nominated', lambda x: try_apply(x, float, int, str)),
        'Qty Loaded': ('cargo_volume', lambda x: try_apply(x, float, int, str)),
        'Loading Rate': ignore_key('irrelevant'),
        'Status': ignore_key('irrelevant'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'sheet_name': ('sheet_name', None),
    }
