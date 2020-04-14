import logging

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys, scale_to_thousand
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(CargoMovement, normalize=True, strict=True, log_level='error')
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]: normalized cargo movement item

    """
    item = map_keys(raw_item, field_mapping())

    # discard items with no date
    if not item.get('berthed') and not item.get('departure') and not item.get('eta'):
        return

    # build proper Cargo model
    buyer = item.pop('cargo_buyer', None)
    volume = item.pop('cargo_volume', None)
    units = Unit.tons if volume else None
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'discharge',
        'volume': volume,
        'volume_unit': units,
        'buyer': {'name': buyer} if buyer and buyer not in ['?'] else None,
    }
    if not item['cargo'].get('buyer') or not item['cargo'].get('buyer').get('name'):
        item['cargo'].pop('buyer')

    return item


def field_mapping():
    return {
        'disport': ('port_name', may_strip),
        'port': ignore_key('get from sheet name'),
        'date of report': ('reported_date', None),
        'vessel': ('vessel', lambda x: {'name': x}),
        'eta': ('eta', lambda x: x if may_strip(x) else None),
        'etb': ('berthed', lambda x: x if may_strip(x) else None),
        'etd': ('departure', lambda x: x if may_strip(x) else None),
        'shifting': ignore_key('shifting'),
        'berth': ('berth', lambda x: x.replace('.0', '') if may_strip(x) else None),
        'mt(x1000)': ('cargo_volume', lambda x: str(scale_to_thousand(x)) if x.isdigit() else None),
        'cargo': ('cargo_product', lambda x: may_strip(x.lower().replace('dischg', ''))),
        'cargo quantity': ignore_key('duplicate'),
        'loading port': ignore_key('load port'),
        'disport port': ignore_key('dis port'),
        'receiver': ('cargo_buyer', lambda x: may_strip(x) if x else None),
        'last port': ignore_key('last port'),
        'next port': ignore_key('next port'),
        'consignee/shipper': ignore_key('consignee/shipper'),
        'agent': ('shipping_agent', None),
        'remark': ignore_key('remark'),
        'port closure from': ignore_key('irrelevant'),
        'port closure to': ignore_key('irrelevant'),
        'fact': ignore_key('irrelevant'),
        'reason': ignore_key('irrelevant'),
        'bunker availability (yes/no)': ignore_key('irrelevant'),
        'provider_name': ('provider_name', None),
        'port_name': ('port_name', may_strip),
    }
