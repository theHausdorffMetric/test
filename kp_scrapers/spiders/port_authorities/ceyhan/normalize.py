from xlrd.xldate import xldate_as_datetime

from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into normalized item.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # discard portcalls with invalid arrival dates
    if not item.get('arrival'):
        return

    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'flag_name': item.pop('flag'),
        'gross_tonnage': item.pop('gross_tonnage'),
        'vessel_type': item.pop('vessel_type'),
    }

    item['cargoes'] = normalize_cargo(item)

    return item


def field_mapping():
    return {
        '0': ('vessel_name', may_strip),
        '1': ('flag', may_strip),
        '2': ('gross_tonnage', lambda x: try_apply(x, float, int)),
        '3': ('vessel_type', may_strip),
        '4': ('shipping_agent', may_strip),
        '5': ignore_key('ignore transit'),
        '6': ignore_key('from country'),
        '7': ('arrival', lambda x: xldate_as_datetime(x, 0) if isinstance(x, float) else None),
        '8': ignore_key('next_zone, bacause ceyhan is not up to date'),
        '9': ('departure', lambda x: xldate_as_datetime(x, 0) if isinstance(x, float) else None),
        '10': ('evacuation_type', None),
        '11': ('evacuation_tonnage', lambda x: try_apply(x, float, int)),
        '12': ignore_key('discharge at port'),
        '13': ('loading_type', None),
        '14': ('loading_tonnage', lambda x: try_apply(x, float, int)),
        '15': ignore_key('loading port'),
        # meta fields
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_cargo(item):
    """Normalize cargo.

    Args:
        item:

    Returns:

    """
    cargoes = []
    d_product = item.pop('evacuation_type')
    d_volume = item.pop('evacuation_tonnage')
    if d_product:
        cargoes.append(
            {
                'product': d_product,
                'movement': 'discharge',
                'volume': str(d_volume),
                'volume_unit': Unit.tons,
            }
        )

    l_product = item.pop('loading_type')
    l_volume = item.pop('loading_tonnage')
    if l_product:
        cargoes.append(
            {
                'product': l_product,
                'movement': 'discharge',
                'volume': str(l_volume),
                'volume_unit': Unit.tons,
            }
        )
    return cargoes
