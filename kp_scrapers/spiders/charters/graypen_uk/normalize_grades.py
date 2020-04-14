import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


UNIT_MAPPING = {
    'CBM': Unit.cubic_meter,
    'KB': Unit.kilobarrel,
    'MT': Unit.tons,
    'MB': Unit.megabarrel,
}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]: normalized cargo movement item

    """
    item = map_keys(raw_item, field_mapping())

    # build proper vessel model
    item['vessel'] = {'name': item.pop('vessel_name', None), 'imo': item.pop('vessel_imo', None)}

    # build proper cargo model
    volume = item.pop('cargo_volume', None)
    volume_unit = item.pop('cargo_volume_unit', None)
    platform = item.pop('platform', None)

    if volume_unit == Unit.kilobarrel:
        volume = volume * 1000
        volume_unit = Unit.barrel

    # LPG analyst request to change product, Ilya
    if platform == 'lpg' and 'raffinate' in item['cargo_product'].lower():
        item['cargo_product'] = 'olefins'

    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': item.pop('cargo_movement', None),
        'volume': try_apply(volume, int, str),
        'volume_unit': volume_unit,
    }

    return item


def field_mapping():
    return {
        'Berth': (ignore_key('not required for now')),
        'Charterer': (ignore_key('not required for cargo movement')),
        'Dates': ('arrival', normalize_matching_date),
        'Grade': ('cargo_product', None),
        'IMO': ('vessel_imo', lambda x: try_apply(x, int, str)),
        'Last Port': (ignore_key('not required for now')),
        'movement': ('cargo_movement', None),
        'Next Port': (ignore_key('not required for now')),
        'Port': ('port_name', None),
        'provider_name': ('provider_name', None),
        'QTY': ('cargo_volume', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
        'Supp/Rcvr': (ignore_key('not required for now')),
        'Unit': ('cargo_volume_unit', lambda x: UNIT_MAPPING.get(x)),
        'Vessel': ('vessel_name', None),
        'platform': ('platform', None),
    }


def normalize_matching_date(raw_date):
    """Normalize matching dates to an ISO-8601 timestamp.

    Args:
        raw_date (str): raw date string

    Returns:
        str: ISO-8601 formatted date string

    Examples:
        >>> normalize_matching_date('11.07.18')
        '2018-07-11T00:00:00'
        >>> normalize_matching_date('12.06.18 08:00')
        '2018-06-12T08:00:00'
        >>> normalize_matching_date('SLD: 04.06.18')
        '2018-06-04T00:00:00'
        >>> normalize_matching_date('O/B')
        >>> normalize_matching_date('ANCH')

    """
    date_match = re.match(r'.*(\d{2}\.\d{2}\.\d{2}(?: \d{2}:\d{2})?)', raw_date)
    return to_isoformat(date_match.group(1), dayfirst=True) if date_match else None
