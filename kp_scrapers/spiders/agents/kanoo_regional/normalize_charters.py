import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


UNIT_MAPPING = {'Metric Ton (MT)': Unit.tons}

logger = logging.getLogger(__name__)


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """

    item = map_keys(raw_item, field_mapping())

    # for now we assume crude and liquid gas are only dedicated for loading
    # and Naphta,JetA1,Gasoil product as well
    # for the rest we don't know yet

    if item['cargo_type'] in {'CRUDE OIL', 'LIQUID GAS'} or item['cargo_product'] in {
        'NAPHTHA',
        'GASOIL',
        'JET A-1',
    }:
        item['movement'] = 'load'

    if 'movement' in item.keys() and item['movement'] == 'load':
        processed_item = process_exports_item(item)
    else:
        logger.info(
            f'Item is not processed yet because we lack information on type of movement: {raw_item}'
        )
        return None

    return processed_item


def field_mapping():
    return {
        'CALLNO': ignore_key('redundant'),
        'Charterer Name': ('charterer', lambda x: may_strip(x)),
        'Responsible Party': ignore_key('redundant'),
        'Owner - Operator (Principal Name)': ignore_key('redundant'),
        'Vessel': ('vessel_name', None),
        'Arrival': ('lay_can_start', None),
        'Departure': ('lay_can_end', None),
        'Cargo Type': ('cargo_type', None),
        'Commodity': ('cargo_product', None),
        'Cargo Description': ('provider_name', None),
        'Quantity(Cargo)': ('cargo_volume', lambda x: None if x == '' else x),
        'Measurement Unit': ('cargo_unit', lambda x: None if x == '' else UNIT_MAPPING.get(x, x)),
        'Country Name': ignore_key('redundant'),
        'Port Name': ('current_port', None),
        'Next Port Name': ('next_port', None),
        'Prev Port Name': ('prev_port', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: parse_date(x).strftime('%d %b %Y')),
    }


def process_exports_item(item):
    """Process export spot charters.

    Args:
        item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    return {
        'charterer': item['charterer'],
        'departure_zone': item['current_port'],
        'lay_can_start': item['lay_can_start'],
        'lay_can_end': item['lay_can_end'],
        'provider_name': item['provider_name'],
        'reported_date': item['reported_date'],
        'vessel': {'name': item['vessel_name']},
    }
