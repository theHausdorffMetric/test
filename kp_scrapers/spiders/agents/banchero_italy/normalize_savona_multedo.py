import logging
import re
from typing import Any, Dict, List, Tuple

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


UNIT_MAPPING = {'MT': Unit.tons, 'BBL': Unit.barrel}
DATE_TYPE = {
    'sailed': 'departure',
    'ets': 'departure',
    'arrived': 'arrival',
    'eta': 'arrival',
    'etb': 'berthed',
}


@validate_item(CargoMovement, normalize=True, strict=True)
def process_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    item = map_keys(raw_item, field_mapping())

    item['vessel'] = {'name': item.pop('vessel_name', None), 'imo': item.pop('vessel_imo', None)}

    if item['port_name'] == 'Genoa':
        _datetype, _date = extract_status_date(item.pop('potential_date', ''))
        item[_datetype] = _date

    movement = item.pop('cargo_movement', None)
    units = item.pop('cargo_units', None)

    # determine player type
    if movement and movement == 'load':
        player_type = 'seller'
    if movement and movement == 'discharge':
        player_type = 'buyer'

    player = item.pop('cargo_player', None)
    item.pop('potential_date', None)

    # yield individual items for multiple cargos
    if item['cargo_product']:
        for f_cargo in split_cargo_volume(item.pop('cargo_product'), item.pop('cargo_volume')):
            # discard null products
            item['cargo'] = {
                'product': f_cargo[0],
                'movement': movement,
                'volume': f_cargo[1],
                'volume_unit': units,
                player_type: {'name': player} if player else None,
            }
            if item.get('cargo').get('product', '') == 'TBN':
                continue

            yield item


def field_mapping() -> Dict[str, tuple]:
    return {
        'reported_date': ('reported_date', None),
        'provider_name': ('provider_name', None),
        'port_name': ('port_name', None),
        'TERMINAL': ('installation', None),
        'STATUS': ('potential_date', None),
        'VESSEL': ('vessel_name', None),
        'IMO NUMBER': ('vessel_imo', None),
        'PREVIOUS PORT': ignore_key('previous port'),
        'NEXT PORT': ignore_key('next port'),
        'LOAD/DISCHARGE': ('cargo_movement', lambda x: x.lower()),
        'GRADE GROUP': ignore_key('grade group'),
        'GRADE DETAIL': ('cargo_product', None),
        'QUANTITY': ('cargo_volume', lambda x: x.replace('.', '')),
        'CHARTERER': ignore_key('seems like it is always na'),
        'SUPPLIER': ('cargo_player', None),
        'BBL/MT': ('cargo_units', lambda x: UNIT_MAPPING.get(x, None)),
        'ETA': ('eta', lambda x: to_isoformat(x.replace('.', ':'), dayfirst=True)),
    }


def extract_status_date(status_date: str) -> Tuple[str, str]:
    _match = re.match(r'([A-z]+)\s(\d+\/\d+\/\d+)', may_strip(status_date))
    if _match:
        date_type, date = _match.groups()
        return DATE_TYPE.get(date_type.lower()), parse_date(date, dayfirst=True).isoformat()

    logger.warning('unable to extract date from {status_date}')
    return '', None


def split_cargo_volume(raw_cargo_information: str, raw_volume: str) -> List[Tuple[str]]:
    if raw_cargo_information:
        list_of_products = re.split(r'[\/\+]', raw_cargo_information)
        list_volume = re.split(r'[\/\+]', raw_volume)

        if len(list_of_products) == len(list_volume):
            return list(zip(list_of_products, list_volume))

        vol = (
            str(int(raw_volume) / len(list_of_products))
            if len(list_of_products) > 1
            else raw_volume
        )

        final_list = []
        for cargo in list_of_products:
            tuple_cargo = (cargo, vol)
            final_list.append(tuple_cargo)

        return final_list
    return None, None
