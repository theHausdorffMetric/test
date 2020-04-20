import logging
from typing import List, Dict, Callable
from parse import compile

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.cargo import Cargo
from kp_scrapers.models.utils import validate_item

logger = logging.getLogger(__name__)

tf_type = Callable[[str], str]

def remove_endline_dec(g: tf_type) -> tf_type:
    def f(s: str) -> str:
        s = s.replace("\n", " ")
        return g(s)

    return f


def remove_endline(s):
    s = s.replace("\n", " ")
    return s


def portcall_mapping():
    return {
        'Vessel Name': ('vessel_name', remove_endline_dec(may_strip)),
        'arrival': ('arrival', None),
        'berthed': ('berthed', None),
        'cargoes':('cargoes',None),
        'Berth Name': ('berth', remove_endline),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
    }

def normalize_movement(s):
    if s=="D":
        return "discharge"
    elif s=="L":
        return "load"
    return None

pattern = compile("{}-{}/{:f}")

@validate_item(Cargo, normalize=True, strict=False)
def process_cargo_item(data:str) -> Dict[str, str]:
    try:
        data=data.strip()
        result = pattern.parse(data)

        return {
            'movement':normalize_movement(result[0]),
            'product':result[1],
            'volume':str(result[2]),
            'volume_unit':'meter',
        }
    except :
        print(data)
        assert (False)
        # movement = StringType(metadata='build year of vessel', validators=[is_valid_cargo_movement])
    # product = StringType(metadata='name of cargo')
    # volume = StringType(metadata='absolute quantity of cargo', validators=[is_valid_numeric])
    # volume_unit = StringType(
    #     metadata='unique numeric identifier of vessel', choices=[value for _, value in Unit]
    # )

def process_cargo_list_items(data:str) -> List[Dict[str,str]]:
    data = remove_endline(data)
    l = data.split(",")
    return [process_cargo_item(i) for i in l if i]


@validate_item(PortCall, normalize=True, strict=True)
def process_item(raw_item: Dict[str, str]) -> Dict[str, str]:
    """Transform raw item into a usable event.

    """
    table_label = raw_item['table_label']
    if table_label == 'VESSELS EXPECTED TO ARRIVE PORT':
        eta = raw_item.pop('ETA')
        raw_item['arrival'] = to_isoformat(eta, dayfirst=True)
        raw_item['cargoes'] = process_cargo_list_items(raw_item['Activity / Cargo / Quantity'])
    elif table_label == 'VESSELS AT BERTH FOR  LOADING':
        etc = raw_item.pop('Arrival Date')
        raw_item['berthed'] = to_isoformat(etc, dayfirst=True)
        raw_item['cargoes'] = process_cargo_list_items(raw_item['Activity / Cargo / Quantity'])
    elif table_label == 'VESSELS AT BERTH FOR  DISCHARGE':
        etc = raw_item.pop('Berth Date')
        raw_item['berthed'] = to_isoformat(etc, dayfirst=True)
        raw_item['cargoes'] = process_cargo_list_items(raw_item['Activity / Cargo / Quantity'])
    elif table_label == 'VESSELS WAITING FOR BERTH':
        etc = raw_item.pop('Arrival Date')
        raw_item['arrival'] = to_isoformat(etc, dayfirst=True)
        raw_item['cargoes'] = process_cargo_list_items(raw_item['Activity / Cargo / Quantity'])
    else:
        logger.error(f"unexpected label {table_label}")

    item = map_keys(raw_item, portcall_mapping())
    item['vessel'] = {'name': item.pop('vessel_name')}

    return item
