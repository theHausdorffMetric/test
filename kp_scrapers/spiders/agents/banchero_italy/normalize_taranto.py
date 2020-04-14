import datetime as dt
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

MOVEMENT_MAPPING = {
    'LOADING': 'load',
    'DISCHARGING': 'discharge',
}


@validate_item(CargoMovement, normalize=True, strict=True)
def process_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    item = map_keys(raw_item, field_mapping())

    # ignore empty vessels
    if not item['vessel']['name']:
        return

    # normalize dates
    if item.get('berthed'):
        try:
            item['berthed'] = to_isoformat(item['berthed'], dayfirst=True)
        except Exception:
            item['berthed'] = normalize_date_time(
                item['berthed'].replace('/', ''), item['reported_date']
            )

    if item.get('arrival'):
        try:
            item['arrival'] = to_isoformat(item['arrival'], dayfirst=True)
        except Exception:
            item['arrival'] = normalize_date_time(
                item['arrival'].replace('/', ''), item['reported_date']
            )
    if item.get('eta'):
        try:
            item['eta'] = to_isoformat(item['eta'], dayfirst=True)
        except Exception:
            item['eta'] = normalize_date_time(item['eta'].replace('/', ''), item['reported_date'])

    # get movement details
    if item.get('move_load') and 'X' in item.get('move_load'):
        movement = 'load'
    elif item.get('move_dis') and 'X' in item.get('move_dis'):
        movement = 'discharge'
    elif item.get('cargo_movement'):
        movement = MOVEMENT_MAPPING.get(item.pop('cargo_movement', ''))
    else:
        movement = None

    for col in ('move_load', 'move_dis'):
        item.pop(col, None)

    # yield individual items for multiple cargos
    if item['cargo_product']:
        for f_cargo in split_cargo_volume(item.pop('cargo_product'), item.pop('cargo_volume')):
            # discard null products
            item['cargo'] = {
                'product': f_cargo[0],
                'movement': movement,
                'volume': f_cargo[1],
                'volume_unit': Unit.tons,
            }
            if item.get('cargo').get('product', '') == 'TBN':
                continue

            yield item


def field_mapping() -> Dict[str, tuple]:
    return {
        'reported_date': ('reported_date', None),
        'provider_name': ('provider_name', None),
        'port_name': ('port_name', None),
        'MOTOR TANKER': ('vessel', lambda x: {'name': x}),
        'VESSEL': ('vessel', lambda x: {'name': x}),
        'ARRVD FM': ignore_key('arr from port'),
        'ARRIVE FM': ignore_key('arr from port'),
        'LOADING': ('move_load', None),
        'DISCHARGING': ('move_dis', None),
        'OPERATION': ('cargo_movement', None),
        'OPS': ('cargo_movement', None),
        'TIPE OF CGO': ('cargo_product', None),
        'TYPE OF CGO': ('cargo_product', None),
        'CARGO': ('cargo_product', None),
        'TONN': ('cargo_volume', lambda x: x.replace('.', '') if x else None),
        'QUANTITY': ('cargo_volume', lambda x: x.replace('.', '') if x else None),
        'ARRIVAL': ('arrival', None),
        'ARRIVAL TIME': ('arrival', None),
        'ETA': ('eta', None),
        'ATA': ('eta', None),
        'ETB/ETC/ETS': ('berthed', None),
        'ETC/ETS': ('berthed', None),
        'ETB': ('berthed', None),
        'ATB': ('berthed', None),
        'PIER': ('berth', None),
        'TERMINAL': ignore_key('terminal'),
        'LAST PORT': ignore_key('last port'),
        'NEXT PORT': ignore_key('next port'),
        'AGENT': ignore_key('agent'),
        'REMARK': ignore_key('remarks'),
    }


def normalize_date_time(raw_datetime: str, raw_reported_date: str) -> Optional[str]:
    # get year and month from reported date
    rpt_year = parse_date(raw_reported_date).year
    rpt_month = parse_date(raw_reported_date).month

    # handle vague time
    if 'AM' in raw_datetime:
        raw_datetime = raw_datetime.replace('AM', '0700')
    if 'PM' in raw_datetime:
        raw_datetime = raw_datetime.replace('PM', '1200')

    if raw_datetime.isdigit():
        string_length = len(raw_datetime)
        time = raw_datetime[-4:]
        _day = raw_datetime[: (string_length - 4)]

        f_time = normalize_time(time)

        # handle roll over dates
        try:
            f_date = dt.datetime(year=int(rpt_year), month=int(rpt_month), day=int(_day))
        except Exception:
            f_date = dt.datetime(year=int(rpt_year), month=int(rpt_month) - 1, day=int(_day))

        # to accomodate end of year parsing, prevent dates too old or far into
        # the future. 100 days was chosen as a gauge
        if (f_date - parse_date(raw_reported_date)).days < -15:
            f_date = dt.datetime(
                year=int(rpt_year), month=int(rpt_month), day=int(_day)
            ) + relativedelta(months=1)

        if (f_date - parse_date(raw_reported_date)).days > 15:
            f_date = dt.datetime(
                year=int(rpt_year), month=int(rpt_month), day=int(_day)
            ) + relativedelta(months=11)

        return dt.datetime.combine(f_date, f_time).isoformat()

    logger.warning('unable to parse %s', raw_datetime)
    return None


def normalize_time(raw_time_string: str) -> dt.time:
    if raw_time_string:
        if raw_time_string.isdigit():
            return dt.time(int(raw_time_string[:2]), int(raw_time_string[2:]))

    return dt.time(hour=0)


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
