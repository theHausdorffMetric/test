import datetime as dt
import logging
from typing import Any, Dict, List, Optional, Tuple

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(CargoMovement, normalize=True, strict=True)
def process_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw item to cargo movement model."""
    item = map_keys(raw_item, field_mapping())

    if not item['vessel']['name']:
        return

    item['berthed'] = normalize_date_time(item['berthed'], item['reported_date'])

    item['departure'] = normalize_date_time(item['departure'], item['reported_date'])

    for f_cargo in split_cargo_volume(item.pop('cargo_product'), item.pop('cargo_volume')):
        if not f_cargo:
            continue

        item['cargo'] = {
            'product': f_cargo[0],
            'movement': None,
            'volume': f_cargo[1],
            'volume_unit': Unit.tons,
            'buyer': {'name': item.pop('cargo_buyer', None)} if item.get('cargo_buyer') else None,
        }

        # discard null products
        if not item['cargo']['product']:
            continue
        yield item


def field_mapping() -> Dict[str, tuple]:
    return {
        'Jetties': ('berth', None),
        'Vessel': ('vessel', lambda x: {'name': x}),
        'Arr/NOR': ('arrival', None),
        'ETA': ('eta', None),
        'ETB': ('berthed', None),
        'ETD': ('departure', None),
        'Cargo': ('cargo_product', None),
        'Quantity': ('cargo_volume', lambda x: x.replace('.', '') if x else None),
        'Loading port': ignore_key('loading port'),
        'Receivers/Charterers': ('cargo_buyer', None),
        'Destination': ignore_key('Destination'),
        'Berth': ('berth', None),
        'reported_date': ('reported_date', None),
        'provider_name': ('provider_name', None),
        'port_name': ('port_name', None),
    }


def normalize_date_time(raw_datetime: str, raw_reported_date: str) -> Optional[str]:
    # get year and month from reported date
    rpt_year = parse_date(raw_reported_date).year
    rpt_month = parse_date(raw_reported_date).month

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
        try:
            if (f_date - parse_date(raw_reported_date)).days < -15:
                f_date = dt.datetime(
                    year=int(rpt_year), month=int(rpt_month), day=int(_day)
                ) + relativedelta(months=1)

            if (f_date - parse_date(raw_reported_date)).days > 15:
                f_date = dt.datetime(
                    year=int(rpt_year), month=int(rpt_month), day=int(_day)
                ) + relativedelta(months=11)
        except Exception:
            pass

        return dt.datetime.combine(f_date, f_time).isoformat()

    logger.warning('unable to parse %s', raw_datetime)
    return None


def normalize_time(raw_time_string: str) -> dt.time:
    if raw_time_string and raw_time_string.isdigit():
        _hour = int(raw_time_string[:2]) if int(raw_time_string[:2]) != 24 else 0
        _min = int(raw_time_string[2:])
        return dt.time(_hour, _min)

    return dt.time(hour=0)


def split_cargo_volume(raw_cargo_information: str, raw_volume: str) -> List[Tuple[str]]:
    """split cargo, movement and volume"""
    if raw_cargo_information:
        list_of_products = raw_cargo_information.split('/')

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
