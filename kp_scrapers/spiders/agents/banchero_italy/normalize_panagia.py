import datetime as dt
import logging
import re
from typing import Any, Dict, Optional

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MOVEMENT_MAPPING = {'load.': 'load', 'load': 'load', 'disch': 'discharge', 'disch.': 'discharge'}


VAGUE_TIME_MAPPING = {
    'am': '0900',
    'eam': '0300',
    'pm': '1200',
    'morn': '0900',
    'morning': '0900',
    'noon': '1200',
    'evening': '1800',
    'ev': '1800',
}


@validate_item(CargoMovement, normalize=True, strict=True)
def process_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    item = map_keys(raw_item, field_mapping())
    # ignore empty vessels
    if not item['vessel']['name']:
        return

    # normalize dates
    if item.get('berthed'):
        item['berthed'] = normalize_date_time(item['berthed'], item['reported_date'])

    if item.get('arrival'):
        item['arrival'] = normalize_date_time(item['arrival'], item['reported_date'])

    if item.get('eta'):
        item['eta'] = normalize_date_time(item['eta'], item['reported_date'])

    if item.get('departure'):
        item['departure'] = normalize_date_time(item['departure'], item['reported_date'])

    # discard null products
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': item.pop('cargo_movement', None),
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.kilotons,
    }

    return item


def field_mapping() -> Dict[str, tuple]:
    return {
        'reported_date': ('reported_date', None),
        'provider_name': ('provider_name', None),
        'port_name': ('port_name', None),
        'berth': ('berth', None),
        'vessel': ('vessel', lambda x: {'name': x}),
        'eta': ('eta', lambda x: clean_datetimes(x)),
        'arrival': ('arrival', lambda x: clean_datetimes(x)),
        'berthed': ('berthed', lambda x: clean_datetimes(x)),
        'departure': ('departure', lambda x: clean_datetimes(x)),
        'movement': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x.lower(), x.lower())),
        'volume': ('cargo_volume', None),
        'product': ('cargo_product', None),
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


def clean_datetimes(raw_dt: str) -> Optional[str]:
    list_of_values = re.split(r'[ \/]', raw_dt)
    if len(list_of_values) > 1:
        list_of_values[1] = VAGUE_TIME_MAPPING.get(may_strip(list_of_values[1].lower()), '')
        return ''.join(list_of_values)

    return (
        raw_dt.lower().replace('hrs', '')
        if raw_dt.lower() and 'n.a' not in raw_dt.lower()
        else None
    )
