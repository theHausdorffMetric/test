import datetime as dt
import logging
from typing import Any, Dict, Union

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.units import Unit


logger = logging.getLogger(__name__)


def process_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    item = map_keys(raw_item, item_mapping())

    # if the stock level is not set, scraped output is meaningless as Kpler platform displays 0
    if not item['level_o']:
        logger.debug("Stock level is not set, discarding as output is meaningless")
        return

    # if no forecast level, scraped output is meaningless as well
    if not item['output_forecast_o'] and not item['output_o']:
        logger.debug("Forecast and actual levels are not set, discarding as output is meaningless")
        return

    # use forecasted figures if actual output figures are not filled-in
    if not item['output_o']:
        logger.debug("Actual output level is not set, use forecast levels in-place")
        item['output_o'] = item['output_forecast_o']

    # from 2014-10-01 onwards, all LNG inventory is expressed in thousand cubic meters of LNG
    # before 2014-10-01, all LNG inventory is expressed in kilowatthour
    # we standardize to kilowatthour as the LNG market prefers to use this as a base unit
    item['unit'] = Unit.kilowatt_hour
    if parse_date(item['date'], dayfirst=False) >= dt.datetime(2014, 10, 1):
        item['level_o'] = _kwh_from_km3lng(item['level_o'])

    return item


def item_mapping():
    return {
        'Jour': ('date', _normalize_gas_day),
        'provider_name': ('provider_name', None),
        'Quantités allouées': ('output_o', _normalize_inventory_levels),
        'Quantités nominées': ('output_forecast_o', _normalize_inventory_levels),
        'reported_date': ('reported_date', None),
        'Stock GNL à 6h': ('level_o', _normalize_inventory_levels),
    }


def _normalize_inventory_levels(raw_level: str) -> int:
    """Normalize inventory levels given as numeric strings.

    Source will provide inventory levels as numeric strings,
    but with digit grouping using whitespaces as a thousands separator.

    TODO maybe this could be made generic

    Examples:
        >>> _normalize_inventory_levels('115 091 562')
        115091562
        >>> _normalize_inventory_levels('115091562')
        115091562
        >>> _normalize_inventory_levels('-')

    """
    raw_level = raw_level.replace(' ', '')
    return try_apply(raw_level, float, int)


def _normalize_gas_day(raw_date: str) -> str:
    """Normalize date string describing inventory levels.

    Source actually describes data as taken at 1800h daily, but this is not recorded
    on the data table, so we need to edit it in post.

    Examples:
        >>> _normalize_gas_day('13/03/2020')
        '2020-03-13T18:00:00'

    """
    gas_day = parse_date(raw_date, dayfirst=True, yearfirst=False)
    return (gas_day + dt.timedelta(hours=18)).isoformat()


def _kwh_from_km3lng(number: Union[int, float]) -> int:
    """Convert value of (1000 * m^3) of LNG into an energy value in kWh.

    TODO: Jean: refactor with unit modules

    Examples:
        >>> _kwh_from_km3lng(1)
        6857858
        >>> _kwh_from_km3lng(1.000)
        6857858
        >>> _kwh_from_km3lng('1')  # doctest:+IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        TypeError

    """
    if isinstance(number, str):
        raise TypeError("Cannot convert strings to kWh values")

    exp_ratio = 570
    _kwh_value = number * (10 ** 9 * exp_ratio) // (3412 * 24.36)
    return int(_kwh_value)
