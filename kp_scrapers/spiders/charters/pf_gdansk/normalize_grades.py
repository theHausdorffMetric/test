import logging
import re
from typing import Any, Dict

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.charters.pf_gdansk import spider


logger = logging.getLogger(__name__)

VOLUME_UNIT_MAPPING = {'cbm': Unit.cubic_meter, 'mtons': Unit.tons, 'mts': Unit.tons}


@validate_item(CargoMovement, normalize=True, strict=True)
def process_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw item into an portcall event.
    Args:
        raw_item (Dict[str, str]):
    Returns:
        Dict[str, Any]: normalized cargo movement item
    """
    item = map_keys(raw_item, field_mapping())

    item['vessel'] = search_vessel_name(item['raw_string'])

    if not item['vessel'] or item['vessel']['name'].replace(' ', '') == 'tbn':
        return

    # build cargo sub model
    item['cargo'] = search_cargo_details(item['raw_string'], item.pop('cargo_movement'))
    item['eta'] = search_eta(item['raw_string'], item['reported_date'])
    item['berthed'] = search_etb(item['raw_string'], item['reported_date'])
    item['departure'] = search_etd(item['raw_string'], item['reported_date'])

    if (
        not item.get('arrival')
        and not item.get('berthed')
        and not item.get('departure')
        and not item.get('eta')
    ):
        spider.MISSING_ROWS.append(item['raw_string'])
        return

    item.pop('raw_string')

    return item


def field_mapping() -> Dict[str, tuple]:
    return {
        'raw_string': ('raw_string', None),
        'cargo_movement': ('cargo_movement', None),
        'volume': ('volume', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x)),
    }


def search_vessel_name(raw_string: str) -> Dict[str, str]:
    vessel_name = re.search(r'm\/t\W+(?P<vessel_name>[A-z\s0-9]+)', raw_string)
    if vessel_name:
        return {'name': may_strip(vessel_name.group('vessel_name'))}

    logger.debug(f'unable to locate vessel {raw_string}')
    return None


def search_cargo_details(raw_string: str, cargo_movement: str) -> Dict[str, str]:
    cargo = re.search(
        r'approx\W+'
        r'(?P<cargo_volume_unit>[A-z0-9,]+\s[A-z]+\s)(?:of)?'
        r'(?P<cargo_product>[A-z\s]+)\sfrom',
        raw_string,
    )
    if cargo:
        volume_unit = cargo.group('cargo_volume_unit')
        volume, unit = may_strip(volume_unit).split(' ')
        return {
            'product': may_strip(cargo.group('cargo_product')),
            'volume': may_strip(volume.replace(',', '')),
            'volume_unit': VOLUME_UNIT_MAPPING.get(may_strip(unit.lower()), None),
            'movement': cargo_movement,
        }
    logger.debug(f'unable to locate cargo {raw_string}')
    return None


def search_eta(raw_string: str, reported_date: str) -> str:
    eta = re.search(
        r'e\s?t\s?a\s[A-z\s]+\s'
        r'(?P<eta>\d{1,2}[A-z]+\s[A-z]+)\s'
        r'(?P<eta_year>\d{2,4})?(?:[|\s])(?:at\s)?'
        r'(?P<eta_time>\d{1,2}:\d{1,2})?',  # noqa
        raw_string,
    )
    if eta:
        if eta.group('eta_year'):
            a_day, a_month = eta.group('eta').split(' ')
            a_year = _get_lay_can_year(parse_date(f'{a_month}-01').month, reported_date)
            return (
                parse_date(f"{a_year}-{a_month}-{a_day} {eta.group('eta_time')}").isoformat()
                if eta.group('eta_time')
                else parse_date(f"{a_year}-{a_month}-{a_day}").isoformat()
            )
        else:
            return (
                parse_date(f"{eta.group('eta')} {eta.group('eta_time')}").isoformat()
                if eta.group('eta_time')
                else parse_date(f"{eta.group('eta')}").isoformat()
            )
    logger.debug(f'unable to locate eta {raw_string}')
    return None


def search_etb(raw_string: str, reported_date: str) -> str:
    etb = re.search(
        r'e\s?t\s?b\s(?P<etb>[0-9A-z]+\s[A-z]+)\s(?:at\s)?(?P<etb_time>[0-9:]+)?\s', raw_string
    )
    if etb:
        b_day, b_month = etb.group('etb').split(' ')
        b_year = _get_lay_can_year(parse_date(f'{b_month}-01').month, reported_date)
        return parse_date(f"{b_year}-{b_month}-{b_day} {etb.group('etb_time')}").isoformat()
    logger.debug(f'unable to locate etb {raw_string}')
    return None


def search_etd(raw_string: str, reported_date: str) -> str:
    etd = re.search(
        r'(?:e t c \/ e t s|etc\/ets|e t c|etc|e t s|ets)\s'
        r'(?P<etd>[0-9A-z]+\s[A-z]+)'
        r'(?P<etd_year>\s\d{2,4})?\s(?:at\s)?'
        r'(?P<etd_time>[0-9]+:[0-9]+)?\shrs',
        raw_string,
    )
    if etd:
        if etd.group('etd_year'):
            d_day, d_month = may_strip(etd.group('etd')).split(' ')
            d_year = _get_lay_can_year(parse_date(f'{d_month}-01').month, reported_date)
            return (
                parse_date(f"{d_year}-{d_month}-{d_day} {etd.group('etd_time')}").isoformat()
                if etd.group('etd_time')
                else parse_date(f"{d_year}-{d_month}-{d_day}").isoformat()
            )
        else:
            return (
                parse_date(f"{etd.group('etd')} {etd.group('etd_time')}").isoformat()
                if etd.group('etd_time')
                else parse_date(f"{etd.group('etd')}").isoformat()
            )
    logger.debug(f'unable to locate etd {raw_string}')
    return None


def _get_lay_can_year(month: str, reported: str) -> int:
    year = parse_date(reported).year
    if ('12' == month or '11' == month) and 'Jan' in reported:
        year -= 1
    if ('01' == month or '1' == month) and 'Dec' in reported:
        year += 1
    return year
