import datetime as dt
import logging
import re
from typing import Any, Dict

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.services import kp_api
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.charters.pf_gdansk import spider


logger = logging.getLogger(__name__)

VOLUME_UNIT_MAPPING = {'cbm': Unit.cubic_meter, 'mtons': Unit.tons, 'mts': Unit.tons}


@validate_item(SpotCharter, normalize=True, strict=True)
def process_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    item = map_keys(raw_item, field_mapping())

    item['vessel'] = search_vessel_name(item['raw_string'])

    if not item['vessel'] or item['vessel']['name'].replace(' ', '') == 'tbn':
        return

    # build cargo sub model
    item['cargo'] = search_cargo_details(item['raw_string'])
    item['eta'] = search_eta(item['raw_string'], item['reported_date'])
    item['berthed'] = search_etb(item['raw_string'], item['reported_date'])
    item['departure'] = search_etd(item['raw_string'], item['reported_date'])
    # since the source doesn't provide lay_can information,
    # we need to use Kp_api to get the trades info and from that we get lay_can dates
    for date_col in ('departure', 'eta', 'arrival', 'berthed', 'cargo_movement'):
        if item.get(date_col):
            _trade = None
            # get trade from either oil or cpp platform
            for platform in ('oil', 'cpp'):
                _trade = kp_api.get_session(platform, recreate=True).get_import_trade(
                    vessel=item['vessel']['name'],
                    origin='',
                    dest=item.get('port_name'),
                    end_date=item.get(date_col),
                )
                if _trade:
                    break
            post_process_import_item(item, _trade)
            break
        else:
            spider.MISSING_ROWS.append(item['raw_string'])
            return

    for col in (
        'berthed',
        'eta',
        'departure',
        'berthed',
        'raw_string',
        'port_name',
        'cargo_movement',
    ):
        item.pop(col, None)

    return item


def field_mapping() -> Dict[str, tuple]:
    return {
        'raw_string': ('raw_string', None),
        'cargo_movement': ('cargo_movement', None),
        'volume': ('volume', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: parse_date(x).strftime('%d %b %Y')),
    }


def search_vessel_name(raw_string: str) -> Dict[str, str]:
    vessel_name = re.search(r'm\/t\W+(?P<vessel_name>[A-z\s0-9]+)', raw_string)
    if vessel_name:
        return {'name': may_strip(vessel_name.group('vessel_name'))}

    logger.debug(f'unable to locate vessel {raw_string}')
    return None


def search_charterer(raw_string: str) -> str:
    charterer = re.search(r'(?:a \/ c|a\/c)\W+(?P<charterer>.*?)\|cargo\sreceiver', raw_string)
    if charterer:
        return may_strip(re.sub(r'\W', ' ', charterer.group('charterer')))

    logger.debug(f'unable to locate charterer {raw_string}')
    return None


def search_cargo_details(raw_string: str) -> Dict[str, str]:
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
            'movement': 'load',
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


def post_process_import_item(item, trade):
    if trade:
        # laycan period should be +/- 1 day from trade date (c.f. analysts)
        lay_can = parse_date(trade['Date (origin)'], dayfirst=False)
        item['lay_can_start'] = (lay_can - dt.timedelta(days=1)).isoformat()
        item['lay_can_end'] = (lay_can + dt.timedelta(days=1)).isoformat()
        # use origin port as departure zone, destination port as arrival zone
        item['arrival_zone'] = [trade['Zone Destination']]
        item['departure_zone'] = trade['Zone Origin']
    else:
        item['lay_can_start'] = None
        item['lay_can_end'] = None
        item['departure_zone'] = None
        item['arrival_zone'] = [item.get('port_name')]
