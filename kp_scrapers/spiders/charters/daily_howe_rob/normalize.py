import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import is_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MISSING_ROWS = []


@validate_item(SpotCharter, normalize=True, strict=True)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, charters_mapping())

    # remove vessels not named
    if not item['vessel']['name']:
        return

    if not item['lay_can_start']:
        MISSING_ROWS.append(str(raw_item))

    # build a proper cargo dict according to Cargo model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.kilotons,
        'movement': 'load',
    }
    item['rate_value'] = normalize_currency_rate(item.pop('currency', None), item.pop('rate', None))

    return item


def charters_mapping():
    return {
        'date reported': ('reported_date', lambda x: parse_date(x).strftime('%d %b %Y')),
        'vessel': ('vessel', lambda x: {'name': x if 'TBN' not in x else None}),
        'cargo size (kt)': ('cargo_volume', None),
        'load': ('departure_zone', None),
        'discharge': ('arrival_zone', lambda x: x.split(',')),
        'laycan': ('lay_can_start', lambda x: normalize_laycan(x)),
        'currency': ('currency', None),
        'rate': ('rate', None),
        'charterer': ('charterer', lambda x: x if x != 'CNR' else None),
        'cargo type': ('cargo_product', None),
        'clean/dirty': ignore_key('clean or dirty'),
        'provider_name': ('provider_name', None),
    }


def normalize_currency_rate(raw_currency, raw_rate):
    """Combine currency and rate column together

    Args:
        raw_currency (str):
        raw_rate (str):

    Returns:
        str:

    Examples:
        >>> normalize_currency_rate('WS', '167.5')
        'WS 167.5'
        >>> normalize_currency_rate('RNR', None)
        'RNR'
    """
    return may_strip(f'{raw_currency} {raw_rate}') if raw_currency and raw_rate else raw_currency


def normalize_laycan(raw_laycan):
    """normalize laycans

    Args:
        raw_laycan (str):

    Returns:
        str:

    Examples:
        >>> normalize_laycan('2020-01-01T00:00:00')
        '2020-01-01T00:00:00'
        >>> normalize_laycan('20Feb20')
        '2020-02-20T00:00:00'

    """
    if is_isoformat(raw_laycan):
        return raw_laycan
    else:
        try:
            _match = re.match(r'(\d+)([A-z]+)(\d+)', raw_laycan)
            if _match:
                day, month, year = _match.groups()
                return parse_date(f'20{year} {month} {day}').isoformat()
        except Exception:
            return None
