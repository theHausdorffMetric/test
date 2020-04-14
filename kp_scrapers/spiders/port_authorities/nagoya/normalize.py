import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

VESSEL_BLACKLIST = [
    'フルコンテナ船',  # barge
    '訓練船',  # training vessel
    '客船',  # cruise ship
    '自動車専用船',  # vehicle carrier
    '自動車航送船',  # vehicle carrier
    'RORO船',  # ro-ro vessel
    'その他の船舶',  # other uncategorised vessels
    '曳船・押船',  # dredger
    '作業船',  # work boat
    'セミコンテナ船',  # container vessel
    'チップ船',  # chip carrier
]

# website provides data for a lot of small domestic japanese vessels without IMOs
MIN_GROSS_TONNAGE = 5000


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str])

    Returns:
        Dict

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # discard irrelevant vessels
    vessel_type = item.pop('vessel_type', None)
    if not vessel_type or vessel_type in VESSEL_BLACKLIST:
        logger.info('Vessel %s is of an irrelevant type: %s', item['vessel_name'], vessel_type)
        return

    vessel_gt = item.pop('vessel_gt', 0)
    if vessel_gt < MIN_GROSS_TONNAGE:
        logger.info('Vessel %s is too small: %s', item['vessel_name'], vessel_gt)
        return

    # build Vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'call_sign': item.pop('vessel_callsign'),
        'length': item.pop('vessel_length'),
        'gross_tonnage': vessel_gt,
    }

    # build proper portcall dates
    item['eta'] = normalize_date(item.get('eta'), item['reported_date'])
    item['arrival'] = normalize_date(item.get('arrival'), item['reported_date'])
    item['berthed'] = normalize_date(item.get('berthed'), item['reported_date'])
    item['departure'] = normalize_date(item.get('departure'), item['reported_date'])

    return item


def portcall_mapping():
    return {
        # common fields
        '\u3000': ignore_key('empty field for picture display on website'),
        '係留施設': ignore_key('mooring facility'),
        '船名': ('vessel_name', may_strip),
        'コールサイン': ('vessel_callsign', may_strip),
        '総トン数': ('vessel_gt', normalize_numeric),
        '全長': ('vessel_length', normalize_numeric),
        '船種': ('vessel_type', may_strip),
        'コンテナ航路': ignore_key('container route'),
        '出港予定時刻': ('departure', None),
        '代理店名': ('shipping_agent', None),
        'installation': ('installation', may_strip),
        # vessels in-port
        '着岸時刻': ('arrival', None),
        # vessels forecast
        '高潮入港時刻': ('arrival', None),
        '入港予定時刻': ('eta', None),
        '伊良湖水道通過時刻': ignore_key('passage time through water channel; irrelevant'),
        '港外着予定時刻': ('berthed', None),
        # meta info
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', normalize_reported_date),
    }


def normalize_numeric(raw_data):
    """Normalize numeric data.

    Examples:
        >>> normalize_numeric('498 tons')
        498
        >>> normalize_numeric('76.23 m')
        76
        >>> normalize_numeric('foobar')

    Args:
        raw_data (str):

    Returns:
        int | None:

    """
    _search = re.search(r'[\d\.]*', raw_data)
    return try_apply(_search.group(), float, int) if _search else None


def normalize_date(raw_date, reported_date):
    """Normalize date with reported date as reference, the date lack year information.

    Examples:
        >>> normalize_date('05/14 08:00', '2019-05-14T03:10:00')
        '2019-05-14T08:00:00'
        >>> normalize_date('01/14 05:00', '2018-12-20T03:10:00')
        '2019-01-14T05:00:00'
        >>> normalize_date('05/14 20:50 A', '2019-05-16T00:00:00')
        '2019-05-14T20:50:00'

    Args:
        raw_date (str):
        reported_date (str):

    Returns:

    """
    if not raw_date:
        return

    # get rid of extraneous trailing characters
    raw_date = raw_date.split()
    raw_date = ' '.join(raw_date[:-1]) if len(raw_date) > 2 else ' '.join(raw_date)

    reported_date = parse_date(reported_date)
    proper_date = parse_date(f'{reported_date.year}/{raw_date}', dayfirst=False)

    # 180 days is a safe enough duration such that we won't get overflow cases
    if reported_date - proper_date > dt.timedelta(days=180):
        proper_date = proper_date.replace(year=proper_date.year + 1)

    return proper_date.isoformat()


def normalize_reported_date(raw_date):
    """Normalize raw reported date.

    Args:
        raw_date (str):

    Returns:
        str: reported date in ISO8601 format

    Examples:
        >>> normalize_reported_date('( 2019/05/16 12:57 現在)')
        '2019-05-16T00:00:00'
        >>> normalize_reported_date('(2019/05/16 ～ )')
        '2019-05-16T00:00:00'

    """
    _search = re.search(r'(\d{4}/\d{2}/\d{2})', raw_date)
    if not _search:
        logger.error('Unknown date format: %s', raw_date)
        return None

    return to_isoformat(_search.group(0), dayfirst=False)
