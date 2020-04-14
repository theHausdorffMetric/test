from copy import deepcopy
import datetime as dt
import logging
import re
from typing import Any, Dict, List, Optional

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

# relevant vessels to be scraped, discard everything else
VESSEL_TYPE_MAPPING = {
    'ガスタンカー': 'Gas Tanker',
    'タンカー': 'Liquids Tanker',
}

# TODO we should insert them as vessel aliases instead
VESSEL_NAME_TRANSLATION = {
    'あかつき丸': 'AKATSUKI MARU',
    'ブルーク': 'BROOG',
    '泉州丸': 'SENSHU MARU',
}

# TODO we should insert them as zone aliases instead
ZONE_NAME_TRANSLATION = {
    'オーストラリア': 'Australia',
    '千葉': 'Chiba',
    '中国': 'China',
    'インドネシア': 'Indonesia',
    '愛媛': 'Kikuma',  # refers to Ehime prefecture actually; we have multiple installations there
    '川崎': 'Kawasaki',
    '衣浦': 'Kinuura',
    'クウェート': 'Kuwait',
    '神戸': 'Kobe',
    'マレーシア': 'Malaysia',
    '松山': 'Matsuyama',
    'メキシコ': 'Mexico',
    'オマーン': 'Oman',
    'パナマ': 'Panama',
    'パプアニューギ': 'Papua New Guinea',
    'ペルシャ湾': 'Persian Gulf',
    'フィリピン': 'Phillipines',
    '大分': 'Oita',
    'オマーン': 'Oman',
    '尼崎': 'Osaka',  # actually Amagasaki
    '堺': 'Osaka',  # actually Sakai
    '大阪': 'Osaka',
    'カタール': 'Qatar',
    'ロシア': 'Russia',
    '坂出': 'Sakaide',
    'サウジアラビア': 'Saudi Arabia',
    '仙台': 'Sendai',
    'シンガポール': 'Singapore',
    '韓国': 'South Korea',
    '東京': 'Tokyo',
    '宇部': 'Ube',
    'アラブ首長国': 'United Arab Emirates',
    'アメリカ': 'United States',
    'ベトナム': 'Vietnam',
    '名古屋': 'Yokkaichi',  # actually Nagoya
    '四日市': 'Yokkaichi',
    '横浜': 'Yokohama',
    '未定': None,  # means "uncertain"
}


@validate_item(PortCall, normalize=True, strict=False)
def combine_event(event: Dict[str, Any], events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Combine multiple distinct arrival and departure movements into a single Portcall item.

    How this is done is by looping through the list of portcalls and retrieving the first
    departure portcall of the 'event', if any. That is to say, the iteration breaks whenever
    the first departure portcall is spotted. This prevents us from matching unrelated departure
    events that are not of the same portcall.

    Note that is only works because we assume that the portcalls are listed in ascending order
    of movement date (which is currently the case on the provider website).

    """
    # don't process departure event on its own
    if event.get('departure'):
        return

    # begin to process eta events by combining with departure events
    # this can be done, because we assume the source already arranges all portcalls in asc order
    _DEPARTURE_FOUND = False
    for dep_event in events:
        # discard non-departure events
        if not dep_event.get('departure'):
            continue

        if dep_event['vessel']['name'] == event['vessel']['name']:
            _DEPARTURE_FOUND = True
            break

    # fill in 'port_name' field for current arrival event
    event.pop('previous_zone')
    event['port_name'] = event.pop('next_zone')

    # append departure details to arrival event, if any
    if _DEPARTURE_FOUND:
        # event['departure'] = dep_event['departure']  # TODO we don't need dep date yet
        event['next_zone'] = dep_event.get('next_zone')

    # yield two events, one without 'next_zone' and the other with it
    if event.get('next_zone'):
        yield _event_without_next_zone(event)
    yield event


def process_item(raw_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Transform raw item into a usable event."""

    item = map_keys(raw_item, portcall_mapping())
    # discard irrelevant vessels
    vessel_type = item.pop('vessel_type', None)
    if vessel_type not in VESSEL_TYPE_MAPPING.values():
        logger.info('Vessel is irrelevant: %s (%s)', item.get('vessel_name'), vessel_type)
        return

    # build Vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'length': item.pop('vessel_length', None),
        'gross_tonnage': item.pop('vessel_gross_tonnage', None),
    }

    # build portcall dates
    item[item.pop('pc_event')] = normalize_eta_date(item.pop('pc_date'), item['reported_date'])

    return item


def portcall_mapping():
    return {
        '日時': ('pc_date', None),
        '入航種別': ('pc_event', _map_pc_event),
        '船名': ('vessel_name', lambda x: _translate_to_english(x, VESSEL_NAME_TRANSLATION)),
        '種別': ('vessel_type', lambda x: VESSEL_TYPE_MAPPING.get(x, x)),
        'トン数': ('vessel_gross_tonnage', None),
        '長さ': ('vessel_length', None),
        '巨': ignore_key('signifies if vessel has length >= 200 metres'),
        '危': ignore_key('signifies if vessel contains dangerous/volatile cargo'),
        'P': ignore_key('unsure; to be clarified with analysts'),
        '国籍': ignore_key('vessel flag country'),
        '仕出港': ('previous_zone', lambda x: _translate_to_english(x, ZONE_NAME_TRANSLATION)),
        '仕向港': ('next_zone', lambda x: _translate_to_english(x, ZONE_NAME_TRANSLATION)),
        '代理店': ('shipping_agent', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', _normalize_reported_date),
    }


def normalize_eta_date(raw_date: str, reported_date: str) -> Optional[str]:
    """Normalize portcall dates as they do not contain the year.

    Format:
        1. mm/dd HH:MM

    Examples:
        >>> normalize_eta_date('04/07 04:35', '2019-04-03T10:01:00')
        '2019-04-07T04:35:00'
        >>> normalize_eta_date('12/30 01:00', '2019-12-30T10:01:00')
        '2019-12-30T01:00:00'
        >>> normalize_eta_date('12/30 01:00', '2020-01-01T10:01:00')
        '2019-12-30T01:00:00'
        >>> normalize_eta_date('01/03 08:45', '2019-12-30T10:01:00')
        '2020-01-03T08:45:00'

    """
    year = parse_date(reported_date).year
    _match = re.match(r'(\d{2})\/(\d{2})\s(\d{2})\:\s?(\d{2})', raw_date)

    if _match:
        _month, _day, _hour, _min = _match.groups()

        eta = dt.datetime(
            year=int(year), month=int(_month), day=int(_day), hour=int(_hour), minute=int(_min)
        )

        # to accomodate end of year parsing, prevent dates too old or far into
        # the future. 100 days was chosen as a gauge
        if (eta - parse_date(reported_date)).days < -100:
            eta = dt.datetime(
                year=int(year) + 1,
                month=int(_month),
                day=int(_day),
                hour=int(_hour),
                minute=int(_min),
            )

        if (eta - parse_date(reported_date)).days > 100:
            eta = dt.datetime(
                year=int(year) - 1,
                month=int(_month),
                day=int(_day),
                hour=int(_hour),
                minute=int(_min),
            )

        return eta.isoformat()

    logger.error('Unable to parse portcall date: %s', raw_date)
    return None


def _event_without_next_zone(event):
    event_without = deepcopy(event)
    event_without.pop('next_zone', None)
    return event_without


def _map_pc_event(raw_direction: str) -> Optional[str]:
    """Check if the cardinal direction of the vessel is present, signifying an arrival/departure.

    The source describes a port located in northern Ise Bay,
    which opens out to the Pacific in the south.

    Therefore:
        - if the cardinal direction signifies 'North', the portcall event is an 'ETA'
        - if the cardinal direction signifies 'South', the portcall event is a 'Departure'

    Examples:
        >>> _map_pc_event('南航船')
        'departure'
        >>> _map_pc_event('南 foobar')
        'departure'
        >>> _map_pc_event('北')
        'eta'
        >>> _map_pc_event('foobar')  # doctest:+IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ValueError: ["Unknown portcall event: 'foobar'"]

    """
    if '南' in raw_direction:  # south
        return 'departure'

    if '北' in raw_direction:  # north
        return 'eta'

    raise ValueError(f"Unknown portcall event: '{raw_direction}'")


def _normalize_reported_date(raw_date: str) -> str:
    """Normalize a raw reported date as given by the source:

    Examples:  # noqa
        >>> _normalize_reported_date('最終更新日時:\\n\\t2020年\\n\\t02月\\n\\t14日\\n\\t15時\\n\\t10分\\n')
        '2020-02-14T15:10:00'
    """
    search = re.search(
        r'(?P<year>\d{4})年'
        r'.*(?P<month>\d{2})月'
        r'.*(?P<day>\d{2})日'
        r'.*(?P<hour>\d{2})時'
        r'.*(?P<minute>\d{2})分',
        raw_date,
        flags=re.DOTALL,
    )
    if search:
        return parse_date(
            '{year}-{month}-{day}T{hour}:{minute}:00'.format(**search.groupdict()), dayfirst=False
        ).isoformat()

    raise ValueError(f'Unrecognisable reported date: {raw_date}')


def _translate_to_english(raw: str, name_mapping: Dict[str, str]) -> str:
    """Translate names to English.

    This source sometimes provides vessel/zone names as Japanese characters,
    so we need to translate them ourselves.

    Relying on a service like Google Translate proved to be inaccurate for our needs,
    hence we are doing it like this manually for now.
    If we miss out any translation, a log message will be printed.

    """
    res = name_mapping.get(raw, raw)
    # naive parsing for japanese characters
    if res and not re.search('[a-zA-Z]', res):
        logger.info('Name untranslated: %s', raw)

    # remove special character (◎ = maru for japanese lng tankers)
    res = res.replace('◎', '') if res else res

    return res
