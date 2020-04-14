import datetime
import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_remove_substring, may_strip, try_apply
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

STRING_BLACKLIST = ['N/A', 'TBA', 'TBC']
STATUS_MAPPING = {
    'SUBS': SpotCharterStatus.on_subs,
    'FIXED': SpotCharterStatus.fully_fixed,
    'FFXD': SpotCharterStatus.fully_fixed,
    'FAILED': SpotCharterStatus.failed,
    'FLD': SpotCharterStatus.failed,
}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.
    Args:
        raw_item (Dict[str, str]):
    Yields:
        Dict[str, str]:
    """
    item = map_keys(raw_item, charters_mapping())

    if item['status'] == SpotCharterStatus.failed:
        logger.info(f'Spot Charter has status failed: {item}')
        return

    # discard items without vessel names
    vessel_name = item.pop('vessel_name')
    if not vessel_name:
        logger.warning(f'Item has no vessel name, discarding:\n{item}')
        return

    item = normalize_item_dates(item)
    # discard items without valid dates
    if not item.get('charterer'):
        logger.warning(f'Item has no valid charterer, discarding:\n{item}')
        return

    # discard items without valid dates
    if not item.get('departure_zone'):
        logger.warning(f'Item has no valid departure zone, discarding:\n{item}')
        return

    # discard items without valid dates
    if not (item.get('reported_date') and item.get('lay_can_start')):
        logger.warning(f'Item has no valid dates, discarding:\n{item}')
        return

    # build Vessel sub-model
    item['vessel'] = {'name': vessel_name}

    # build Cargo sub-model
    product, volume = get_products_and_volumes(item)
    item['cargo'] = {
        'product': product,
        'volume': try_apply(volume, float, int),
        'volume_unit': Unit.tons,
    }

    # get arrival zone
    item = normalize_item_arrival_zone(item)

    yield item


def charters_mapping():
    return {
        'REPORTED DATE': ('reported_date', None),
        'VESSEL': ('vessel_name', None),
        'CARGO': ('cargo_product', lambda x: x if x and x not in STRING_BLACKLIST else None),
        'GRADE': ('cargo_grade', lambda x: x if x and x not in STRING_BLACKLIST else None),
        'QUANTITY': ('cargo_volume', lambda x: x if x and x not in STRING_BLACKLIST else None),
        'LAYCAN FROM ': ('lay_can_start', None),
        'LAYCAN TO': ('lay_can_end', None),
        'LOADING': ('departure_zone', None),
        'DISCHARGE OPT 1': ('arrival_zone_1', may_strip),
        'DISCHARGE OPT 2': ('arrival_zone_2', may_strip),
        'DISCHARGE OPT 3': ('arrival_zone_3', may_strip),
        'CHARTERER': ('charterer', None),
        'STATUS': ('status', lambda x: STATUS_MAPPING.get(may_strip(x), None)),
        'provider_name': ('provider_name', None),
    }


def normalize_lay_can_date(lay_can, reported_date):
    """Normalize a lay can date regarding its reported date
    Args:
        lay_can (str)
        reported_date (datetime.datetime)
    Returns:
        str:
    Examples:
        >>> normalize_lay_can_date('03/18/2020', datetime.datetime(2020, 3, 1, 0, 0))
        '2020-03-18T00:00:00'
        >>> normalize_lay_can_date('18/03', datetime.datetime(2020, 3, 1, 0, 0))
        '2020-03-18T00:00:00'
    """
    if not lay_can:
        return None
    if re.search(r'(\d+(/|-)\d+(/|-)\d{2,4})', lay_can):
        return to_isoformat(lay_can, dayfirst=False)
    if re.search(r'(\d+(/|-)\d+)', lay_can):
        lay_can = lay_can + '/' + str(reported_date.year)
        return to_isoformat(lay_can)


def normalize_item_dates(item):
    """Normalize item dates
    Args:
        item (dict)
    Returns:
        dict:
    Examples:
>>> normalize_item_dates({'lay_can_start': '03/18/2020', 'lay_can_end': '03/20/2020',\
 'reported_date': '2020-03-01T00:00:00'})
{'lay_can_start': '2020-03-18T00:00:00', 'lay_can_end': '2020-03-20T00:00:00',\
 'reported_date': '01 Mar 2020'}
>>> normalize_item_dates({'lay_can_start': '18/03', 'lay_can_end': '20/03',\
 'reported_date': '2020-03-01T00:00:00'})
{'lay_can_start': '2020-03-18T00:00:00', 'lay_can_end': '2020-03-20T00:00:00',\
 'reported_date': '01 Mar 2020'}
    """
    if item['reported_date']:
        reported_date = datetime.datetime.strptime(item['reported_date'], '%Y-%m-%dT%H:%M:%S')
        item['lay_can_start'] = normalize_lay_can_date(item['lay_can_start'], reported_date)
        item['lay_can_end'] = normalize_lay_can_date(item['lay_can_end'], reported_date)
        item['reported_date'] = reported_date.strftime('%d %b %Y')
    return item


def normalize_item_arrival_zone(item):
    """Normalize the arrival zones
    Args:
        item (dict)
    Returns:
        dict:
    Examples:
        >>> normalize_item_arrival_zone({'arrival_zone_1': 'A', 'arrival_zone_2': 'B',\
         'arrival_zone_3': 'C'})
        {'arrival_zone': ['A', 'B', 'C']}

    """
    arrival_zone = []
    if item.get('arrival_zone_1'):
        arrival_zone.append(item.get('arrival_zone_1'))
    if item.get('arrival_zone_2'):
        arrival_zone.append(item.get('arrival_zone_2'))
    if item.get('arrival_zone_3'):
        arrival_zone.append(item.get('arrival_zone_3'))
    item.pop('arrival_zone_1')
    item.pop('arrival_zone_2')
    item.pop('arrival_zone_3')
    item['arrival_zone'] = arrival_zone
    return item


def get_products_and_volumes(item):
    """Get products and volumes according to the presence of grade features
    Args:
        item (Dict[str, str]):
    Returns:
        Tuple[List, List]:
    Examples:
        >>> get_products_and_volumes({'cargo_grade': 'A', 'cargo_product': 'C',\
         'cargo_volume': '1000'})
        ('A', 1000.0)
        >>> get_products_and_volumes({'cargo_grade': None, 'cargo_product': 'C',\
         'cargo_volume': '1000'})
        ('C', 1000.0)
        >>> get_products_and_volumes({'cargo_grade': None, 'cargo_product': None,\
         'cargo_volume': '3000'})
        ([], [])
        >>> get_products_and_volumes({'cargo_grade': None, 'cargo_product': None,\
         'cargo_volume': None})
        ([], [])
    """
    grade = item.pop('cargo_grade', None)
    cargo = item.pop('cargo_product', None)
    volumes = item.pop('cargo_volume', None)

    products = grade if grade else cargo
    products = products if products else []
    volumes = float(may_remove_substring(volumes, '.')) if volumes and products else []

    return products, volumes
