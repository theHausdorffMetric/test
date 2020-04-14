import logging

from kp_scrapers.lib.date import get_date_range
from kp_scrapers.lib.parser import may_remove_substring, may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


UNIT_MAPPING = {'kb': Unit.kilobarrel}


STATUS_MAPPING = {
    'FLD': SpotCharterStatus.failed,
    'DENIED': SpotCharterStatus.failed,
    'OLD': SpotCharterStatus.fully_fixed,
    'RPLC': SpotCharterStatus.fully_fixed,
    'RCNT': SpotCharterStatus.on_subs,
    'RPTD': SpotCharterStatus.on_subs,
}


ZONE_SUBSTR_BLACKLIST = ['STS', '+1', '+ 1']


logger = logging.getLogger(__name__)


@validate_item(SpotCharter, normalize=True, strict=True)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    if not item['vessel']:
        return

    item['cargo'] = {
        'product': may_strip(item.pop('cargo_product', None)),
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.kilotons
        if not item.get('unit')
        else UNIT_MAPPING.get(item.get('unit').lower(), item.get('unit')),
        'movement': 'load',
    }

    item['lay_can_start'], item['lay_can_end'] = get_date_range(
        item.pop('lay_can'), '/', '-', item['reported_date']
    )

    if item.get('arrival_charterer'):
        item['arrival_zone'], item['charterer'] = split_arrival_charterer(
            item.pop('arrival_charterer', None)
        )

    item['charterer'] = normalize_charterer(item['charterer'])
    item['departure_zone'] = normalize_zone(item['departure_zone'])[0]
    item['arrival_zone'] = normalize_zone(item['arrival_zone'])

    item.pop('unit', None)

    return item


def field_mapping():
    return {
        'vessel': ('vessel', lambda x: {'name': x.replace(' OOS', '')} if 'TBN' not in x else None),
        'volume': ('cargo_volume', None),
        'cargo': ('cargo_product', None),
        'lay_can': ('lay_can', None),
        'arrival': ('arrival_zone', None),
        'arrival_charterer': ('arrival_charterer', None),
        'departure': ('departure_zone', None),
        'rate': ('rate_value', None),
        'charterer': ('charterer', None),
        'unit': ('unit', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_charterer(raw_charterer):
    """Remove unnecessary strings

    Examples:
        >>> normalize_charterer('SHELL')
        'SHELL'
        >>> normalize_charterer('SHELL-REPLACED')
        'SHELL'

    Args:
        raw_charterer (str):

    Returns:
        str:

    """
    charter = raw_charterer.partition('-')[0]
    return may_strip(charter)


def normalize_zone(raw_zone):
    """Normalize arrival zones.

    Args:
        raw_zone (str):

    Returns:
        List[str]:

    Examples:
        >>> normalize_zone('BASRAH-KAA')
        ['BASRAH', 'KAA']
        >>> normalize_zone('BASRAH')
        ['BASRAH']
        >>> normalize_zone('BASRAH+1')
        ['BASRAH']
        >>> normalize_zone('BUKIT TUA+1')
        ['BUKIT TUA']
        >>> normalize_zone('STS YOSU')
        ['YOSU']
    """
    arrival_zone = []
    for single_zone in raw_zone.split('-'):
        zone = may_strip(may_remove_substring(single_zone, ZONE_SUBSTR_BLACKLIST))

        arrival_zone.append(zone)

    return arrival_zone if arrival_zone else [raw_zone]


def split_arrival_charterer(raw_arrival_charterer):
    """source is tricky in obtaining charterer and arrival zone.
    for now it seems like china is the only zone with one extra
    spacing. this function serves to safely seperate the zone and
    charterer

    Args:
        raw_arrival_charterer (str):

    Returns:
        Tuple[str, str]:

    Examples:
        >>> split_arrival_charterer('N China Linkoil')
        ('china', 'linkoil')
        >>> split_arrival_charterer('M-N China Linkoil')
        ('china', 'linkoil')
        >>> split_arrival_charterer('N China BP SINOPEC')
        ('china', 'bp sinopec')
        >>> split_arrival_charterer('Sing-jap SINOPEC')
        ('sing-jap', 'sinopec')
        >>> split_arrival_charterer('Sing-jap bp SINOPEC')
        ('sing-jap', 'bp sinopec')
    """
    token_list = raw_arrival_charterer.lower().split(' ')
    if 'china' in token_list:
        return 'china', ' '.join(token_list[2:])

    return token_list[0], ' '.join(token_list[1:])
