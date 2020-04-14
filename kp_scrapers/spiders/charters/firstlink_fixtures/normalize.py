import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


STATUS_MAPPING = {
    'SUBS': SpotCharterStatus.on_subs,
    'SUB': SpotCharterStatus.on_subs,
    'WORKING': SpotCharterStatus.on_subs,
    'FXD': SpotCharterStatus.fully_fixed,
    'RPLC': SpotCharterStatus.fully_fixed,
    'FLD': SpotCharterStatus.failed,
}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # discard TBN vessels
    if any(sub in item['vessel']['name'] for sub in ('TBN', 'VESSEL')):
        return

    if not item['lay_can']:
        return

    # build cargo item
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.kilotons,
    }

    item['lay_can_start'] = normalize_lay_can(
        # laycan cannot be popped here to have it shown in missing rows list
        item['lay_can'],
        item['reported_date'],
    )

    item.pop('lay_can')
    return item


def field_mapping():
    return {
        'VESSEL': ('vessel', lambda x: {'name': may_strip(x)}),
        'SIZE': ('cargo_volume', None),
        'CARGO': ('cargo_product', None),
        'LAYCAN': ('lay_can', None),
        'LOAD': ('departure_zone', None),
        'DISCHARGE': ('arrival_zone', lambda x: normalize_voyage(x)),
        'RATE': ('rate_value', None),
        'CHTRS': ('charterer', None),
        'REMARK': ('status', lambda x: STATUS_MAPPING.get(x, None)),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    In this report, the lay can date can vary differently, however, we only extract below formats:
    - 16/OCT

    Examples:
        >>> normalize_lay_can('16/OCT', '22 Sep 2018')
        '2018-10-16T00:00:00'

    Args:
        raw_lay_can (str)
        reported (str)

    Returns:
        str:

    """
    _day, _month = raw_lay_can.split('/')
    year = _get_year(_month, reported)
    return to_isoformat(f'{_day} {_month} {year}', dayfirst=True)


def _get_year(lay_can_str, reported):
    """Get lay can year with reference of reported date.

    Args:
        lay_can_str (str):
        reported (str):

    Returns:
        str:

    """
    year = parse_date(reported).year
    if 'Dec' in reported and 'JAN' in lay_can_str.upper():
        year += 1
    if 'Jan' in reported and 'DEC' in lay_can_str.upper():
        year -= 1

    return year


def normalize_voyage(raw_voyage):
    """Normalize departure zone and arrival zone from voyage field.

    Examples:
        >>> normalize_voyage('MED-MALTA')
        ['MED', 'MALTA']
        >>> normalize_voyage('UKC')
        ['UKC']

    Args:
        raw_voyage (str):

    Returns:
        List[str]: arrival zones

    """
    return raw_voyage.split('-')
