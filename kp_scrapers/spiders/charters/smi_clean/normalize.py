import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


STATUS_MAPPING = {
    'FLD': SpotCharterStatus.failed,
    'DENIED': SpotCharterStatus.failed,
    'OLD': SpotCharterStatus.fully_fixed,
    'FXD': SpotCharterStatus.fully_fixed,
    'RPLC': SpotCharterStatus.fully_fixed,
    'RCNT': SpotCharterStatus.on_subs,
    'RPTD': SpotCharterStatus.on_subs,
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
    if not item['vessel']:
        return

    # build a proper cargo dict according to Cargo model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.kilotons,
        'movement': 'load',
    }

    item['lay_can_start'] = normalize_lay_can(item.pop('laycan'), item['reported_date'])

    return item


def field_mapping():
    return {
        '0': ('vessel', lambda x: {'name': x} if 'TBN' not in x.split() else None),
        '1': ('cargo_volume', None),
        '2': ('cargo_product', None),
        '3': ('departure_zone', None),
        '4': ('arrival_zone', lambda x: x.split('-') if x else None),
        '5': ('laycan', None),
        '6': ('rate_value', None),
        '7': ('charterer', lambda x: may_strip(x.replace('-', ''))),
        '8': ('status', lambda x: STATUS_MAPPING.get(x, None)),
        'provider': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Lay can date may comes in different patterns:
    1. 24/OCT
    2. 24OCT


    Examples:
        >>> normalize_lay_can('19/OCT', '08 August 2018')
        '2018-10-19T00:00:00'

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        str:

    """
    _match = re.match(r'(\d{1,2}).*?([A-z]+)', raw_lay_can)
    if _match:
        day, month = _match.group(1), _match.group(2)
        year = _get_year(month, reported)
        return to_isoformat(f'{day} {month} {year}', dayfirst=True)

    logger.error(f'unable to parse laycan {raw_lay_can}')


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
