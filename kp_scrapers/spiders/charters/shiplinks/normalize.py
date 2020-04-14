import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


UNIT_MAPPING = {'mt': Unit.tons}

STATUS_MAPPING = {
    'subs': SpotCharterStatus.on_subs,
    'confirmed': SpotCharterStatus.fully_fixed,
    'failed': SpotCharterStatus.failed,
}

ZONE_MAPPING = {'Spore': 'Singapore'}

LAY_CAN_PATTERN = re.compile(r'(\d{1,2}).(\d{1,2}.)?([A-Za-z]{3,4})')

logger = logging.getLogger(__name__)


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    if not item.get('vessel'):
        return

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    # build cargo sub-model
    item['cargo'] = normalize_cargo(item.pop('volume'), item.pop('product'))

    return item


def field_mapping():
    return {
        'Vessel Name': ('vessel', lambda x: {'name': x}),
        'Vessel': ('vessel', lambda x: {'name': x}),
        'Quantity': ('volume', lambda x: ''.join(re.findall(r'\d+', x))),
        'Cargo': ('product', None),
        'Loadport': ('departure_zone', normalize_zone),
        'Discharge': ('arrival_zone', lambda x: x.split('-')),
        'Laycan': ('lay_can', None),
        # Freight field might change overtime
        'Freight (bss ws 2018)': ('rate_value', None),
        'Charterer': ('charterer', None),
        'Status': ('status', normalize_status),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_cargo(volume, product):
    """Normalize cargo.

    Args:
        volume (str):
        product (str):

    Returns:
        Dict[str, str]:

    """
    return {'product': product, 'movement': 'load', 'volume': volume, 'volume_unit': Unit.tons}


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date to ISO 8601 format with reported year as reference.

    The date pattern should be:
    1. 15-16 Sep
    2. 18 Aug

    In this report, we do encounter a typo, with start day and end day reversed, we don't handle
    such situation, just return them, it would fail in validation.

    And also, this report doesn't have cross month situation and vague month (ELY, MID, END), hence
    we don't handle it for now.

    Examples:
        >>> normalize_lay_can('15-16 Sep', '8 Aug 2018')
        ('2018-09-15T00:00:00', '2018-09-16T00:00:00')
        >>> normalize_lay_can('18 Aug', '8 Aug 2018')
        ('2018-08-18T00:00:00', '2018-08-18T00:00:00')
        >>> normalize_lay_can('21-15 Aug', '8 Aug 2018')
        ('2018-08-21T00:00:00', '2018-08-15T00:00:00')

    Args:
        raw_lay_can (str): already striped the blanks
        reported (str):

    Returns:
        Tuple[str, str]: lay can start, lay can end tuple

    """
    # lay can start year roll over
    year = parse_date(reported).year
    if 'Dec' in raw_lay_can and 'Jan' in reported:
        year -= 1
    if 'Jan' in raw_lay_can and 'Dec' in reported:
        year += 1

    # extract lay can start day, month and lay can end day (if any)
    match = LAY_CAN_PATTERN.match(raw_lay_can)
    if not match:
        logger.warning(f'Invalid lay can: {raw_lay_can}')
        return None, None

    start_day, end_day, month = match.groups()
    lay_can_start = to_isoformat(f'{start_day} {month} {year}')
    lay_can_end = to_isoformat(f'{end_day} {month} {year}') if end_day else lay_can_start

    return lay_can_start, lay_can_end


def normalize_status(vague_status):
    """Normalize status.

    Examples:
        >>> normalize_status('still on subs')
        'On Subs'
        >>> normalize_status('still on subs- replace anafi warrior')
        'On Subs'
        >>> normalize_status('confirmed')
        'Fully Fixed'

    Args:
        vague_status (str):

    Returns:
        SpotCharterStatus:

    """
    for status in STATUS_MAPPING:
        if status in vague_status.replace('-', ' ').split():
            return STATUS_MAPPING[status]


def normalize_zone(zone):
    """Normalize departure zone and arrival zone.

    Examples:
        >>> normalize_zone('Spore')
        'Singapore'
        >>> normalize_zone('STS Spore')
        'Singapore'
        >>> normalize_zone('Trafigura')
        'Trafigura'

    Args:
        zone (str):

    Returns:
        str:

    """
    if 'STS' in zone.split():
        zone = zone.replace('STS', '').strip()

    zone = ZONE_MAPPING.get(zone, zone)

    return zone
