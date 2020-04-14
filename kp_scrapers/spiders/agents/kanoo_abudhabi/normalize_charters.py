import datetime
import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_remove_substring, may_strip, try_apply
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

STRING_BLACKLIST = ['N/A', 'TBA', 'TBC', '', 'CNR']


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, charters_mapping())

    # discard items without vessel names
    vessel_name = item.pop('vessel_name')
    if not vessel_name:
        logger.warning(f'Item has no vessel name, discarding:\n{item}')
        return

    # discard items without laycan dates
    item = normalize_item_dates(item)
    if not item.get('lay_can_start'):
        logger.warning(f'Item has no valid laycan start date, discarding:\n{item}')
        return

    # build Vessel sub-model
    item['vessel'] = {'name': vessel_name, 'imo': item.pop('vessel_imo', None)}

    # build Cargo sub-model
    product = item.pop('cargo_product', [])
    # Kanoo Shipping in Abu Dhabi only reports exports :
    # assumption => cargo movement = 'load'
    movement = 'load'

    # associate volumes to their respective products
    volume, unit = normalize_volume_unit(item.pop('cargo_volume', None))

    # discard items without valid cargo volume
    if not volume or volume < 1:
        logger.warning(f'Item has no valid cargo volume, discarding:\n{item}')
        return

    item['cargo'] = {
        'product': product,
        'movement': movement,
        'volume': try_apply(volume, float, int),
        'volume_unit': unit,
    }

    if not item.get('charterer'):
        logger.warning(f'Item has no valid charterer, discarding:\n{item}')
        return

    yield item


def charters_mapping():
    return {
        'COMMODITY': ('cargo_product', None),
        'port_name': ('departure_zone', None),
        'provider_name': ('provider_name', None),
        'QTY': ('cargo_volume', None),
        'PRINCIPAL / CHARTERER': ('charterer', normalize_charterer),
        'reported_date': ('reported_date', None),
        'VESSELS': ('vessel_name', normalize_vessel),
        'LAY CAN': ('lay_can_start', None),
    }


def normalize_vessel(raw_vessel):
    """ Normalize raw vessel string.
        Args:
           raw_vessel (str):
        Returns:
            str:
        Examples:
        >>> normalize_vessel('NESTE')
        'NESTE'
        >>> normalize_vessel('N/A')
        >>> normalize_vessel('')

    """

    return None if may_strip(raw_vessel) in STRING_BLACKLIST else may_strip(raw_vessel)


def normalize_charterer(raw_charterer):
    """Normalize raw charterer string.

    This function will split a raw charterer name by "/" and
     select the last name.

    Examples:
        >>> normalize_charterer('LITASCO/NESTE')
        'NESTE'
        >>> normalize_charterer('NESTE')
        'NESTE'
        >>> normalize_charterer('CNR')
        >>> normalize_charterer('')

    """
    if raw_charterer.split('/'):
        charterer = raw_charterer.split('/')[-1]
    else:
        charterer = raw_charterer

    charterer = may_strip(charterer)

    return None if charterer in STRING_BLACKLIST else charterer


def normalize_volume_unit(raw_volume):
    """Normalize a raw volume into a float quantity and a unit.
       Args:
           raw_volume (str):
       Returns:
            float ,str:
       Examples:
           >>> normalize_volume_unit('N/A')
           (None, None)
           >>> normalize_volume_unit('42.0 MT')
           (42.0, 'tons')
           >>> normalize_volume_unit('42.0 BBLS')
           (42.0, 'barrel')
    """
    if not raw_volume or raw_volume in STRING_BLACKLIST:
        return None, None

    volume = may_remove_substring(raw_volume, ',')
    volume_match = re.match(r'\d+', volume)
    if volume_match:
        volume = volume_match.group(0)
        if re.compile('BBLS').search(raw_volume):
            return float(volume), Unit.barrel
        else:
            return float(volume), Unit.tons
    else:
        return None, None


def normalize_item_dates(item):
    """Cleanup item dates.

    Args:
        item (dict):

    Returns:
        item (dict):

    Examples:
    >>> normalize_item_dates({'lay_can_start': '12-14', \
         'reported_date': '05.03.2020'})
    {'lay_can_start': '2020-03-14T00:00:00', 'reported_date': '05 Mar 2020'}
    >>> normalize_item_dates({'lay_can_start': 'TBA', 'reported_date': '05.03.2020'})
    {'lay_can_start': None, 'reported_date': '05 Mar 2020'}
    """
    try:
        reported_date = datetime.datetime.strptime(item['reported_date'], '%d.%m.%Y')
        item['reported_date'] = reported_date.strftime('%d %b %Y')
    except ValueError:
        item['reported_date'] = None
        item['lay_can_start'] = None
        return item

    if not item['lay_can_start'] or item['lay_can_start'] in STRING_BLACKLIST:
        item['lay_can_start'] = None
    else:
        if item['lay_can_start'].split('-'):
            lay_can = may_strip(item['lay_can_start'].split('-')[-1])
        else:
            lay_can = may_strip(item['lay_can_start'])
        year_month = str(reported_date.year) + '-' + str(reported_date.month) + '-'
        item['lay_can_start'] = to_isoformat(year_month + lay_can, dayfirst=False)

    item['reported_date'] = reported_date.strftime('%d %b %Y')
    return item
