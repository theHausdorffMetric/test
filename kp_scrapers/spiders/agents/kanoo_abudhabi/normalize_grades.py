import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_remove_substring, may_strip, try_apply
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

STRING_BLACKLIST = ['N/A', 'TBA', 'TBC', '', 'CNR']


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, grades_mapping())

    # discard items without vessel names
    vessel_name = item.pop('vessel_name')
    if not vessel_name:
        logger.warning(f'Item has no vessel name, discarding:\n{item}')
        return

    # discard items without valid dates
    item = normalize_item_dates(item)
    if not (item.get('arrival') or item.get('eta')):
        logger.warning(f'Item has no valid portcall dates, discarding:\n{item}')
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

    yield item


def grades_mapping():
    return {
        'ETA & PROSPECT': ('eta_prospect', None),
        'COMMODITY': ('cargo_product', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'QTY': ('cargo_volume', None),
        'reported_date': ('reported_date', normalize_reported_date),
        'VESSELS': ('vessel_name', normalize_vessel),
        'LAY CAN': ('lay_can', None),
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


def normalize_item_dates(item):
    """Cleanup item dates.

    Args:
        item (dict):

    Returns:
         item (dict):

    Examples:
        >>> normalize_item_dates({'eta_prospect': '14.03.2020', 'lay_can': '12-14', \
             'reported_date': '2020-07-05T17:15'})
        {'reported_date': '2020-07-05T17:15', 'arrival': None, 'eta': '2020-03-14T00:00:00'}
        >>> normalize_item_dates({'eta_prospect': 'TBA', 'lay_can': '12-14', \
             'reported_date': '2020-03-05T17:15'})
        {'reported_date': '2020-03-05T17:15', 'arrival': None, 'eta': '2020-03-14T00:00:00'}
    """
    date_str = item.pop('eta_prospect', None)
    item['arrival'] = None
    item['eta'] = None

    if re.compile('TBA').search(item['lay_can']) or not item['lay_can']:
        item['lay_can'] = None
    else:
        if item['lay_can'].split('-'):
            lay_can = may_strip(item['lay_can'].split('-')[-1])
        else:
            lay_can = may_strip(item['lay_can'])
        year_month = re.match(r'\d+-\d+-', item['reported_date']).group()
        item['lay_can'] = to_isoformat(year_month + lay_can, dayfirst=False)

    if re.compile('TBA').search(date_str):
        item['eta'] = item['lay_can']
    elif re.compile('LOAD|ANCHOR').search(date_str):
        item['arrival'] = item['reported_date']
    else:
        date_str = re.match(r'(\d+.\d+.\d+)', date_str).group()
        item['eta'] = to_isoformat(date_str.replace('.', '-'))

    item.pop('lay_can', None)

    return item


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


def normalize_reported_date(reported_date):
    """Normalize a raw volume into a float quantity and a unit.
           Args:
               reported_date (str):
           Returns:
                str:
           Examples:
               >>> normalize_reported_date('19.02.2020')
               '2020-02-19T00:00:00'
    """
    return to_isoformat(reported_date)
