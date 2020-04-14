from datetime import datetime
import re

from kp_scrapers.lib.date import ISO8601_FORMAT
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


ARRIVAL_ZONE_SEPARATOR = ['-', '+']

EMPTY_LAY_CAN_DATE = ['DNR']

IRRELEVANT_VESSEL = ['TBN']

RELEVANT_PRODUCT = ['FO', 'DPP', 'COND']


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.
     Args:
        raw_item (Dict[str, str]):
     Returns:
        SpotCharter | None
     """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can_date(
        item.pop('lay_can'), item['reported_date']
    )

    if not item['vessel'] or not item['lay_can_start']:
        return

    item['departure_zone'], item['arrival_zone'] = normalize_departure_arrival_zone(
        item.pop('voyage')
    )

    item['cargo'], item['status'] = normalize_cargo_status(item.pop('cargo'))

    return item


def field_mapping():
    return {
        '0': (
            'vessel',
            lambda x: {'name': normalize_vessel_name(x)} if normalize_vessel_name(x) else None,
        ),
        '1': ('cargo', None),
        '2': ('voyage', None),
        '3': ('rate_value', None),
        '4': ('lay_can', None),
        '5': ('charterer', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_vessel_name(raw_vessel):
    """Filter vessel and remove irrelevant letters.

     Examples:
        >>> normalize_vessel_name('HEIDMAR TBN')
        >>> normalize_vessel_name('BTTBN')
        'BTTBN'

     Args:
        raw_vessel (str):

     Returns:
        str:
     """
    for each in IRRELEVANT_VESSEL:
        if each in raw_vessel.split():
            return

    return raw_vessel


def normalize_cargo_status(raw_product):
    """Normalize cargo and status.

    Examples:
        >>> normalize_cargo_status('FO 80')
        ({'product': 'FO', 'movement': 'load', 'volume': '80', 'volume_unit': 'kilotons'}, None)
        >>> normalize_cargo_status('FLD 260')
        (None, 'Failed')

    Args:
        raw_product:

    Returns:

    """
    _split = raw_product.split()
    _product = _split[0]
    product = None
    if _product in RELEVANT_PRODUCT:
        product = {
            'product': _product,
            'movement': 'load',
            'volume': _split[-1],
            'volume_unit': Unit.kilotons,
        }

    status = None
    if 'FLD' in raw_product:
        status = SpotCharterStatus.failed
    return product, status


def normalize_lay_can_date(raw_lay_can, reported):
    """Get lay can date with reported year as reference.

    Examples:
        >>> normalize_lay_can_date('06 SEP', '23 Aug 2018')
        ('2018-09-06T00:00:00', '2018-09-06T00:00:00')
        >>> normalize_lay_can_date('DNR', '23 Aug 2018')
        (None, None)
        >>> normalize_lay_can_date('10 JAN', '12 Dec 2018')
        ('2019-01-10T00:00:00', '2019-01-10T00:00:00')

    Args:
        raw_lay_can:
        reported:

    Returns:

    """
    for pattern in EMPTY_LAY_CAN_DATE:
        if re.match(pattern, raw_lay_can):
            return None, None

    year = int(reported.split(' ')[-1])
    if 'DEC' in raw_lay_can and 'Jan' in reported:
        year -= 1
    if 'JAN' in raw_lay_can and 'Dec' in reported:
        year += 1

    lay_can = _change_date_format(raw_lay_can + ' ' + str(year), '%d %b %Y', ISO8601_FORMAT)
    return lay_can, lay_can


def normalize_departure_arrival_zone(raw_voyage):
    """Get departure and arrival zone info from voyage.

    Examples:
        >>> normalize_departure_arrival_zone('NOVO/UKCM-NINGBO')
        ('NOVO', ['UKCM', 'NINGBO'])

    Args:
        raw_voyage:

    Returns:

    """
    if '/' not in raw_voyage:
        return raw_voyage, None

    departure_zone, arrival_zone_str = raw_voyage.split('/')
    arrival_zone = [arrival_zone_str]

    for separator in ARRIVAL_ZONE_SEPARATOR:
        if separator in arrival_zone_str:
            arrival_zone = arrival_zone_str.split(separator)
            break
    return departure_zone, arrival_zone


def _change_date_format(date_str, original_format, to_format):
    """Change date format from original format to a new format.

    Examples:
        >>> _change_date_format('23/08/2018', '%d/%m/%Y', '%d %b %Y')
        '23 Aug 2018'

    Args:
        date_str (str):
        original_format (str):
        to_format (str):

    Returns:
        str:

    """
    return datetime.strptime(date_str, original_format).strftime(to_format)
