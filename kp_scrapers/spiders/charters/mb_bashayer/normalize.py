import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


UNIT_MAPPING = {'KB': Unit.kilobarrel, 'MB': Unit.megabarrel}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    if not item.get('vessel'):
        return

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    rate, arrival_zone_1 = normalize_rate_value(item.pop('rate_value', ''))

    item['rate_value'] = rate
    item['arrival_zone'] = item.get('arrival_zone') or arrival_zone_1
    item['departure_zone'] = 'Bashair'

    if item.get('cargo_qty_unit') and item['cargo_qty_unit']:
        quantity, units = normalize_qty_unit(item.pop('cargo_qty_unit'))
    else:
        quantity, units = None, None

    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'volume': quantity,
        'volume_unit': UNIT_MAPPING.get(units.lower() if units else None, None),
        'movement': 'load',
    }

    return item


def field_mapping():
    return {
        '0': ('lay_can', None),
        '1': ('cargo_qty_unit', None),
        '2': ('cargo_product', None),
        '3': ('charterer', None),
        '4': ('vessel', lambda x: {'name': x} if 'TBN' not in x.split() else None),
        '5': ('rate_value', None),
        '6': ('arrival_zone', normalize_arrival_zone),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Lay can date pattern:
        - Normal pattern: 27-29 SEP

    Month rollover case not spotted yet.

    Examples:
        >>> normalize_lay_can('27-29 SEP', '26 Nov 2018')
        ('2018-09-27T00:00:00', '2018-09-29T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        str: lay can start
        str: lay can end

    """
    _normal_match = re.match(r'(\d{1,2}).(\d{1,2}.)?([A-Za-z]{3,4})', raw_lay_can)
    if _normal_match:
        start_day, end_day, month = _normal_match.groups()
        year = _get_lay_can_year(month, reported)

        lay_can_start = _build_lay_can_date(start_day, month, year)
        lay_can_end = _build_lay_can_date(end_day, month, year) if end_day else lay_can_start

        return lay_can_start.isoformat(), lay_can_end.isoformat()


def _get_lay_can_year(month, reported):
    """Get the year of lay can date with reference of reported date.

    Args:
        month:
        reported:

    Returns:
        int:

    """
    year = parse_date(reported).year
    if 'DEC' == month and 'Jan' in reported:
        year -= 1
    if 'JAN' == month and 'Dec' in reported:
        year += 1
    return year


def _build_lay_can_date(day, month, year):
    """Build lay can date.

    Args:
        day:
        month:
        year:

    Returns:
        datetime.datetime:

    """
    return parse_date(f'{day} {month} {year}', dayfirst=True)


def normalize_rate_value(raw_rate_value):
    """Normalize rate value and identify if there's departure zone.

    Rate value might contain departure zone info, in this case, we'll extract each part.

    Examples:
        >>> normalize_rate_value('80KT X WS 165   (JAMNAGAR)')
        ('80KT X WS 165', ['JAMNAGAR'])
        >>> normalize_rate_value('80KT X RNR')
        ('80KT X RNR', [])
        >>> normalize_rate_value('')
        (None, None)

    Args:
        raw_rate_value (str):

    Returns:

    """
    if not raw_rate_value:
        return None, None
    match = re.match(r'([^()]+)\(?([^()]+)?\)?', raw_rate_value)
    if match:
        rate, arrival_zone = match.groups()

        return may_strip(rate), may_strip(arrival_zone).split('+') if arrival_zone else []


def normalize_arrival_zone(raw_departure_zone):
    """Normalize departure zone.

    Remove the brackets at either side of the string.

    Examples:
        >>> normalize_arrival_zone('(JAMNAGAR)')
        ['JAMNAGAR']
        >>> normalize_arrival_zone('(FUJ+SUNGAI LINGGI)')
        ['FUJ', 'SUNGAI LINGGI']
        >>> normalize_arrival_zone('(CHINA)')
        ['CHINA']
        >>> normalize_arrival_zone([])

    Args:
        raw_departure_zone (str): with brackets

    Returns:
        str:

    """
    raw_departure_zone = may_strip(raw_departure_zone)
    if not raw_departure_zone:
        return
    return re.sub(r'\(([^()]+)\)', r'\1', raw_departure_zone).split('+')


def normalize_qty_unit(raw_qty_unit):
    """Normalize quantity unit

    Examples:
        >>> normalize_qty_unit('600kb')
        ('600', 'kb')

    Args:
        raw_qty_unit (str):

    Returns:
        Tuple[str, str]:

    """
    _prod_match = re.match(r'([0-9.]+)(.*)', raw_qty_unit)

    return _prod_match.groups() if _prod_match else None
