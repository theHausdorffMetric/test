import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.parser import may_strip, split_by_delimiters
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


MOVEMENT_MAPPING = {'D': 'discharge', 'L': 'load'}

PORT_MAPPING = {'BYCO': 'Byco', 'KARACHI': 'Karachi', 'QASIM': 'Bin Qasim'}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict(str, str):

    """
    item = map_keys(raw_item, grades_mapping())
    # discard unknown vessels
    if 'TBA' in item['vessel']['name']:
        return

    # separate conjoined cells
    _movement, item['eta'] = item.pop('movement_eta')
    # discard the item without movement info, according to analysts
    if not _movement:
        return

    # normalize dates
    for date in ['berthed', 'eta', 'departure']:
        item[date] = normalize_date(item[date], item['reported_date'])

    # build Cargoes sub-model
    item['cargoes'] = []
    _product_volume_list = split_cargo(item.pop('cargo'))
    for _product_volume in _product_volume_list:
        item['cargoes'].append(
            {
                'product': _product_volume[1],
                # TODO activate when necessary
                'movement': next(
                    (MOVEMENT_MAPPING[m] for m in MOVEMENT_MAPPING if m in _movement), None
                ),
                'volume': _product_volume[0],
                'volume_unit': Unit.tons,
            }
        )
        yield item


def grades_mapping():
    return {
        '0': ('vessel', lambda x: {'name': x}),
        '1': ('cargo', None),
        '2': ('movement_eta', normalize_movement_eta),
        '3': ('berthed', None),
        '4': ('departure', lambda x: x.replace('(*)', '')),
        'port_name': (
            'port_name',
            lambda x: next((PORT_MAPPING[port] for port in PORT_MAPPING if port in x), None),
        ),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def split_cargo(raw_cargo):
    """Extract multiple cargos

    Args:
        raw_cargo (str):

    Returns:
        List[Tuple[str, str]]

    Examples:
        >>> split_cargo('65,000 / LNG')
        [('65000.0', 'LNG')]
        >>> split_cargo('26,000 / MOGAS+HSD')
        [('13000.0', 'MOGAS'), ('13000.0', 'HSD')]
        >>> split_cargo('26,000 / MOGAS/HSD')
        [('13000.0', 'MOGAS'), ('13000.0', 'HSD')]
        >>> split_cargo('MOGAS')
        [(None, 'MOGAS')]
        >>> split_cargo('MOGAS/HSD')
        [(None, 'MOGAS'), (None, 'HSD')]
    """
    vol_cargoes = split_by_delimiters(raw_cargo, '+', '/')
    if len(vol_cargoes) >= 2:
        final_list = []
        for idx, vol_cargo in enumerate(vol_cargoes):
            if idx == 0:
                if may_strip(vol_cargo).replace(',', '').isnumeric():
                    vol = int(may_strip(vol_cargo).replace(',', '')) / (len(vol_cargoes) - 1)
                    continue
                else:
                    vol = None

            final_list.append((str(vol) if vol else None, may_strip(vol_cargo)))
        return final_list

    return [(None, raw_cargo)]


def normalize_date(raw_date, reported_date):
    """Normalize raw port-call date into a full date.

    Args:
        raw_date (str): raw date with only day/month
        reported_date (str): ISO-8601 formatted date

    Returns:
        str: port-call date as an ISO8601 date

    Examples:
        >>> normalize_date('25/12', '2018-12-23T00:00:00')
        '2018-12-25T00:00:00'
        >>> normalize_date('25/12', '2019-01-02T00:00:00')
        '2018-12-25T00:00:00'
        >>> normalize_date('03/01', '2018-12-28T00:00:00')
        '2019-01-03T00:00:00'
        >>> normalize_date('19/12 (NE)/2018', '2018-12-19T00:00:00')
        '2018-12-19T00:00:00'
    """
    reported_date = parse_date(reported_date, dayfirst=False)
    raw_date = re.findall(r'\d{1,2}\/\d{1,2}', raw_date)[0]
    full_date = parse_date(f'{raw_date}/{reported_date.year}', dayfirst=True)

    if (full_date - reported_date).days > 180:
        full_date -= relativedelta(years=1)
    elif (full_date - reported_date).days < -180:
        full_date += relativedelta(years=1)

    return full_date.isoformat()


def normalize_movement_eta(movement_eta):
    """Normalize movement and eta.

    Examples:
        >>> normalize_movement_eta('(D) 24/11')
        ['(D', ' 24/11']
        >>> normalize_movement_eta('26/11')
        (None, None)

    Args:
        movement_eta:

    Returns:

    """
    if ')' not in movement_eta:
        return None, None

    return movement_eta.split(')')
