import logging
import re

from dateutil.parser import parse as parse_date
from xlrd.xldate import xldate_as_datetime

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys, protect_against
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

MOVEMENT_MAPPING = {'LOAD': 'load', 'DISCHARGE': 'discharge', 'DISCH': 'discharge'}

PORT_NAME_MAPPING = {
    'AG.THEODOROI': 'Agioi Theodoroi',
    'AG. THEODOROI': 'Agioi Theodoroi',
    'ELEUSIS': 'Elefsina',
}


@validate_item(CargoMovement, normalize=True, strict=False)
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

    item['berthed'], item['arrival'], item['departure'] = normalize_matching_date(
        item.pop('matching_date', '')
    )

    for cargo in normalize_cargo(item.pop('movement'), item.pop('product'), item.pop('volume')):
        item['cargo'] = cargo
        yield item


def field_mapping():
    return {
        'PORT': ('port_name', lambda x: PORT_NAME_MAPPING.get(x, x)),
        'TERMINAL': (ignore_key('terminal')),
        'VESSEL': ('vessel', lambda x: None if 'TBN' in x.split() or not x else {'name': x}),
        'PREVIOUS PORT': (ignore_key('not required for Portcall for now')),
        'NEXT PORT': (ignore_key('not required for Portcall for now')),
        'NEXT / PREVIOUS PORT': (ignore_key('not required for Portcall for now')),
        'NEXT/PREVIOUS': (ignore_key('not required for Portcall for now')),
        'DISCH/LOAD': ('movement', lambda x: MOVEMENT_MAPPING.get(x, None)),
        'LOAD/DISCHARGE': ('movement', lambda x: MOVEMENT_MAPPING.get(x, None)),
        'GRADE GROUP': ('product', None),
        'QUANTITY': ('volume', None),
        'DATES': ('matching_date', None),
        'COMMENTS': ('matching_date', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', to_isoformat),
    }


def normalize_matching_date(raw_date):
    """Normalize arrival date.

    Date might appear be a range, pick the later date.
        - 2018/10/17
        - 30/07-01/08/13

    Examples:
        >>> normalize_matching_date('11-13/12/15')
        (None, '2015-12-11T00:00:00', '2015-12-13T00:00:00')
        >>> normalize_matching_date('24/01-01/02/14')
        (None, '2014-01-24T00:00:00', '2014-02-01T00:00:00')
        >>> normalize_matching_date(42359.0)
        ('2015-12-21T00:00:00', None, None)

    Args:
        raw_date (str):

    Returns:
        str: berthed
        str: arrival date
        str: departure date

    """
    if not raw_date:
        return None, None, None

    # xlrd date float
    if isinstance(raw_date, float):
        return _convert_xlrd_date(raw_date), None, None

    # date range
    else:
        _match = re.match(r'(\d{1,2})/?(\d{1,2})?-(\d{1,2}/\d{1,2}/\d{1,2})', raw_date)
        if not _match:
            logger.error(f'Date pattern is invalid: {raw_date}')
            return None, None, None

        arrival_day, arrival_month, departure = _match.groups()
        arrival_day, arrival_month = try_apply(arrival_day, int), try_apply(arrival_month, int)

        departure = parse_date(departure, dayfirst=True)
        arrival = None
        if arrival_day and arrival_month:
            arrival = departure.replace(day=arrival_day, month=arrival_month)
        if arrival_day and not arrival_month:
            arrival = departure.replace(day=arrival_day)

        return None, arrival.isoformat(), departure.isoformat()


@protect_against()
def _convert_xlrd_date(raw_date_float):
    """Convert xlrd date float to iso format date.

    Args:
        raw_date_float (float):

    Returns:
        str:

    """
    return xldate_as_datetime(raw_date_float, datemode=0).isoformat()


def normalize_cargo(movement, products, volumes):
    """Normalize cargoes with given information.

    See https://kpler1.atlassian.net/browse/KP-6455 for how we deal with multiple cargoes.

    Args:
        movement: load or discharge
        products:
        volumes (str | float):

    Yields:
        Dict[str, str]:

    """
    multi_product = '/' in products

    volume_list, unit = _extract_volume(volumes)
    for idx, product in enumerate(products.split('/')):
        if not volume_list:
            volume = None
        else:
            if multi_product and len(volume_list) == 1:
                volume = _split_volume(volume_list[0])
            else:
                volume = volume_list[idx]

        yield {'product': product, 'movement': movement, 'volume': volume, 'volume_unit': unit}


def _extract_volume(raw_volume):
    """Extract specific volume and remove other irrelevant strings.

    Examples:
        >>> _extract_volume('24.230 MT / 10.935 MT ')
        (['24230', '10935'], 'tons')
        >>> _extract_volume('21.000 MT')
        (['21000'], 'tons')
        >>> _extract_volume(4000.0)
        (['4000.0'], 'tons')
        >>> _extract_volume('3000 m3')
        (['3000'], 'cubic_meter')
        >>> _extract_volume('')
        (None, None)


    Args:
        raw_volume (List[str]):

    Returns:
        str: volume
        str: unit

    """
    if not raw_volume:
        return None, None

    if isinstance(raw_volume, float) or isinstance(raw_volume, int):
        return [str(raw_volume)], Unit.tons

    else:
        raw_volume = raw_volume.upper()

        unit = Unit.tons
        if 'M3' in raw_volume:
            unit = Unit.cubic_meter

        return (
            [
                may_strip(cell)
                for cell in raw_volume.replace('.', '')
                .replace('MT', '')
                .replace('M3', '')
                .split('/')
            ],
            unit,
        )


def _split_volume(raw_volume):
    """Split the volume.

    Args:
        raw_volume (str):

    Returns:
        str:

    """
    _float = try_apply(raw_volume, float)
    if _float:
        return str(_float / 2)
