from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import xldate_to_datetime
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping())
    item['cargo'] = normalize_cargo(item.pop('product'), item.pop('volume'))
    return item


def field_mapping():
    return {
        '0': ('port_name', None),
        '1': ('vessel', lambda x: {'name': x}),
        '2': ('product', None),
        '3': ('volume', None),
        '4': ignore_key('B/L QUANTITY'),
        '5': ignore_key('B/L DATE'),
        '6': ignore_key('BUYERS'),
        '7': ignore_key('SHIPPER'),
        '8': ignore_key('DISH PORT'),
        '9': ('arrival', normalize_date),
        '10': ('berthed', normalize_date),
        '11': ('departure', normalize_date),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
    }


def normalize_date(raw_date):
    """Dealing with date extracted from excel, could be a date text or a float.

    Examples:
        >>> normalize_date(43373.0)
        '2018-09-30T00:00:00'
        >>> normalize_date('29.09.18')
        '2018-09-29T00:00:00'

    Args:
        raw_date:

    Returns:

    """
    if isinstance(raw_date, float):
        return xldate_to_datetime(raw_date, sheet_datemode=0).isoformat()
    else:
        return to_isoformat(raw_date, dayfirst=True)


def normalize_cargo(product, volume, volume_unit=Unit.tons, movement='load'):
    """Normalize cargoes.

    Args:
        product (str):
        volume (str):
        volume_unit (str): 'tons' by default
        movement (str): 'load' by default

    Returns:
        List[Dict[str, str]]

    """
    return {
        'product': product,
        'volume': str(volume),
        'volume_unit': volume_unit,
        'movement': movement,
    }
