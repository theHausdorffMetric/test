from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_apply, may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


CARGO_BLACKLIST = ['CNTR', 'NA']

PRODUCT_MAPPING = {'PRPAN': 'Propane'}


@validate_item(PortCall, normalize=True, strict=True)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # discard empty berth's "portcalls"
    if not item['vessel']['name']:
        return

    # discard container vessels
    if item.get('cargo_product', 'NA') in CARGO_BLACKLIST:
        return

    item['cargoes'] = [build_cargo(item)]
    item['arrival'] = combine_date_and_time(item.pop('arrival_date'), item.pop('arrival_time'))
    item['berthed'] = combine_date_and_time(item.pop('berthed_date'), item.pop('berthed_time'))

    # discard portcall if no relevant portcall date found
    if not (item.get('arrival') or item.get('berthed')):
        return

    return item


def portcall_mapping():
    return {
        'Berth From': ignore_key('from berth'),
        'Berth Name': ('berth', None),
        'Cargo Code': ('cargo_product', lambda x: PRODUCT_MAPPING.get(x, x)),
        'Commence Work Date': ('berthed_date', None),
        'Commence Work Time': ('berthed_time', None),
        'Finish Work Date': ignore_key('finish date'),
        'Finish Work Time': ignore_key('finish time'),
        'Haul In Date': ('arrival_date', None),
        'Haul In Time': ('arrival_time', None),
        'Haul Out Date': ignore_key('departure date'),
        'Haul Out Time': ignore_key('departure time'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'S.No': ignore_key('serial number'),
        'To Be LD/SH': ('cargo_volume_handled', None),
        'Total Quantity (MT)': ('cargo_volume_leftover', None),
        'Vessel Due': ignore_key('vessel due'),
        'Vessel Name': ('vessel', lambda x: {'name': normalize_vessel_name(x)}),
        'Vessel Shifting': ignore_key('vessel shifting'),
    }


def normalize_vessel_name(raw_name):
    """Normalize vessel name.

    Remove M.T., M.V. in front.

    Args:
        raw_name (str):

    Returns:
        str:

    Examples:
        >>> normalize_vessel_name('M.T. GLOBAL RANI')
        'GLOBAL RANI'
        >>> normalize_vessel_name('Vacant')

    """
    if may_strip(raw_name) == 'Vacant':
        return None

    return may_strip(raw_name.replace('M.V.', '').replace('M.T.', ''))


def combine_date_and_time(_date, _time):
    """Combine date and time provided.

    Examples:
        >>> combine_date_and_time('29/04/2019', '23:50:00')
        '2019-04-29T23:50:00'

    Args:
        _date (str):
        _time (str):

    Returns:
        str:

    """
    return to_isoformat(f'{_date} {_time}', dayfirst=True) if _date else None


def build_cargo(item):
    """Normalize cargo.

    Args:
        item:

    Yields:
        Dict:

    """
    product = item.pop('cargo_product')
    volume_handled = may_apply(item.pop('cargo_volume_handled', 0), int)
    volume_leftover = may_apply(item.pop('cargo_volume_leftover', 0), int)

    return {
        'product': product,
        'volume': str(volume_handled + volume_leftover),
        'volume_unit': Unit.tons,
    }
