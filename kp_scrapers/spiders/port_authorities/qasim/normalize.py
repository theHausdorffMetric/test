import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


IRRELEVANT_PRODUCT = ['NIL']

IRRELEVANT_VESSEL_PREFIXES = ['M.T', 'M.V']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, portcall_mapping())

    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_length', None)}

    # discard items that don't contain a vessel at all
    # source will display those vessels as "VACANT"
    if item['vessel']['name'].lower() == 'vacant':
        return

    # discard items with vessels that carry irrelevant products
    if any(prod in item['cargo_product'] for prod in IRRELEVANT_PRODUCT):
        logger.info(
            f'Vessel {item["vessel"]["name"]} is carrying irrelevant product: '
            f'{item["cargo_product"]}'
        )
        return

    # build proper cargo
    item['cargoes'] = [
        {
            'product': item.pop('cargo_product'),
            'movement': None,
            'volume': item.pop('cargo_volume', None),
            'volume_unit': Unit.tons,
        }
    ]

    if 'dailyshippingprogram' in item.pop('url').lower():
        if item.get('ETD'):
            item['departure_time'] = item.pop('ETD', None)
            item['departure_date'] = parse_date(item['reported_date']).strftime('%d-%b-%Y')

        if item.get('ETB'):
            item['berthed_time'] = item.pop('ETB', None)
            item['berthed_date'] = parse_date(item['reported_date']).strftime('%d-%b-%Y')

    # build proper eta/berthed timestamp
    build_proper_date(item, 'eta')
    build_proper_date(item, 'berthed')
    build_proper_date(item, 'departure')

    return item


def portcall_mapping():
    return {
        'Agent': ('shipping_agent', None),
        'Arrival Date': ('eta_date', None),
        'Arrival Date &amp; Time': (
            'arrival',
            lambda x: to_isoformat(x.replace(':', ''), dayfirst=True),
        ),
        'BalanceUnloadedLoaded': ignore_key('cargo volume remaining to be handled'),
        'Balance UnloadedLoaded': ignore_key('cargo volume remaining to be handled'),
        'Berth': ('berth', None),
        'Berth No.': ('berth', None),
        'Berthing Date': ('berthed_date', None),
        'Berthing Time': ('berthed_time', None),
        'Cargo Handling (Last 24 Hours)Unloaded Loaded': ignore_key('cargo volume handled ytd'),
        'Cargo Handling (Last 24 Hours) Unloaded Loaded': ignore_key('cargo volume handled ytd'),
        'Commodity': ('cargo_product', None),
        'CommodityImport/Export': ('cargo_product', None),
        'Commodity (Import/Export)': ('cargo_product', None),
        'Discharging/Loading Port': ignore_key('vague description of previous/next port'),
        'Draft (M)': ignore_key('vessel draught at outer anchorage'),
        'Draft(M)': ignore_key('vessel draught at outer anchorage'),
        'Draft Aft (Meters)': ignore_key('vessel draught at outer anchorage'),
        'Draft FWR (Meters)': ignore_key('vessel draught at outer anchorage'),
        'Exp': ignore_key('export volume of ETA vessels'),
        'ETB': ('ETB', None),
        'ETD': ('ETD', None),
        'Imp': ignore_key('import volume of ETA vessels'),
        'L.O.A': ('vessel_length', lambda x: str(round(float(x))) if x else None),
        'Manifest QTY(Import) (Export)': ('cargo_volume', clean_volume),
        'Manifest Qty. (Import)(Export)': ('cargo_volume', clean_volume),
        'Manifest Qty (Import)(Export)': ('cargo_volume', clean_volume),
        'Name of Ship': ('vessel_name', clean_vessel_name),
        'Name Of Ship': ('vessel_name', clean_vessel_name),
        'Nationality': ignore_key('vessel flag'),
        'port_name': ('port_name', None),
        'PQA Reg No': ignore_key('internal port authority ID'),
        'PQA Reg. No.': ignore_key('internal port authority ID'),
        'PreviousUnloadedLoaded': ignore_key('cargo volume handled prior'),
        'Previous UnloadedLoaded': ignore_key('cargo volume handled prior'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'Ship Agent': ignore_key('shipping agent'),
        'Ship Agent Name': ignore_key('shipping agent'),
        'Ship Name': ('vessel_name', clean_vessel_name),
        'Time': ('eta_time', None),
        'Tonnage': ('cargo_volume', clean_volume),
        'url': ('url', None),
        'Vessel Name': ('vessel_name', clean_vessel_name),
    }


def clean_vessel_name(raw_name):
    """Clean raw vessel name and remove designations/prefixes.

    Args:
        raw_name (str):

    Returns:
        str:

    Examples:
        >>> clean_vessel_name('M.V- MOL GENEROSITY')
        'MOL GENEROSITY'
        >>> clean_vessel_name('M.V-MOL GENEROSITY')
        'MOL GENEROSITY'
        >>> clean_vessel_name('MAULE -M.T')
        'MAULE'
        >>> clean_vessel_name('CHRIS M.TORM')
        'CHRIS M.TORM'

    """
    if '-' in raw_name:
        tokenised = raw_name.split('-')
        cleaned_name = []
        for token in tokenised:
            if token not in IRRELEVANT_VESSEL_PREFIXES:
                cleaned_name.append(token)
        return may_strip(' '.join(cleaned_name))

    return raw_name


def build_proper_date(item, date_namespace):
    """Mutate a portcall item's portcall dates given a namespace.

    Args:
        item (Dict[str, Any]):

    """
    if item.get(f'{date_namespace}_date') and item.get(f'{date_namespace}_time'):
        _date, _time = item.pop(f'{date_namespace}_date'), item.pop(f'{date_namespace}_time')
        item[date_namespace] = to_isoformat(f'{_date} {_time}', dayfirst=True)


def clean_volume(raw_volume):
    """clean volume string

    Args:
        raw_volume (str):

    Returns:
        str:

    Examples:
        >>> clean_volume('-')
        >>> clean_volume(' 3000 ')
        '3000'

    """
    if '-' in raw_volume:
        return None

    return may_strip(raw_volume)
