import datetime as dt
import logging
import re

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.static_data import vessels
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


INSTALLATION_PRODUCT_MAPPING = {
    'EII': ('Dampier Iron Ore', 'Dry Bulk'),
    'EII-L': ('Dampier Iron Ore', 'Dry Bulk'),
    'LNG': ('Dampier', 'LNG'),
    'LNG1': ('NWS', 'LNG'),
    'LNG2': ('NWS', 'LNG'),
    'LPG': ('Dampier', 'LPG'),
    'PLNG': ('Pluto', 'LNG'),
    'PP': ('Dampier Iron Ore', 'Dry Bulk'),
    'PP2': ('Dampier Iron Ore', 'Dry Bulk'),
    'PP3': ('Dampier Iron Ore', 'Dry Bulk'),
    'PP4': ('Dampier Iron Ore', 'Dry Bulk'),
    'PP5': ('Dampier Iron Ore', 'Dry Bulk'),
    'Wheatstone Marine Terminal LNG': ('Wheatstone', 'LNG'),
}


# cache vessels found on our platforms as a singleton
__KPLER_VESSELS = []


def _kpler_vessels():
    """Download a list of all vessels on our platforms.

    This is required because of the following analyst rules specific to this source.
    The source does not provide the DWT of the vessel,
    so we must try matching them by their names in order to obtain their DWT.

    Given a dry-bulk vessel arriving at Dampier:
        - if its DWT is below 50k, it must carry salt
        - if its DWT is between 50k and 75k, it must carry ore
        - if its DWT is above 75k, it must carry iron ore

    Returns:
        List[Dict[str, Any]]:

    """
    global __KPLER_VESSELS
    __KPLER_VESSELS = __KPLER_VESSELS if __KPLER_VESSELS else vessels(disable_cache=True)
    return __KPLER_VESSELS


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Dispatch item processing to the respective functions.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    if 'in-port' in raw_item['data_type']:
        yield from process_inport_item(raw_item)

    if 'forecast' in raw_item['data_type']:
        yield from process_forecast_item(raw_item)


def process_inport_item(raw_item):
    """Transform raw item into a usable event, if it is obtained from in-port report.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    # each table row actually contains two concatenated items, so we need to split them
    mapping = inport_mapping()
    for item in [map_keys(raw_item, mapping[0]), map_keys(raw_item, mapping[1])]:
        # remove vessels with no name
        if not item['vessel']['name']:
            return

        # each berth can only accomodate a certain commodity, hence we use this to derive cargo
        raw_berth = item.pop('berth', None)
        item['installation'], product = _normalize_installation_and_cargo(
            raw_berth, item['vessel']['name']
        )
        item['cargoes'] = [{'product': product}]

        # don't yield portcalls not matched to any installation or product as a sanity check
        if not item['installation'] or not product:
            continue

        yield item


def process_forecast_item(raw_item):
    """Transform raw item into a usable event, if it is obtained from forecast report.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, forecast_mapping())
    # remove vessels with no name
    if not item['vessel']['name']:
        return

    # discard table rows that show completed/departure vessel movements (i.e. historical movements)
    if 'COMP' not in item.pop('status', '') and 'ARR' in item.pop('movement', ''):
        # each berth can only accomodate a certain commodity, hence we use this to derive cargo
        raw_berth = item.pop('berth', None)
        item['installation'], product = _normalize_installation_and_cargo(
            raw_berth, item['vessel']['name']
        )
        item['cargoes'] = [{'product': product}]

        # don't yield portcalls not matched to any installation or product as a sanity check
        if item['installation'] and product:
            yield item


def inport_mapping():
    return [
        {
            '1': ('vessel', lambda x: {'name': may_strip(x).title() if x else None}),
            '2': ('berth', None),
            '5': ignore_key('shipping agent'),
            '6': ('eta', _normalize_date),
            'port_name': ('port_name', None),
            'provider_name': ('provider_name', None),
            'reported_date': ('reported_date', None),
        },
        {
            '7': ('vessel', lambda x: {'name': may_strip(x).title() if x else None}),
            '8': ('berth', None),
            '11': ignore_key('shipping agent'),
            '12': ('eta', _normalize_date),
            'port_name': ('port_name', None),
            'provider_name': ('provider_name', None),
            'reported_date': ('reported_date', None),
        },
    ]


def forecast_mapping():
    return {
        '0': ('status', None),
        '1': ('eta', _normalize_date),
        '3': ('movement', None),
        '4': ('vessel', lambda x: {'name': may_strip(x).title() if x else None}),
        '6': ('berth', None),
        '10': ignore_key('shipping agent'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def _normalize_installation_and_cargo(raw_berth, vessel_name):
    """Normalize raw berth name into an installation and a product.

    There are special analyst rules specific to this source, depending on the vessel.
    Given a dry-bulk vessel arriving at Dampier:
        - if its DWT is below 50k, it must carry salt
        - if its DWT is between 50k and 75k, it must carry ore
        - if its DWT is above 75k, it must carry iron ore

    Examples:
        >>> _normalize_installation_and_cargo('PLNG', 'Bilbao Knutsen')
        ('Pluto', 'LNG')
        >>> _normalize_installation_and_cargo(None, 'Bilbao Knutsen')
        (None, None)
        >>> _normalize_installation_and_cargo('EII', 'Basic Princess')  # 38k DWT
        ('Dampier Iron Ore', 'Salt')
        >>> _normalize_installation_and_cargo('PP3', 'Nord Pacific')  # 61k DWT
        ('Dampier Iron Ore', 'Ore')
        >>> _normalize_installation_and_cargo('PP', 'Pacific Opal')  # 97k DWT
        ('Dampier Iron Ore', 'Iron Ore')

    """
    installation, product = INSTALLATION_PRODUCT_MAPPING.get(raw_berth, (None, None))
    if installation == 'Dampier Iron Ore':
        try:
            dwt = _kpler_vessels().get(vessel_name, 'name')['dead_weight']
            if dwt < 50000:
                product = 'Salt'
            elif 50000 <= dwt <= 75000:
                product = 'Ore'
            else:
                product = 'Iron Ore'
        except (ValueError, KeyError):
            logger.warning('Vessel not found at Kpler, returning default product: %s', product)

    return installation, product


def _normalize_date(raw_date):
    """Normalize raw date into an ISO8601-formatted date string.

    Args:
        raw_date (str):

    Returns:
        str: ISO8601-formatted date string

    Examples:
        >>> _normalize_date('/Date(1529622000000+0800)/')
        '2018-06-21T23:00:00'
        >>> _normalize_date('/1529622000000+0800/')
        >>> _normalize_date(None)

    """
    if not raw_date:
        return None

    date_match = re.match(r'\/Date\((\d+)\+', raw_date)
    if not date_match:
        logger.error(f'Invalid date format: {raw_date}')
        return None

    # remove milliseconds from epoch
    unix_time = int(date_match.group(1)[:-3])
    return dt.datetime.fromtimestamp(unix_time).isoformat()
