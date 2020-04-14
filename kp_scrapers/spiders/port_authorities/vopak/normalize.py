import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # build vessel sub model
    item['vessel'] = {'name': item.pop('vessel_name')}

    # departure and eta
    actual_etd = item.pop('actual_etd', None)
    etd = item.pop('etd', None)
    item['departure'] = actual_etd or etd or item.get('departure')
    item['eta'] = item.pop('actual_eta', None) or item['eta']

    return item


def portcall_mapping():
    return {
        'DERIVED$01': (ignore_key('detail button')),
        'VESSEL': ('vessel_name', None),
        'ETA_PILOT': ('eta', normalize_date),
        'ETA_FIRST_BERTH': ('berthed', normalize_date),
        'ARRIVAL': ('arrival', normalize_date),
        'ETD': ('etd', normalize_date),
        'DEPARTURE': ('departure', normalize_date),
        'STATUS': (ignore_key('portcall status')),
        'INTERNET_REMARK': (ignore_key('remarks')),
        'URL': (ignore_key('detail link, always None')),
        # call detail
        'Status': (ignore_key('status')),
        'Authorities ref': (ignore_key('ignore')),
        'Port': (ignore_key('port name, duplicate')),
        'Remarks': (ignore_key('remarks, duplicate')),
        'Last updated on': ('reported_date', normalize_date),
        # terminal detail
        'Terminal': ('installation', normalize_berth_installation),
        'Jetty': ('berth', normalize_berth_installation),
        'Portnumber': (ignore_key('ignore')),
        '(E)TA': ('actual_eta', normalize_date),
        '(E)TD': ('actual_etd', normalize_date),
        # meta info
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
    }


def normalize_date(raw_date):
    _search = re.search(r'\d{2}-[A-Za-z]{3,4}-\d{4} \d{2}:\d{2}', raw_date)

    if _search:
        return to_isoformat(_search.group())


def normalize_berth_installation(raw_berth):
    return None if raw_berth.lower() == 'to be advised' or not raw_berth else raw_berth
