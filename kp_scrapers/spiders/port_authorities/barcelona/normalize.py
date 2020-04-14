import datetime as dt

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable portcall event.

    Args:
        Dict[str, str]:

    Return:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, field_mapping())

    # discard ETA if date is historical
    # Barcelona is 2 hours ahead of UTC
    if parse_date(item['eta'], dayfirst=False) < (dt.datetime.utcnow() + dt.timedelta(hours=2)):
        return

    # build Vessel sub-model
    item['vessel'] = {
        'imo': item.pop('imo', None),
        'name': item.pop('vessel_name', None),
        'gross_tonnage': item.pop('gross_tonnage', None),
        'length': item.pop('length', None),
        'beam': item.pop('beam', None),
        'flag_code': item.pop('flag_code', None),
    }

    item['reported_date'] = (
        dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    )

    return item


def field_mapping():
    return {
        # NOTE since we are only scraping scheduled/future portcalls, we hardcode 'eta'
        'ship_name': ('vessel_name', None),
        'pier_Assigned': ('berth', None),
        'ship_type': ignore_key('vessel type'),
        'date_arrival': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'date_departure': ('departure', lambda x: to_isoformat(x, dayfirst=True)),
        'depth': ignore_key('depth'),
        'Ship name': ignore_key('Ship name'),
        'Indicative Radius:': ignore_key('Indicative Radius:'),
        'IMO Number:': ('imo', None),
        'GT London 1969:': ignore_key('irrelevant'),
        'GT:': ('gross_tonnage', None),
        'Country:': ('flag_code', lambda x: x.partition('-')[0]),
        'Consignee:': ignore_key('consignee'),
        'Shipowner:': ignore_key('companies'),
        'Ship type:': ('vessel_type', None),
        'Tanks:': ignore_key('Tanks:'),
        'Beam:': ('beam', None),
        'Length:': ('length', None),
        'Draught:': ignore_key('Draught:'),
        'provider_name': ('provider_name', None),
        'port_name': ('port_name', None),
    }
