from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.
    Args:
        raw_item (Dict[str, str]):
    Returns:
        Dict[str, Any]:
    """
    item = map_keys(raw_item, portcall_mapping())

    # build Vessel sub model
    item['vessel'] = {'name': item.pop('vessel_name', None), 'imo': item.pop('vessel_imo', None)}

    return item


def portcall_mapping():
    return {
        'Call ID': ignore_key('irrelevant'),
        'Vessel Name': ('vessel_name', None),
        'IMO': ('vessel_imo', None),
        'Agent': ('shipping_agent', None),
        'Estimated Time of Arrival': ('eta', to_isoformat),
        'Last Port': ignore_key('previous port of call'),
        'Next Port': ignore_key('next port of call'),
        'Status': ignore_key('irrelevant'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }
