import datetime as dt
import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, portcall_mapping())

    # check if item describes a proper portcall arrival; discard if not
    movement = item.pop('movement')
    if not (movement.startswith('Arrival to') or movement.startswith('Anchorage to')):
        logger.info(f"Portcall for vessel {item['vessel_name']} does not describe an arrival")
        return

    # build Vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'imo': item.pop('vessel_imo'),
        'call_sign': item.pop('vessel_callsign'),
    }

    return item


def portcall_mapping():
    return {
        'agent': ignore_key('shipping agent'),
        'callSign': ('vessel_callsign', None),
        'from': ignore_key('previous vessel location'),
        'hmNumber': ignore_key('internal portcall number'),
        'imoNo': ('vessel_imo', None),
        'moveType': ('movement', None),
        'operation': ignore_key('remarks on portcall; no valuable info'),
        'orderTime': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'orderType': ignore_key('estimate or actual eta'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': (
            'reported_date',
            lambda x: dt.datetime.utcfromtimestamp(x)
            .replace(hour=0, minute=0, second=0)
            .isoformat(),
        ),
        'to': ignore_key('next vessel location'),
        'vessel': ('vessel_name', None),
    }
