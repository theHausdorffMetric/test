import datetime as dt
import logging
import re

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

ACCEPTED_INSTALLATIONS = ['Australia Pacific LNG', 'Queensland Curtis LNG', 'Santos GLNG']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        ArrivedEvent | BerthedEvent | EtaEvent:

    """
    item = map_keys(raw_item, field_mapping())

    # discard irrelevant vessel movement events
    _movement = item.pop('event')
    _status = item.pop('vessel_status')
    if _movement != 'ARR':
        logger.info(f"Vessel {item['vessel_name']} has irrelevant event {_movement}, discarding")
        return

    # discard vessel movements with irrelevant installations
    if item.get('installation') not in ACCEPTED_INSTALLATIONS:
        logger.info(
            f"Vessel {item['vessel_name']} is at an irrelevant installation: "
            f"{item.get('installation')}"
        )
        return

    # discard vessel movements with irrelevant statuses
    if _status == 'CANC':
        logger.info(f"Vessel {item['vesse_name']} has a cancelled status, discarding")
        return

    # build sub-models
    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('vessel_imo')}
    # NOTE vessels filtered so far only carry LNG for the installations
    item['cargoes'] = [{'product': 'lng'}]

    return item


def field_mapping():
    return {
        '0': ignore_key('unknown'),
        '1': ignore_key('unknown'),
        '2': ('event', None),
        '3': ('vessel_name', None),
        '4': ignore_key('vessel type'),
        '5': ignore_key('unknown'),
        '6': ignore_key('shipping agent'),
        '7': ('eta', normalize_date),
        '8': ignore_key('alternate PC date'),
        '9': ignore_key('previous location'),
        '10': ('installation', None),
        '11': ('vessel_status', None),
        '12': ignore_key('irrelevant'),
        '13': ignore_key('irrelevant'),
        '14': ignore_key('internal voyage id'),
        '15': ignore_key('internal vessel id'),
        '16': ignore_key('unknown'),
        'imo': ('vessel_imo', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_date(raw_date):
    """Transform raw date string into an ISO-8601 formatted string.

    Args:
        raw_date (str):

    Returns:
        str:

    Examples:
        >>> normalize_date('/Date(1523397600000-0400)/')
        '2018-04-10T22:00:00'

    """
    return dt.datetime.utcfromtimestamp(
        int(re.match(r'.*\((\d{10})', raw_date).group(1))
    ).isoformat()
