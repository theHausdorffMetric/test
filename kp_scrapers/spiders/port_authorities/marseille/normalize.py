import datetime as dt
import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

# maximum time diff between current time and ETA, before it is considered too old
# expressed in days
MAX_ETA_AGE = 90


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        Dict[str, str]:

    Return:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, field_mapping())

    # discard portcalls without ETA; they are meaningless
    if not item.get('eta'):
        logger.error(f'Portcall for vessel {item.get("vessel_name")} has no ETA, discarding')
        return

    # discard portcall with dates that are too old
    # TODO logic should be made generic
    if (dt.datetime.utcnow() - parse_date(item['eta'])).days >= MAX_ETA_AGE:
        logger.warning(f'Portcall for vessel {item.get("vessel_name")} has old ETA, discarding')
        return

    # build Vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'imo': item.pop('vessel_imo'),
        'length': item.pop('vessel_length', None),
    }

    return item


def field_mapping():
    return {
        # NOTE since we are only scraping scheduled/future portcalls, we hardcode 'eta'
        'Arrival': ('eta', lambda x: to_isoformat(x, dayfirst=True) if x else None),
        'Departure': ignore_key('departure date; not required since it is covered by AIS'),
        # FIXME stop yielding `next_zone` as a hotfix since it is causing
        # the watchlist to be repeatedly triggered for a lot of users
        # 'Destination': ('next_zone', normalize_zone),
        'Dock': ('port_name', None),
        'Flag': ignore_key('vessel flag'),
        # ignore decimal place for vessel length
        'Height': ('vessel_length', lambda x: may_strip(x.split('.')[0])),
        'Last name': ignore_key('duplicate of `Ship` field'),
        'Lloyd number': ('vessel_imo', may_strip),
        'Maritime agent': ignore_key('shipping agent'),
        'Origin': ignore_key('previous port of call'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'Ship': ('vessel_name', None),
        'Type': ('cargoes', lambda x: [{'product': may_strip(x)}]),
        'Voyage reference': ignore_key('internal voyage ID used by source'),
    }


def normalize_zone(raw_zone):
    # `Inconnu` means "unknown" in French
    return may_strip(raw_zone) if 'Inconnu' not in raw_zone else None
