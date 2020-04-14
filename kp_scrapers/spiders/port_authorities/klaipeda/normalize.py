import datetime as dt
import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


# lower and upper bounds for what is a valid portcall to yield
# numbers are in days from current date
PORTCALL_DATE_BOUNDS = -1, 30


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # build Vessel sub model
    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('vessel_imo', None)}

    # discard ETAs that are in the past, or too far into the future
    if not item.get('matching_date'):
        return

    # build proper portcall date by event type
    item[item.pop('event_type')] = item.pop('matching_date')

    return item


def portcall_mapping():
    return {
        'Agent': ('shipping_agent', None),
        'Date, time': ('matching_date', normalize_matching_date),
        'event_type': ('event_type', None),
        'Flag': ignore_key('vessel flag; ignored because they are not in ISO3166 format'),
        'IMO No.': ('vessel_imo', lambda x: may_apply(x, int)),
        'No. ': ignore_key('table row serial number; irrelevant'),
        'Port Company': ignore_key('port company; stevedore ?'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'Quay No.': ('berth', None),
        'reported_date': ('reported_date', None),
        'Ship': ('vessel_name', None),
        'unknown': ignore_key('contains a date; unsure; to clarify with product owner'),
    }


def normalize_matching_date(raw_date):
    """Normalize a portcall's date.

    Sometimes this date may be too far in the past or future.
    We want to ignore those cases.

    Args:
        raw_date (str):

    Returns:
        Optional[str]: datestamp in ISO8601 if valid, else None

    """
    # sanity check
    if not raw_date:
        return None

    time_diff = (parse_date(raw_date, dayfirst=False) - dt.datetime.utcnow()).days
    if not (PORTCALL_DATE_BOUNDS[0] < time_diff < PORTCALL_DATE_BOUNDS[1]):
        logger.warning('Portcall date is too far in the past/future: %s', raw_date)
        return None

    return to_isoformat(raw_date, dayfirst=False)
