import logging
from typing import Any, Callable, Dict, Optional, Tuple

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(PortCall, normalize=True, strict=True, log_level='error')
def process_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw item into a usable event."""

    item = map_keys(raw_item, portcall_mapping())

    # discard vessels without any portcall dates
    if not (item.get('eta') or item.get('departure')):
        logger.info('Vessel %s has no portcall date', item.get('vessel_name'))
        return

    # build Vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'length': item.pop('vessel_length'),
        'gross_tonnage': item.pop('vessel_gt'),
    }

    # build proper portcall dates
    _reported_date = item['reported_date']
    for event in ('eta', 'departure'):
        item[event] = normalize_date(item[event], _reported_date) if item.get(event) else None

    return item


def portcall_mapping() -> Dict[str, Tuple[str, Optional[Callable]]]:
    return {
        '#': ignore_key('internal portcall ID'),
        'Arrival': ('eta', None),
        'Departure': ('departure', None),
        'Destination': ignore_key('next zone; discuss with analysts on accuracy and value'),
        'Expected Arrival': ('eta', None),
        'Flag': ignore_key('empty'),
        'GT': ('vessel_gt', None),
        'GT 100': ('vessel_gt', lambda x: int(x) * 100 if try_apply(x, int) else None),
        'Home Port': ignore_key('home port'),
        'Length': ('vessel_length', may_strip),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
        'Situation': ignore_key('current vessel situation'),
        'Vessel': ('vessel_name', lambda x: may_strip(x).upper() if x else None),
    }


def normalize_date(raw_portcall_date: str, reported_date: str) -> str:
    """Normalize raw portcall date using contextual info from reported date.

    This source provides dates only in the format "dd bbb", without any year info.
    Which means we need to infer the year from the reported date.

    Examples:
        >>> normalize_date('12 Jun', '2019-05-23T00:00:00')
        '2019-06-12T00:00:00'
        >>> normalize_date('30 Dec', '2020-01-29T00:00:00')
        '2019-12-30T00:00:00'
        >>> normalize_date('3 Jan', '2019-12-05T00:00:00')
        '2020-01-03T00:00:00'

    """
    _current_year = int(reported_date[:4])
    portcall_date = parse_date(f'{raw_portcall_date} {_current_year}', dayfirst=True)
    reported_date = parse_date(reported_date, dayfirst=False)

    # case 1: positive rollover
    if (reported_date - portcall_date).days > 180:  # half a year is a safe enough buffer
        portcall_date += relativedelta(years=1)
    # case 2: negative rollover
    elif (reported_date - portcall_date).days < -180:
        portcall_date -= relativedelta(years=1)

    return portcall_date.isoformat()
