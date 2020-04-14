import datetime as dt
import logging
from typing import Any, Dict, Optional

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


IRRELEVANT_INSTALLATIONS = ['anchorage', 'pilot station']


IRRELEVANT_PRODUCTS = ['container', 'miscellaneous', 'passenger']


@validate_item(PortCall, normalize=True, strict=True, log_level='error')
def process_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw item into a usable event."""

    item = map_keys(raw_item, portcall_mapping())

    # check if portcall is relevant by movement status
    if item.pop('is_pc_relevant', False):
        return

    # sanity check; in case no ETA found
    eta = item.get('eta')
    if not eta:
        logger.error('No ETA date: %s', raw_item.get('Arrival'))
        return

    # discard if portcall is older than 1 month from reported date
    reported_date = item.get('reported_date')
    if (parse_date(reported_date) - parse_date(eta)) > dt.timedelta(days=30):
        logger.info(f"Portcall for vessel {item['vessel']['name']} is too old, skipping")
        return

    # build Cargo sub-model
    if not _is_product_irrelevant(item.get('cargo_product')):
        item['cargoes'] = [
            {
                'product': item.pop('cargo_product', None),
                'movement': 'load',
                'volume': None,
                'volume_unit': None,
            }
        ]
        return item


def portcall_mapping() -> Dict[str, tuple]:
    return {
        'arrival': ('eta', lambda x: to_isoformat(x, dayfirst=False)),
        'berth': ('installation', _clean_installation),
        'cargo': ('cargo_product', None),
        'departure': ('departure', lambda x: to_isoformat(x, dayfirst=False)),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'ship': ('vessel', lambda x: {'name': x}),
        'status': ('is_pc_relevant', _is_portcall_irrelevant),
    }


def _is_portcall_irrelevant(status: Optional[str]) -> bool:
    """Check if portcall is irrelevant, according to its movement status."""
    if not status:
        return True

    return may_strip(status.lower()) in ('departed',)


def _clean_installation(raw_installation: str) -> Optional[str]:
    """Clean raw installation names, and removes anchorages/pilot stations.

    Examples:
        >>> _clean_installation('Arrives to Ridley Terminal')
        'Ridley Terminal'
        >>> _clean_installation('Ridley Terminal')
        'Ridley Terminal'
        >>> _clean_installation('Anchorage 04')

    """
    raw_installation = raw_installation.replace('Arrives to ', '')

    if not any(each in raw_installation.lower() for each in IRRELEVANT_INSTALLATIONS):
        return raw_installation


def _is_product_irrelevant(raw_product: str) -> bool:
    return any(each in raw_product.lower() for each in IRRELEVANT_PRODUCTS)
